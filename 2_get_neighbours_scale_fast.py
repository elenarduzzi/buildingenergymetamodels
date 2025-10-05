"""
Batch download Pand JSON objects (LoD 1.2) from 3DBAG API
for a folder of neighbour Pand ID lists.

Input
~~~~~
C:\thesis\CLEAN_WORKFLOW\2B_adjacency_out\2C_nb_pands_ids\adj_jsons_6\*.json

Each input file contains e.g.:
    [
      "NL.IMBAG.Pand.0599100000013049",
      "NL.IMBAG.Pand.0599100000124472",
      "NL.IMBAG.Pand.0599100000264323"
    ]

Outputs
~~~~~~~
For each input file, create a subfolder named after the file:

C:\thesis\CLEAN_WORKFLOW\2B_adjacency_out\3_collect_neighbour_jsons\nb_jsons_6\0599100000013049_neighbour_ids\*.json

Each output JSON file contains:
- metadata
- LoD 1.2 geometry and attributes.

Retries, batching, concurrency remain unchanged.
Now also processes multiple input files concurrently.
"""

import asyncio
import datetime
import json
import logging
import pathlib
from typing import Iterable, List, Dict, Any, Tuple

import aiohttp
import boto3
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from tqdm.asyncio import tqdm

# inputs
INPUT_FOLDER = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\1_nb_pands_ids\adj_jsons_21")
OUTPUT_ROOT  = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\2_collect_neighbour_jsons\nb_jsons_21")
LOG_DIR = OUTPUT_ROOT / "logs"; LOG_DIR.mkdir(parents=True, exist_ok=True)

S3_BUCKET: str = ""  # leave empty to store locally

CONCURRENT_REQUESTS = 10   # simultaneous TCP connections to api.3dbag.nl per file
BATCH_SIZE  = 500          # Pand IDs handled per batch
TIMEOUT_S   = 60           # per-request timeout
MAX_RETRIES = 4            # Tenacity retries on 5xx / time-out
MAX_CONCURRENT_FILES = 5   # limit how many input files are processed in parallel

TIMEOUT_LOG = LOG_DIR / f"timed_out_{datetime.date.today()}.txt"

# setup

s3       = boto3.client("s3")
_timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)

# helpers

def chunked(it: Iterable[str], n: int) -> Iterable[List[str]]:
    buf: List[str] = []
    for item in it:
        buf.append(item)
        if len(buf) == n:
            yield buf
            buf = []
    if buf:
        yield buf

#  network / IO layer

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(min=5, max=120),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
)
async def fetch_one(full_pid: str, output_dir: pathlib.Path, session: aiohttp.ClientSession) -> None:
    key = f"{full_pid}.json"

    if not S3_BUCKET:
        output_dir.mkdir(parents=True, exist_ok=True)
        if (output_dir / key).exists():
            return  # already cached

    url = f"https://api.3dbag.nl/collections/pand/items/{full_pid}"
    async with session.get(url, timeout=_timeout) as resp:
        resp.raise_for_status()
        data: Dict[str, Any] = await resp.json()

    features = data.get("features") or [data.get("feature")]
    if not features:
        return
    root = features[0]

    out: Dict[str, Any] = {
        "metadata": {**data.get("metadata", {}), "vertices": root.get("vertices", [])},
        "buildings": [],
    }

    for obj_id, obj in root.get("CityObjects", {}).items():
        if obj.get("type") != "Building":
            continue

        attr = obj.get("attributes", {})
        building: Dict[str, Any] = {
            "Pand ID": full_pid,
            "Status": attr.get("status"),
            "Construction Year": attr.get("oorspronkelijkbouwjaar"),
            "Number of Floors": attr.get("b3_bouwlagen"),
            "Roof Type": attr.get("b3_dak_type"),
            "Wall Area": attr.get("b3_opp_buitenmuur"),
            "Roof Area (Flat)": attr.get("b3_opp_dak_plat"),
            "Roof Area (Sloped)": attr.get("b3_opp_dak_schuin"),
            "Floor Area": attr.get("b3_opp_grond"),
            "Shared Wall Area": attr.get("b3_opp_scheidingsmuur"),
            "Ground Elevation (NAP)": attr.get("b3_h_maaiveld"),
            "LoD 1.2 Data": {},
            "Boundaries (LoD 1.2)": [],
        }

        for child_id in obj.get("children", []):
            child = root["CityObjects"].get(child_id, {})
            for geom in child.get("geometry", []):
                if geom.get("lod") != "1.2":
                    continue

                for surf in geom.get("semantics", {}).get("surfaces", []):
                    if surf.get("type") == "RoofSurface":
                        building["LoD 1.2 Data"] = {
                            "Building Height (Mean)": surf.get("b3_h_dak_50p"),
                            "Building Height (70%)":   surf.get("b3_h_dak_70p"),
                            "Building Height (Max)":   surf.get("b3_h_dak_max"),
                            "Building Height (Min)":   surf.get("b3_h_dak_min"),
                        }
                        break

                boundaries = geom.get("boundaries")
                if boundaries:
                    building["Boundaries (LoD 1.2)"].append(boundaries)

        out["buildings"].append(building)

    data_bytes = json.dumps(out, indent=2).encode()

    if S3_BUCKET:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data_bytes)
    else:
        (output_dir / key).write_bytes(data_bytes)


async def fetch_safe(pid: str, output_dir: pathlib.Path, session: aiohttp.ClientSession) -> Tuple[str, Exception | None]:
    try:
        await fetch_one(pid, output_dir, session)
        return pid, None
    except Exception as exc:
        return pid, exc


async def fetch_batch(batch: List[str], output_dir: pathlib.Path, session: aiohttp.ClientSession) -> List[str]:
    tasks = [asyncio.create_task(fetch_safe(pid, output_dir, session)) for pid in batch]
    failed: List[str] = []

    for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        pid, exc = await fut
        if exc is not None:
            failed.append(pid)
            logging.error("Unhandled error for %s – %s", pid, exc)

    return failed

async def process_file(id_file: pathlib.Path) -> None:
    output_dir = OUTPUT_ROOT / id_file.stem
    print(f"Processing {id_file.name} → {output_dir}")
    pand_ids: List[str] = json.loads(id_file.read_text())

    if not isinstance(pand_ids, list):
        print(f"Skipped {id_file} – not a list of IDs.")
        return

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        for batch in chunked(pand_ids, BATCH_SIZE):
            await fetch_batch(batch, output_dir, session)

# 

async def main() -> None:
    id_files = list(INPUT_FOLDER.glob("*.json"))
    print(f"Found {len(id_files)} input files.")

    sem = asyncio.Semaphore(MAX_CONCURRENT_FILES)

    async def sem_task(id_file):
        async with sem:
            await process_file(id_file)

    await asyncio.gather(*(sem_task(f) for f in id_files))

    print("All downloads complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR, format="%(levelname)s:%(message)s")
    asyncio.run(main())


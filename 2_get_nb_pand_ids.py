"""
Batch‑extract neighbouring Pand IDs for every building JSON in the
sampled ADJ folders, with asynchronous I/O, limited concurrency,
batching, Tenacity retry with exponential backoff, and a `tqdm` progress bar.

Folders scanned (relative to `SOURCE_ROOT`):
    adj_jsons_6 | adj_jsons_7 | adj_jsons_8 | adj_jsons_21

For each *.json file:
    • buffer ground‑surface polygon by 0.5 m
    • create bounding box from buffered polygon
    • query 3DBAG `/collections/pand/items` for intersecting features
    • write a neighbouring‑IDs list to the mirror location under
      `OUTPUT_ROOT`, keeping sub‑folder structure

Example output file (per input PID):
    C:\…\debug_neighbours\adj_jsons_6\0599100000012801_neighbour_ids.json

Requirements: `aiohttp`, `tqdm`, `shapely`, `tenacity`.
"""

import asyncio
import json
import pathlib
import traceback
from typing import List, Set, Tuple

import aiohttp
from shapely.geometry import Polygon
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm.asyncio import tqdm

# inputs
SOURCE_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\0_surface_ADJ_sampled_20k")
OUTPUT_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\1_nb_pands_ids")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

SUBFOLDERS = [

    "adj_jsons_21",

]

BUFFER_DISTANCE = 0.20  # metres buffer around polygon
URL_ITEMS = "https://api.3dbag.nl/collections/pand/items"
TIMEOUT = aiohttp.ClientTimeout(total=90)
CONCURRENCY = 20  # parallel API calls
RETRIES = 4  # tenacity retries
BATCH_SIZE = 200  # number of JSON files per batch

# helpers

def ground_polygon(bldg: dict) -> Polygon:
    """Return the ground surface (Type 'G') as a Shapely polygon."""
    for surf in bldg.get("Surfaces", []):
        if surf.get("Type") == "G":
            return Polygon([(x, y) for x, y, _ in surf["Coordinates"][0]])
    raise ValueError("No ground surface found")


@retry(
    stop=stop_after_attempt(RETRIES),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
)
async def fetch_features(session: aiohttp.ClientSession, bbox_str: str) -> List[dict]:
    """GET ?bbox=… from /collections/pand/items and return features list."""
    params = {"bbox": bbox_str, "limit": "500"}
    async with session.get(URL_ITEMS, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return data.get("features", [])


async def process_file(path: pathlib.Path, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> None:
    """Handle a single JSON file write neighbour‑ID list JSON."""
    async with sem:
        try:
            data = json.loads(path.read_text())
            pid, bldg = next(iter(data.items()))

            gpoly = ground_polygon(bldg)
            buffered_poly = gpoly.buffer(BUFFER_DISTANCE)
            xmin, ymin, xmax, ymax = buffered_poly.bounds

            bbox = f"{xmin},{ymin},{xmax},{ymax}"

            features = await fetch_features(session, bbox)

            neighbours: Set[str] = {
                feat.get("id")
                or feat.get("properties", {}).get("bag", {}).get("identificatie")
                or feat.get("properties", {}).get("identificatie")
                for feat in features
            }
            neighbours.discard(pid)

            relative_folder = path.parent.relative_to(SOURCE_ROOT)
            dest_dir = OUTPUT_ROOT / relative_folder
            dest_dir.mkdir(parents=True, exist_ok=True)

            out_path = dest_dir / f"{pid}_neighbour_ids.json"
            out_path.write_text(json.dumps(sorted(neighbours), indent=2))
        except Exception as exc:
            print(f"ERROR processing {path}: {type(exc).__name__} - {exc}")
            traceback.print_exc(limit=1)


async def process_batch(batch: List[pathlib.Path], session: aiohttp.ClientSession, sem: asyncio.Semaphore):
    tasks = [process_file(p, session, sem) for p in batch]
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Batch Processing"):
        try:
            await coro
        except Exception:
            pass


def chunked(it: List[pathlib.Path], n: int) -> List[List[pathlib.Path]]:
    """Yield successive n-sized chunks from it."""
    return [it[i:i + n] for i in range(0, len(it), n)]


# main

async def main() -> None:
    json_paths: List[pathlib.Path] = []
    for sub in SUBFOLDERS:
        json_paths.extend((SOURCE_ROOT / sub).glob("*.json"))

    if not json_paths:
        print("No JSON files found — check SOURCE_ROOT and SUBFOLDERS list.")
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as session:
        batches = chunked(json_paths, BATCH_SIZE)
        for batch in tqdm(batches, desc="Overall Progress"):
            await process_batch(batch, session, sem)

    print("All neighbour‑ID files written to", OUTPUT_ROOT)


if __name__ == "__main__":
    asyncio.run(main())


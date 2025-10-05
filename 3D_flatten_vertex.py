# flatten vertex data

import os, json, csv, itertools
from collections import OrderedDict
from pathlib import Path
from tqdm import tqdm
import concurrent.futures

# vertex data written once. # checked outliers same for all scenarios. 

PAD_VALUE  = -1  # value when vertex/unit‑pair slot does not exist
INDEXES = [6, 7, 8, 21]

# paths
ROOT_INPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\1_enrich_jsons\A1_base_2020")
ROOT_OUTPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat")

def explode_surfaces(surface: dict):
    if surface.get("Type") != "F":
        yield surface
        return

    dists = surface.get("Distances", [])
    units = surface.get("Angles", [])
    n_faces = max(1, len(dists) // 2)
    for i in range(n_faces):
        yield {
            **surface,
            "Distances" : dists[2 * i : 2 * i + 2],
            "Angles" : units[2 * i : 2 * i + 2],
        }

def scan_max_lengths(files):
    max_dists = max_units = 0
    for fn in files:
        with fn.open(encoding="utf-8") as f:
            b = json.load(f)
        for s in b.get("Surfaces", []):
            for face in explode_surfaces(s):
                max_dists = max(max_dists, len(face.get("Distances", [])))
                max_units = max(max_units, len(face.get("Angles", [])))
    return max_dists, max_units

def flatten_json(fn, max_dists, max_units):
    with fn.open(encoding="utf-8") as f:
        b = json.load(f)
    meta = [b.get("Pand ID")]
    rows = []
    surf_idx = 0
    for s in b.get("Surfaces", []):
        for face in explode_surfaces(s):
            dists   = face.get("Distances", [])
            units2d = list(itertools.chain.from_iterable(face.get("Angles", [])))

            dists = [round(d, 4) if d != PAD_VALUE else PAD_VALUE for d in dists]
            units2d = [round(u, 4) if u != PAD_VALUE else PAD_VALUE for u in units2d]

            row = (
                meta +
                [surf_idx, face.get("Type")] +
                dists + [PAD_VALUE] * (max_dists - len(dists)) +
                units2d + [PAD_VALUE] * (max_units * 2 - len(units2d))
            )
            rows.append(row)
            surf_idx += 1
    return rows

def process_subfolder(idx):
    input_dir = ROOT_INPUT / f"enrich_{idx}"
    output_dir = ROOT_OUTPUT / f"flat_vertex_{idx}"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"flat_vertex_{idx}.csv"

    files = list(input_dir.glob("*.json"))
    if not files:
        print(f"No files in {input_dir}")
        return

    # determine max dists/units for columns
    max_dists, max_units = scan_max_lengths(files)

    base_cols = [
        "Pand ID",
        "Surface Index", "Surface Type"
    ]
    dist_cols = [f"d{i+1}" for i in range(max_dists)]
    unit_cols = list(itertools.chain.from_iterable(
        (f"ux{i+1}", f"uy{i+1}") for i in range(max_units)))
    header = base_cols + dist_cols + unit_cols

    # flatten in parallel, then concatenate all rows
    rows = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for chunk in tqdm(executor.map(flatten_json, files, [max_dists]*len(files), [max_units]*len(files)),
                          total=len(files), desc=f"enrich_{idx} -> flat_vertex_{idx}"):
            rows.extend(chunk)

    # write CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"Written: {csv_path}")

if __name__ == "__main__":
    for idx in INDEXES:
        print(f"Processing enrich_{idx} ...")
        process_subfolder(idx)
    print("All done.")


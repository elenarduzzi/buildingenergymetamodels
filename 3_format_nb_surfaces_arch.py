"""
Batch process thousands of 3DBAG building JSONs:
- Load original neighbour-building JSONs.
- Extract LoD 1.2 surfaces.
- Transform local-mm vertices to absolute RD-New metres.
- Classify each surface (G / F / R).
- Write per-building JSON.

Input
~~~~~
C:\thesis\CLEAN_WORKFLOW\2B_adjacency_out\3_collect_neighbour_jsons\nb_jsons_6\*/<building>.json

Output
~~~~~~
C:\thesis\CLEAN_WORKFLOW\2B_adjacency_out\3_formatted_nb_surface_json\nb_format_jsons_6\*/<building>.json
(keep subfolder per original Pand ID)
"""

import json
import pathlib
import asyncio
from concurrent.futures import ThreadPoolExecutor

# folders
INPUT_ROOT  = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\2_collect_neighbour_jsons\nb_jsons_21")
OUTPUT_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\3_formatted_nb_surface_json\nb_format_jsons_21")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


MAPPING_FILE = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\1_data_out\0_map_pands_to_archetype\pand_arch_map_21.json")
with MAPPING_FILE.open("r", encoding="utf-8") as f:
    pand2arch = json.load(f)


# helpers
def load_json(fp: pathlib.Path) -> dict:
    with fp.open() as f:
        return json.load(f)

def abs_vertices(raw_vertices, scale, translate):
    sx, sy, sz = scale
    tx, ty, tz = translate
    return [[x * sx + tx, y * sy + ty, z * sz + tz] for x, y, z in raw_vertices]

def extract_building(data: dict) -> dict | None:
    tform  = data["metadata"]["transform"]
    verts  = abs_vertices(data["metadata"]["vertices"], tform["scale"], tform["translate"])

    for b in data["buildings"]:
        if not all(k in b for k in ["Pand ID", "Boundaries (LoD 1.2)"]):
            continue

        pid = b["Pand ID"]
        key = pid.split('.')[-1] if pid else ""
        archetype_id = pand2arch.get(key, "N/A")

        abs70 = None
        h70 = b.get("LoD 1.2 Data", {}).get("Building Height (70%)")
        ground_elev = b.get("Ground Elevation (NAP)")
        if h70 is not None and ground_elev is not None:
            abs70 = abs(h70 - ground_elev)

        surfaces = []
        for group in b["Boundaries (LoD 1.2)"]:
            for surface in group:
                for ring_group in surface:
                    for ring in ring_group:
                        surfaces.append({"Coordinates": [[verts[i] for i in ring]]})

        return {
            "Pand ID": pid,
            "Archetype ID": archetype_id,
            "Construction Year": b.get("Construction Year"),
            "Number of Floors": b.get("Number of Floors"),
            "Wall Area": b.get("Wall Area"),
            "Roof Area (Flat)": b.get("Roof Area (Flat)"),
            "Roof Area (Sloped)": b.get("Roof Area (Sloped)"),
            "Floor Area": b.get("Floor Area"),
            "Shared Wall Area": b.get("Shared Wall Area"),
            "Absolute Height (70%)": abs70,
            "Surfaces": surfaces,
        }
    return None

def classify_surfaces(building: dict) -> None:
    surfs = building["Surfaces"]
    z_vals = [z for s in surfs for ring in s["Coordinates"] for *_, z in ring]
    z_min, z_max = min(z_vals), max(z_vals)

    for s in surfs:
        z = [p[2] for ring in s["Coordinates"] for p in ring]
        s["Type"] = "G" if all(v == z_min for v in z) else \
                    "R" if all(v == z_max for v in z) else "F"

def write_json(obj: dict, out_fp: pathlib.Path) -> None:
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    with out_fp.open("w") as f:
        json.dump({obj["Pand ID"]: obj}, f, indent=4)

def process_one(fp: pathlib.Path) -> None:
    data = load_json(fp)
    bldg = extract_building(data)
    if bldg:
        classify_surfaces(bldg)
        rel_path = fp.relative_to(INPUT_ROOT)
        out_fp = OUTPUT_ROOT / rel_path
        write_json(bldg, out_fp)

# batch run with multi-threading

def main():
    all_files = list(INPUT_ROOT.rglob("*.json"))
    print(f"Found {len(all_files)} input files.")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(process_one, all_files))

    print("Processing complete.")

if __name__ == "__main__":
    main()


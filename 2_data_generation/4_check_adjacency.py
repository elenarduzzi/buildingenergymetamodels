"""
Label each façade in set of JSONs as either
    - "ADIABATIC"  – wall is shared with a neighbour
    - "EXPOSED"    – wall is exposed to exterior

- Input: thousands of JSONs in one folder
- Multi-threaded per subfolder
- Façade adjacency is isolated within each subfolder only.
"""

import json
import pathlib
from collections import defaultdict
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from shapely.geometry import LineString
from rtree import index as rindex
from tqdm import tqdm

# paths

INPUT_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\3_formatted_nb_surface_json\nb_format_jsons_21")
OUTPUT_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\4_label_adj_json\nb_type_jsons_21")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# adjacency thresholds
TOL2D   = 0.25   # m – max horizontal gap perceived as shared wall
DOT_MIN = 0.95   #    – |dot(n1,n2)| >= DOT_MIN
Z_OVL   = 0.5    #    – 50% vertical overlap

# helpers

def bottom_edge_2d(surf: dict) -> LineString:
    ring = surf["Coordinates"][0]
    zs = [z for x, y, z in ring]
    z_min = min(zs)
    bottom_points = [(x, y) for (x, y, z) in ring if z == z_min]
    if len(bottom_points) < 2:
        return LineString([(ring[0][0], ring[0][1]), (ring[1][0], ring[1][1])])
    return LineString(bottom_points)

def z_range(surf: dict) -> Tuple[float, float]:
    zs = [z for ring in surf["Coordinates"] for *_ , z in ring]
    return min(zs), max(zs)

def unit_normal(surf: dict) -> np.ndarray:
    p0, p1, p2 = surf["Coordinates"][0][:3]
    n = np.cross(np.subtract(p1, p0), np.subtract(p2, p0))
    return n / np.linalg.norm(n)

def vertical_overlap(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    z1, Z1 = a; z2, Z2 = b
    inter = max(0.0, min(Z1, Z2) - max(z1, z2))
    return inter / min(Z1 - z1, Z2 - z2)

def process_subfolder(subfolder: pathlib.Path):
    all_files = list(subfolder.glob("*.json"))
    if not all_files:
        return

    facades = []
    idx = rindex.Index()

    for fp in all_files:
        data = json.loads(fp.read_text())
        bid, bldg = next(iter(data.items()))
        for si, surf in enumerate(bldg["Surfaces"]):
            if surf.get("Type") != "F":
                continue
            line = bottom_edge_2d(surf)
            if line.length == 0:
                continue
            z_rng = z_range(surf)
            n = unit_normal(surf)
            facades.append((bid, si, line, z_rng, n))
            idx.insert(len(facades) - 1, line.buffer(TOL2D).bounds)

    shared: Dict[str, Dict[int, str]] = defaultdict(dict)

    for i, (bid_i, si, line_i, z_i, n_i) in enumerate(facades):
        for j in idx.intersection(line_i.bounds):
            if j <= i:
                continue
            bid_j, sj, line_j, z_j, n_j = facades[j]
            if bid_i == bid_j:
                continue
            dot = np.dot(n_i, n_j)
            if abs(dot) < DOT_MIN or dot > 0:
                continue
            if vertical_overlap(z_i, z_j) < Z_OVL:
                continue
            if line_i.distance(line_j) > TOL2D:
                continue
            shared[bid_i][si] = bid_j
            shared[bid_j][sj] = bid_i

    def label_and_write(fp: pathlib.Path) -> None:
        data = json.loads(fp.read_text())
        bid, bldg = next(iter(data.items()))

        for si, surf in enumerate(bldg["Surfaces"]):
            if surf.get("Type") != "F":
                continue
            if si in shared.get(bid, {}):
                surf["BoundaryCondition"] = "ADIABATIC"
                surf["NeighbourPandID"] = shared[bid][si]
            else:
                surf["BoundaryCondition"] = "EXPOSED"

        rel_path = fp.relative_to(INPUT_ROOT)
        out_fp = OUTPUT_ROOT / rel_path
        out_fp.parent.mkdir(parents=True, exist_ok=True)
        out_fp.write_text(json.dumps(data, indent=2))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(label_and_write, all_files))

# main

def main():
    subfolders = [f for f in INPUT_ROOT.iterdir() if f.is_dir()]
    print(f"Found {len(subfolders)} subfolders.")

    for folder in tqdm(subfolders, desc="Processing subfolders"):
        process_subfolder(folder)

    print(f"adjacency check complete {OUTPUT_ROOT}")

if __name__ == "__main__":
    main()

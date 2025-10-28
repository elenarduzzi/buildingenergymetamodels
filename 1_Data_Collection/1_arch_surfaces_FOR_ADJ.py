"""
takes each single-building 3DBAG JSON,
transforms local-mm vertices to absolute RD-New metres,
extracts LoD1.2 surfaces and attributes,
tags each surface as G / F / R,
writes one clean JSON file per building
"""

import json, pathlib

#  folders 
INPUT_DIR  = pathlib.Path(r"C:\pand_jsons_7")
OUTPUT_DIR = pathlib.Path(r"C:\adj_jsons_7")
OUTPUT_DIR.mkdir(exist_ok=True)

# helpers 
def load_json(fp: pathlib.Path) -> dict:
    with fp.open() as f:
        return json.load(f)

def abs_vertices(raw_vertices, scale, translate):
    # mm to m, then add tile origin
    sx, sy, sz = scale
    tx, ty, tz = translate
    return [[x * sx + tx, y * sy + ty, z * sz + tz] for x, y, z in raw_vertices]

#  core 
def extract_building(data: dict) -> dict | None:
    
    # 1. transform block to absolute vertex list
    tform  = data["metadata"]["transform"]
    verts  = abs_vertices(data["metadata"]["vertices"],
                          tform["scale"], tform["translate"])

    # 2. loop over buildings in file 
    for b in data["buildings"]:
        if not all(k in b for k in ["Pand ID", "Boundaries (LoD 1.2)", "Archetype ID"]):
            continue

        pid          = b["Pand ID"]
        abs70        = None
        h70          = b.get("LoD 1.2 Data", {}).get("Building Height (70%)")
        ground_elev  = b.get("Ground Elevation (NAP)")
        if h70 is not None and ground_elev is not None:
            abs70 = abs(h70 - ground_elev)

        surfaces = []
        for group in b["Boundaries (LoD 1.2)"]:
            for surface in group:
                for ring_group in surface:
                    for ring in ring_group:
                        surfaces.append({"Coordinates": [[verts[i] for i in ring]]})

        return {
            "Pand ID"              : pid,
            "Archetype ID"         : b["Archetype ID"],
            "Construction Year"    : b.get("Construction Year"),
            "Number of Floors"     : b.get("Number of Floors"),
            "Wall Area"            : b.get("Wall Area"),
            "Roof Area (Flat)"     : b.get("Roof Area (Flat)"),
            "Roof Area (Sloped)"   : b.get("Roof Area (Sloped)"),
            "Floor Area"           : b.get("Floor Area"),
            "Shared Wall Area"     : b.get("Shared Wall Area"),
            "Absolute Height (70%)": abs70,
            "Surfaces"             : surfaces,
        }
    return None

def classify_surfaces(building: dict) -> None:
    surfs  = building["Surfaces"]
    z_vals = [z for s in surfs for ring in s["Coordinates"] for *_, z in ring]
    z_min, z_max = min(z_vals), max(z_vals)

    for s in surfs:
        z = [p[2] for ring in s["Coordinates"] for p in ring]
        s["Type"] = "G" if all(v == z_min for v in z) else \
                    "R" if all(v == z_max for v in z) else "F"

def write_json(obj: dict, fp: pathlib.Path) -> None:
    with fp.open("w") as f:
        json.dump({obj["Pand ID"]: obj}, f, indent=4)

# batch run 
for fp in INPUT_DIR.glob("*.json"):
    data = load_json(fp)
    bldg = extract_building(data)
    if bldg:
        classify_surfaces(bldg)
        write_json(bldg, OUTPUT_DIR / fp.name)
        print("saved:", fp.name)
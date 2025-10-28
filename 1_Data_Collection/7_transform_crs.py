"""
transform coordinate reference system (crs)
transform from absolute RD-New metres back to local mm vertices for future handling
"""

import json, pathlib, re
from concurrent.futures import ProcessPoolExecutor

RAW_INT_DIR  = pathlib.Path(r"C:\1B_pand_jsons_8")
METERS_DIR   = pathlib.Path(r"C:\filtered_jsons_8")
OUT_DIR      = pathlib.Path(r"C:\transform_8")
OUT_DIR.mkdir(exist_ok=True)

def load_json(fp):
    with open(fp, encoding="utf-8") as f:
        return json.load(f)

def meters_to_raw(coords, scale, translate):
    sx, sy, sz = scale
    tx, ty, tz = translate
    return [
        [
            int(round((x - tx) / sx)),
            int(round((y - ty) / sy)),
            int(round((z - tz) / sz))
        ]
        for x, y, z in coords
    ]

def process_surfaces(surfaces, scale, translate):
    for surf in surfaces:
        for i, ring in enumerate(surf["Coordinates"]):
            surf["Coordinates"][i] = meters_to_raw(ring, scale, translate)
    return surfaces

def extract_pand_number(meters_fn):
    """Extract Pand number from NL.IMBAG.Pand.0599100000013049.json"""
    match = re.search(r'(\d{16})', meters_fn)
    return match.group(1) if match else None

def process_file(meters_fp_str):
    meters_fp = pathlib.Path(meters_fp_str)
    try:
        meters_data = load_json(meters_fp)
        pand_id = next(iter(meters_data))
        pand_number = extract_pand_number(meters_fp.stem)
        if not pand_number:
            return f"[SKIP] Could not extract Pand number from {meters_fp.name}"
        
        raw_fp = RAW_INT_DIR / f"{pand_number}.json"
        if not raw_fp.exists():
            return f"[SKIP] No raw-int file found for {pand_number}.json"
        
        raw_data = load_json(raw_fp)
        transform = raw_data.get("metadata", {}).get("transform")
        if not transform or "scale" not in transform or "translate" not in transform:
            return f"[SKIP] No transform info in raw-int file for {pand_number}.json"
        
        scale = transform["scale"]
        translate = transform["translate"]
        building = meters_data[pand_id]
        process_surfaces(building["Surfaces"], scale, translate)
        
        out_fp = OUT_DIR / meters_fp.name
        with open(out_fp, "w", encoding="utf-8") as f:
            json.dump({pand_id: building}, f, indent=4)
        return f"[OK]   Transformed: {meters_fp.name}"
    except Exception as e:
        return f"[FAIL]  {meters_fp.name}: {e}"
    
# main

if __name__ == "__main__":
    meters_files = [str(fp) for fp in METERS_DIR.glob("*.json")]
    with ProcessPoolExecutor() as executor:
        for result in executor.map(process_file, meters_files):
            print(result)
    print("crs transformed")

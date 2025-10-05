# rounding values. check missing values. 

import os
import json
from pathlib import Path
import re
from concurrent.futures import ProcessPoolExecutor

# files
input_dir = Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\6_transform_jsons\transform_8")
output_dir = Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\7_clean_jsons\clean_8")

os.makedirs(output_dir, exist_ok=True)

def round_float(val, ndigits=4):
    return round(val, ndigits) if isinstance(val, float) else val

def clean_json_data(data):
    keys_to_round = [
        "Wall Area",
        "Roof Area (Flat)",
        "Roof Area (Sloped)",
        "Floor Area",
        "Shared Wall Area",
        "Absolute Height (70%)"
    ]
    for pid, info in data.items():
        # Rrmove prefix from Pand ID
        if "Pand ID" in info:
            match = re.search(r'(\d{16})', info["Pand ID"])
            if match:
                info["Pand ID"] = match.group(1)
        # round values 
        for key in keys_to_round:
            if key in info and isinstance(info[key], float):
                info[key] = round_float(info[key], 4)
    return data

def process_file(filename):
    if filename.startswith("NL.IMBAG.Pand.") and filename.endswith(".json"):
        new_filename = filename.replace("NL.IMBAG.Pand.", "", 1)
        src = input_dir / filename
        dst = output_dir / new_filename

        try:
            with open(src, "r") as f:
                data = json.load(f)
            data_clean = clean_json_data(data)

            with open(dst, "w") as f:
                json.dump(data_clean, f, indent=2)
            return f"[OK]   Cleaned and saved: {filename}"
        except Exception as e:
            return f"[FAIL] {filename}: {e}"
    return None

if __name__ == "__main__":
    files = os.listdir(input_dir)
    with ProcessPoolExecutor() as executor:
        for result in executor.map(process_file, files):
            if result:
                print(result)
    print("Cleaning, renaming, and copying complete.")


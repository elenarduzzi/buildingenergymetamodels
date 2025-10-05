import os
import json
import pandas as pd
from pathlib import Path
import concurrent.futures
from tqdm import tqdm

""""
A1_base_2020
A2_base_2050
A3_base_2080
B1_retro_2020
B2_retro_2050
B3_retro_2080
"""
# flatten geometry csvs per typology 6, 7, 8, 21 then writes a combines geometry file 

ROOT_INPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\2_add_features\B3_retro_2080")
ROOT_OUTPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\B3_retro_2080")
ROOT_OUTPUT.mkdir(parents=True, exist_ok=True)

combined_csv = ROOT_OUTPUT / "flattened_geometry_all.csv"


INDEXES = [6, 7, 8, 21]

FEATURES = [
    "Pand ID", "Archetype ID", "Construction Year", "Number of Floors",
    "Wall Area", "Roof Area (Flat)", "Roof Area (Sloped)", "Floor Area",
    "Shared Wall Area", "Building Height (70%)", "Building Volume",
    "Total Floor Area", "Compactness Ratio",
    "Annual Heating", "Annual Cooling"
]

def flatten_json(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            jd = json.load(f)
        row = {
            "Pand ID": jd.get("Pand ID", ""),
            "Archetype ID": jd.get("Archetype ID", ""),
            "Construction Year": jd.get("Construction Year", None),
            "Number of Floors": jd.get("Number of Floors", None),
            "Wall Area": round(jd.get("Wall Area", 0), 4),
            "Roof Area (Flat)": round(jd.get("Roof Area (Flat)", 0), 4),
            "Roof Area (Sloped)": round(jd.get("Roof Area (Sloped)", 0), 4),
            "Floor Area": round(jd.get("Floor Area", 0), 4),
            "Shared Wall Area": round(jd.get("Shared Wall Area", 0), 4),
            "Building Height (70%)": round(jd.get("Absolute Height (70%)", 0), 4),
            "Building Volume": round(jd.get("Building Volume", 0), 4),
            "Total Floor Area": round(jd.get("Total Floor Area", 0), 4),
            "Compactness Ratio": round(jd.get("Compactness Ratio", 0), 4),
            "Annual Heating": round(jd.get("Annual Heating", 0), 4),
            "Annual Cooling": round(jd.get("Annual Cooling", 0), 4)
        }
        return row
    except Exception as e:
        return None  # Optionally log e

def process_subfolder(idx):
    input_dir = ROOT_INPUT / f"add_feat_{idx}"
    output_dir = ROOT_OUTPUT / f"flatten_geo_{idx}"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"flattened_geometry_{idx}.csv"

    json_files = list(input_dir.glob("*.json"))
    data = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(flatten_json, json_files),
                            total=len(json_files),
                            desc=f"add_feat_{idx} -> flatten_geo_{idx}"))
        data = [r for r in results if r]

    df = pd.DataFrame(data, columns=FEATURES)
    df.to_csv(csv_path, index=False)
    print(f"Flattened data saved to: {csv_path}")
    return csv_path

if __name__ == "__main__":
    all_csvs = []
    for idx in INDEXES:
        print(f"Processing add_feat_{idx} ...")
        csv_path = process_subfolder(idx)
        all_csvs.append(csv_path)
    print("All per-folder CSVs written.")

    # Merge all per-folder CSVs into one big CSV
    print(f"Combining all subfolder CSVs into: {combined_csv}")

    dfs = [pd.read_csv(csv_path, dtype={"Pand ID": str}) for csv_path in all_csvs]
    merged = pd.concat(dfs, ignore_index=True)
    merged.to_csv(combined_csv, index=False)
    print("All done.")

import json
from collections import OrderedDict
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

# paths
ROOT_INPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\1_enrich_jsons\B3_retro_2080")
ROOT_OUTPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\2_add_features\B3_retro_2080")
INDEXES = [6, 7, 8, 21]

REQUIRED = ["Floor Area", "Absolute Height (70%)", "Number of Floors", "Wall Area"]

def process_file(args):
    json_path, out_dir = args
    log = None
    try:
        with json_path.open(encoding="utf-8") as f:
            data = json.load(f)
        # Validate
        missing = [k for k in REQUIRED if k not in data or data[k] in [None, ""]]
        if missing:
            log = f"{json_path.name}: missing fields: {', '.join(missing)}"
            return log

        try:
            floor_area = float(data["Floor Area"])
            height70   = float(data["Absolute Height (70%)"])
            n_floors   = float(data["Number of Floors"])
            wall_area  = float(data["Wall Area"])
        except Exception as e:
            log = f"{json_path.name}: value conversion error: {e}"
            return log

        building_volume   = floor_area * height70
        total_floor_area  = floor_area * n_floors
        compactness_ratio = wall_area / building_volume if building_volume else None

        data["Building Volume"]    = round(building_volume, 4)
        data["Total Floor Area"]   = round(total_floor_area, 4)
        data["Compactness Ratio"]  = round(compactness_ratio, 4) if compactness_ratio is not None else None

        # Preserve key order, insert after "Absolute Height (70%)"
        ordered = OrderedDict()
        for k, v in data.items():
            ordered[k] = v
            if k == "Absolute Height (70%)":
                ordered["Building Volume"]   = data["Building Volume"]
                ordered["Total Floor Area"]  = data["Total Floor Area"]
                ordered["Compactness Ratio"] = data["Compactness Ratio"]

        out_path = out_dir / json_path.name
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(ordered, f, indent=4, ensure_ascii=False)
        return None
    except Exception as e:
        return f"{json_path.name}: error: {e}"

def process_folder(idx):
    in_dir = ROOT_INPUT / f"enrich_{idx}"
    out_dir = ROOT_OUTPUT / f"add_feat_{idx}"
    out_dir.mkdir(parents=True, exist_ok=True)
    files = list(in_dir.glob("*.json"))
    args_list = [(f, out_dir) for f in files]
    log = []
    with concurrent.futures.ProcessPoolExecutor() as executor, tqdm(total=len(files), desc=f"add_feat_{idx}") as pbar:
        for result in executor.map(process_file, args_list, chunksize=20):
            if result:
                log.append(result)
            pbar.update(1)
    if log:
        log_path = out_dir / "feature_eng_log.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            for entry in log:
                f.write(entry + "\n")
        print(f"Feature engineering issues logged to {log_path}")
    else:
        print(f"add_feat_{idx}: No issues found.")

if __name__ == "__main__":
    for idx in INDEXES:
        print(f"Processing enrich_{idx} ...")
        process_folder(idx)
    print("All done.")


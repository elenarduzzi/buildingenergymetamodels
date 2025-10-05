import os
import json
from pathlib import Path
from tqdm import tqdm
import concurrent.futures

""""
A1_base_2020
A2_base_2050
A3_base_2080
B1_retro_2020
B2_retro_2050
B3_retro_2080
"""

# paths
INDEXES = [6, 7, 8, 21]
ROOT_VERTEX = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\0_vertex_jsons")
ROOT_DEMAND = Path(r"C:\thesis\CLEAN_WORKFLOW\3_demand_out\B3_retro_2080")
ROOT_OUTPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\1_enrich_jsons\B3_retro_2080")

required_fields = [
    "Archetype ID",
    "Construction Year",
    "Number of Floors",
    "Wall Area",
    "Roof Area (Flat)",
    "Roof Area (Sloped)",
    "Floor Area",
    "Shared Wall Area",
    "Absolute Height (70%)",
]
numeric_fields = {
    "Wall Area",
    "Roof Area (Flat)",
    "Roof Area (Sloped)",
    "Floor Area",
    "Shared Wall Area",
    "Absolute Height (70%)",
}

def load_energy_lookup(demands_path):
    with open(demands_path, "r", encoding="utf-8") as f:
        energy_data = json.load(f)["buildings"]
    return {
        str(b["Pand ID"]): {
            "Annual Heating": b.get("Annual Heating", None),
            "Annual Cooling": b.get("Annual Cooling", None),
            "Total Demand": b.get("Total Demand", None),
            "Energy Label": b.get("Energy Label", None),
            "Archetype ID": b.get("Archetype ID", None)
        }
        for b in energy_data
    }

def process_file(args):
    in_path, output_dir, energy_lookup = args
    with open(in_path, "r", encoding="utf-8") as f:
        building_data = json.load(f)

    missing = []
    for field in required_fields:
        val = building_data.get(field, None)
        if val is None or (field in numeric_fields and not isinstance(val, (int, float, str))):
            missing.append(field)
            continue
        if field in numeric_fields:
            try:
                float(val)
            except (TypeError, ValueError):
                missing.append(field)

    if missing:
        pand_id = building_data.get("Pand ID", "UNKNOWN")
        return (pand_id, f"missing: {', '.join(missing)}")

    pand_id = str(building_data.get("Pand ID", "UNKNOWN"))
    pand_id_short = pand_id.split('.')[-1]

    # handles short and full pand if name
    energy = energy_lookup.get(pand_id_short) or energy_lookup.get(pand_id)
    if not energy:
        return (pand_id_short, "No energy data")

    enriched = building_data.copy()
    enriched["Pand ID"] = pand_id_short
    enriched.update(energy)

    out_path = output_dir / f"{pand_id_short}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=4)
    return None

if __name__ == "__main__":
    for idx in INDEXES:
        input_dir = ROOT_VERTEX / f"vertex_{idx}"
        output_dir = ROOT_OUTPUT / f"enrich_{idx}"
        output_dir.mkdir(parents=True, exist_ok=True)
        demands_path = ROOT_DEMAND / f"clean_labels_{idx}.json"

        # Load energy lookup for this index
        energy_lookup = load_energy_lookup(demands_path)

        files = list(input_dir.glob("*.json"))
        args_list = [(f, output_dir, energy_lookup) for f in files]

        validation_log = []
        with concurrent.futures.ProcessPoolExecutor() as executor, tqdm(total=len(files), desc=f"enrich_{idx}") as pbar:
            for result in executor.map(process_file, args_list, chunksize=20):
                if result:
                    validation_log.append(result)
                pbar.update(1)

        if validation_log:
            log_path = output_dir / "validation_log.txt"
            with open(log_path, "w", encoding="utf-8") as log_f:
                for pand_id, issue in validation_log:
                    log_f.write(f"{pand_id}: {issue}\n")
            print(f"Validation issues logged to {log_path}")
        else:
            print(f"enrich_{idx}: No validation issues found.")

    print("All done.")


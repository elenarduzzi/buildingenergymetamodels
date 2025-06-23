import json
import csv
from pathlib import Path

""""
1_baseline.json
A1_base_2020
baseline_materials.csv

1_baseline.json
A2_base_2050
baseline_materials.csv

1_baseline.json
A3_base_2080
baseline_materials.csv

2_retrofit_NI.json
B1_retro_2020
retrofit_materials.csv

2_retrofit_NI.json
B2_retro_2050
retrofit_materials.csv

2_retrofit_NI.json
B3_retro_2080
retrofit_materials.csv

"""

# Input/Output paths
INPUT_MAT  = Path(r"C:\thesis\CLEAN_WORKFLOW\2B_data_out\1_materials\2_retrofit_NI.json")
OUTPUT_ROOT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\B3_retro_2080")
OUTPUT_MAT = OUTPUT_ROOT / "retrofit_materials.csv"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

def get_insulation_by_prefix(materials, prefix):
    """Return insulation value of first material whose ID starts with <prefix>."""
    for mat in materials:
        if str(mat.get("Material ID", "")).startswith(prefix + "."):
            return mat.get("Insulation", "")
    return ""

def get_window_data(materials):
    """Return (U_Factor, SHGC) from the window material dict, or ('','') if missing."""
    for mat in materials:
        if mat.get("Window ID", None):
            return (
                mat.get("U_Factor", ""),
                mat.get("SHGC", "")
            )
    return ("", "")

def flatten_materials(json_path: Path, csv_path: Path) -> None:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for arch_id, specs in data.items():
        mats = specs.get("Materials", [])
        infil = specs.get("Infiltration", "")
        wwr = specs.get("WWR", "")
        if isinstance(infil, (int, float)):
            infil = round(infil, 4)
        if isinstance(wwr, (int, float)):
            wwr = round(wwr, 4)

        u_factor, shgc = get_window_data(mats)

        rows.append({
            "Archetype ID": arch_id,
            "G Insulation": get_insulation_by_prefix(mats, "G"),
            "F Insulation": get_insulation_by_prefix(mats, "F"),
            "R Insulation": get_insulation_by_prefix(mats, "R"),
            "Infiltration": infil,
            "WWR": wwr,
            "U_Factor": u_factor,
            "SHGC": shgc,
        })

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Archetype ID", "G Insulation", "F Insulation", "R Insulation",
                "Infiltration", "WWR", "U_Factor", "SHGC"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} archetypes → {csv_path}")

# Run it
if __name__ == "__main__":
    flatten_materials(INPUT_MAT, OUTPUT_MAT)

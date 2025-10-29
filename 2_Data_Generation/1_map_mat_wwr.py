"""
take input excel with material, construction, window wall ratio (wwr) input data 
convert to JSON for future handling
"""


import pandas as pd
import json
from pathlib import Path


# paths

EXCEL_INPUT  = Path(r"C:\retrofit_NI.xlsx")
JSON_OUTPUT = Path(r"C:\retrofit_NI.json")

df = pd.read_excel(EXCEL_INPUT, sheet_name="materials")
df.columns = [str(col).strip().replace('\n', ' ') for col in df.columns]
df = df.dropna(how='all')
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

structured_data = {}

for _, row in df.iterrows():
    archetype_id = row.get("Archetype ID", "Unknown")
    infiltration = row.get("Infiltration")
    material_id = row.get("Material ID")
    window_id = row.get("Window ID")
    wwr = row.get("WWR")
    wwr_val = round(float(wwr), 4) if pd.notna(wwr) else None  # round to 4 decimals

    if archetype_id not in structured_data:
        structured_data[archetype_id] = {
            "Infiltration": infiltration if pd.notna(infiltration) else None,
            "WWR": wwr_val,
            "Materials": []
        }

    if pd.notna(infiltration):
        structured_data[archetype_id]["Infiltration"] = infiltration
    if wwr_val is not None:
        structured_data[archetype_id]["WWR"] = wwr_val

    if pd.notna(material_id):
        conductivity = row.get("Conductivity")
        conductivity_val = round(float(conductivity), 4) if pd.notna(conductivity) else None
        material_data = {
            "Material ID": material_id,
            "Roughness": row.get("Roughness"),
            "Insulation": row.get("Insulation"),
            "Thickness": row.get("Thickness"),
            "Conductivity": conductivity_val,  # round to 4 decimals
            "Density": row.get("Density"),
            "Specific Heat Capacity": row.get("Specific Heat Capacity")
        }
        structured_data[archetype_id]["Materials"].append(material_data)
    elif pd.notna(window_id):
        window_data = {
            "Window ID": window_id,
            "U_Factor": row.get("U_Factor"),
            "SHGC": row.get("SHGC")
        }
        structured_data[archetype_id]["Materials"].append(window_data)

with open(JSON_OUTPUT, "w") as f:
    json.dump(structured_data, f, indent=2)

print(f"JSON saved to {JSON_OUTPUT}")
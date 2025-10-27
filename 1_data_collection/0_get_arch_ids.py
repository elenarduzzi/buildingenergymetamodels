# takes input pand id and archetype data from excel and outputs to json

import pandas as pd
import json

# paths 
INPUT_EXCEL  = "archetype_6.xlsx"
OUTPUT_JSON = "archetype_6.json"

# select relevant columns
df = pd.read_excel(INPUT_EXCEL)
df_filtered = df[["Pand_ID", "Archetype_ID"]].dropna()

# convert pand id to string to maintain leading zero (example: 0599100000012801)
mapping = {
    str(int(pand_id)).zfill(16): archetype
    for pand_id, archetype in zip(df_filtered["Pand_ID"], df_filtered["Archetype_ID"])
}

# write to json file
with open(OUTPUT_JSON, "w") as json_file:
    json.dump(mapping, json_file, indent=2)

print(f"JSON file created: {OUTPUT_JSON}")
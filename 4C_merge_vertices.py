# COMBINE FLATTENED DATA FROM 4 ARCHETYPES. 

import pandas as pd
from pathlib import Path
from tqdm import tqdm

# configure 
INPUT_FILES = [
    r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\flat_vertex_6\flat_vertex_6.csv",
    r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\flat_vertex_7\flat_vertex_7.csv",
    r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\flat_vertex_8\flat_vertex_8.csv",
    r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\flat_vertex_21\flat_vertex_21.csv",
]
OUTPUT_CSV = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4_merge_vertex\flat_vertex_ALL.csv")

PAD = -1          # value for missing d / ux / uy slots
DEC = 4           # decimal places to keep on real numbers

#  check files to find max column counts 
max_d = max_ux = max_uy = 0
for fp in INPUT_FILES:
    cols = pd.read_csv(fp, nrows=0).columns
    max_d  = max(max_d , sum(c.startswith("d" ) for c in cols))
    max_ux = max(max_ux, sum(c.startswith("ux") for c in cols))
    max_uy = max(max_uy, sum(c.startswith("uy") for c in cols))

# desired full column list in correct order
def make_cols(n, prefix):
    return [f"{prefix}{i+1}" for i in range(n)]

DIST_COLS = make_cols(max_d , "d")
UX_COLS   = make_cols(max_ux, "ux")
UY_COLS   = make_cols(max_uy, "uy")

# read, pad, and store dfs
dfs = []
for fp in tqdm(INPUT_FILES, desc="Padding files", unit="file"):
    df = pd.read_csv(fp, dtype={"Pand ID": "string"})  # Ensures leading zeros are preserved

    # add any missing d / ux / uy cols with PAD value
    for c in DIST_COLS:
        if c not in df.columns:
            df[c] = PAD
    for c in UX_COLS:
        if c not in df.columns:
            df[c] = PAD
    for c in UY_COLS:
        if c not in df.columns:
            df[c] = PAD

    # round numeric d / ux / uy vals (except PAD) to DEC decimals
    for c in DIST_COLS + UX_COLS + UY_COLS:
        df[c] = df[c].apply(lambda v: round(v, DEC) if v != PAD else PAD)

    # uniform column order
    meta_cols = [c for c in df.columns if not (c.startswith("d") or c.startswith("ux") or c.startswith("uy"))]
    df = df[meta_cols + DIST_COLS + UX_COLS + UY_COLS]
    dfs.append(df)

# concat all padded frames row-wise
combined = pd.concat(dfs, ignore_index=True)

# Ensure "Pand ID" is still a string before writing
if "Pand ID" in combined.columns:
    combined["Pand ID"] = combined["Pand ID"].astype(str)

# write out with progress bar
with tqdm(total=len(combined), desc="Writing rows", unit="row") as bar:
    combined.to_csv(OUTPUT_CSV, index=False)
    bar.update(len(combined))

print("\nCombined file saved to:", OUTPUT_CSV)

import dask.dataframe as dd
from sklearn.model_selection import train_test_split
from pathlib import Path
"""
Split a single merged-building CSV into 70 / 10 / 20 train / val / test sets.
One row per Pand ID in the source file (already 1 row / Pand).  
Stratify both splits on *Archetype ID* so each archetypes proportion is
preserved in train, val, and test.


2020
2050
2080
"""

# paths
INPUT_DIR       = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4A_merge_feat\2020")
INPUT_CSV       = INPUT_DIR / "features.csv"

OUTPUT_DIR = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\5_split\2080")
OUTPUT_TRAIN      = OUTPUT_DIR / "train.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "validate.csv"
OUTPUT_TEST       = OUTPUT_DIR / "test.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)



# load dask
df = dd.read_csv(INPUT_CSV, assume_missing=True, dtype={"Pand ID": "object"})

# building level table for stratified split 
buildings = (
    df[["Pand ID", "Archetype ID"]]
    .drop_duplicates()
    .compute()
)

# ─── 3a. first split off 20% test ────────────────────────────────
train_val_bld, test_bld = train_test_split(
    buildings,
    test_size=0.20,  # 20% test
    shuffle=True,
    stratify=buildings["Archetype ID"],
    random_state=42
)

# ─── 3b. split remaining 80% into 70/10 train/val ────────────────
# 10% of *original* is 12.5% of the 80% leftover
train_bld, val_bld = train_test_split(
    train_val_bld,
    test_size=0.125,  # 0.125 * 0.80 = 0.10 of original
    shuffle=True,
    stratify=train_val_bld["Archetype ID"],
    random_state=42
)

train_ids = train_bld["Pand ID"]
val_ids   = val_bld  ["Pand ID"]
test_ids  = test_bld ["Pand ID"]

# ─── 4. filter original rows into train / val / test dask frames ─
train_ddf = df[df["Pand ID"].isin(train_ids)]
val_ddf   = df[df["Pand ID"].isin(val_ids)]
test_ddf  = df[df["Pand ID"].isin(test_ids)]

# ─── 5. write CSVs (single file each) ─────────────────────────────
train_ddf.to_csv(OUTPUT_TRAIN,      single_file=True, index=False)
val_ddf  .to_csv(OUTPUT_VALIDATION, single_file=True, index=False)
test_ddf .to_csv(OUTPUT_TEST,       single_file=True, index=False)

print("\nTrain file      :", OUTPUT_TRAIN)
print("Validation file :", OUTPUT_VALIDATION)
print("Test  file      :", OUTPUT_TEST)

import pandas as pd
from pathlib import Path
from tqdm import tqdm


# combined veretx file should be the same for 2002, 2050, 2080, same pands removed from dataset. 
# paths
SPLITS = ["train", "test", "validate"]
FEATURES_ROOT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\5_split\2080")
VERTEX_CSV = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4C_merge_vertex\flat_vertex_ALL.csv")
OUTPUT_ROOT = FEATURES_ROOT   # Save output alongside input splits

CHUNK = 100_000   # rows per chunk when streaming the large surface file

for split in SPLITS:
    features_csv = FEATURES_ROOT / f"{split}.csv"
    output_csv   = OUTPUT_ROOT  / f"vertex_{split}.csv"
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\nProcessing {split} ...")
    # 1. Load Pand IDs as strings
    pand_ids = pd.read_csv(features_csv, usecols=["Pand ID"], dtype={"Pand ID": "string"})["Pand ID"]
    pand_set = set(pand_ids)
    print(f"Loaded {len(pand_set):,} target Pand IDs for {split}.")

    # 2. Stream-filter the surfaces CSV
    first_write = True
    with tqdm(desc=f"Filtering surface rows ({split})", unit="rows") as pbar:
        for chunk in pd.read_csv(VERTEX_CSV, dtype={"Pand ID": "string"}, chunksize=CHUNK):
            keep_chunk = chunk[chunk["Pand ID"].isin(pand_set)]
            if not keep_chunk.empty:
                keep_chunk.to_csv(output_csv, mode="w" if first_write else "a",
                                  header=first_write, index=False)
                first_write = False
            pbar.update(len(chunk))

    print("Filtered CSV saved to:", output_csv)

print("\nAll splits complete.")

"""
2020
2050
2080
"""
import pandas as pd
from pathlib import Path

# paths
VERTEX_DIR = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\5_split\2080")
FEAT_DIR = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\6_scale\2080")

OUTPUT_DIR = FEAT_DIR


splits = ["train", "validate", "test"]

for split in splits:
    feat_file = FEAT_DIR / f"{split}_scale.csv"
    vertex_file = VERTEX_DIR / f"vertex_{split}.csv"
    output_path = OUTPUT_DIR / f"combined_feat_vertex_{split}.csv"

    # Load datasets
    print(f"Processing {split} ...")
    high_level_df = pd.read_csv(feat_file, dtype={"Pand ID": "string"})
    surface_df    = pd.read_csv(vertex_file, dtype={"Pand ID": "string"})

    # Merge on Pand ID
    merged_df = pd.merge(surface_df, high_level_df, on='Pand ID', how='left')

    # Save to output CSV
    merged_df.to_csv(output_path, index=False)
    print(f"Combined file saved to: {output_path}")

print("All splits processed.")

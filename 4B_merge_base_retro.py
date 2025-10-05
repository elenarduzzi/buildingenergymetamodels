import pandas as pd
from pathlib import Path

""""
2020
baseline_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\A1_base_2020\features.csv")
retrofit_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\B1_retro_2020\features.csv")
output_dir = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4A_merge_feat\2020")
output_csv = output_dir / "features.csv"

2050
baseline_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\A2_base_2050\features.csv")
retrofit_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\B2_retro_2050\features.csv")
output_dir = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4A_merge_feat\2050")
output_csv = output_dir / "features.csv"

2080
baseline_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\A3_base_2080\features.csv")
retrofit_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\B3_retro_2080\features.csv")
output_dir = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4A_merge_feat\2080")
output_csv = output_dir / "features.csv"
"""

# paths


baseline_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\A1_base_2020\features.csv")
retrofit_csv = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\3_flatten_feat\B1_retro_2020\features.csv")
output_dir = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\4A_merge_feat\2020")
output_csv = output_dir / "features.csv"


output_dir.mkdir(parents=True, exist_ok=True)

# Read and append (if memory is an issue, see chunk-based version below)
df1 = pd.read_csv(baseline_csv, dtype=str)
df2 = pd.read_csv(retrofit_csv, dtype=str)

combined = pd.concat([df1, df2], ignore_index=True)
combined.to_csv(output_csv, index=False)

print(f"Combined CSV saved to: {output_csv}")

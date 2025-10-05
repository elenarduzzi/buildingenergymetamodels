"""
SCALE THE COMBINED BASELINE + RETROFIT DATASET

Reads train.csv / val.csv / test.csv, fits:
  - Standard scaler on numeric feature columns
  - Min-max scaler on the two target columns (Heating, Cooling)

Writes CSVs plus joblib scaler dictionaries.

2020
2050
2080

"""

from pathlib import Path
import joblib
import pandas as pd
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm

tqdm.pandas()

# paths
ROOT_INPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\5_clean_split\2020")

PATH_TRAIN      = ROOT_INPUT / "train.csv"
PATH_VALIDATION = ROOT_INPUT / "validate.csv"
PATH_TEST       = ROOT_INPUT / "test.csv"

ROOT_OUTPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\6_clean_scale\2020")

OUT_TRAIN      = ROOT_OUTPUT / "train_scale.csv"
OUT_VALIDATION = ROOT_OUTPUT / "validate_scale.csv"
OUT_TEST       = ROOT_OUTPUT / "test_scale.csv"

SCALER_DIR = ROOT_OUTPUT

ROOT_OUTPUT.mkdir(parents=True, exist_ok=True)

ID_COLS = ["Pand ID", "Archetype ID", "Construction Year"]
STD_COLS = [
    "Number of Floors",	"Wall Area", "Roof Area (Flat)", "Roof Area (Sloped)",
    "Floor Area", "Shared Wall Area", "Building Height (70%)",
    "Building Volume", "Total Floor Area", "Compactness Ratio",
    "G Insulation", "F Insulation", "R Insulation", "Infiltration", 
    "WWR", "U_Factor", "SHGC"
]
TARGET_COLS = ["Annual Heating", "Annual Cooling"]

# Dynamically gather all temp_avg_* and rad_avg_* columns from the train set
def get_const_cols(df):
    return [col for col in df.columns if col.startswith("temp_avg_") or col.startswith("rad_avg_")]

def preprocess(df: pd.DataFrame, const_cols) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    wanted = ID_COLS + TARGET_COLS + STD_COLS + const_cols
    keep   = [c for c in wanted if c in df.columns]
    return df[keep]

print("Loading CSVs …")
train_raw = pd.read_csv(PATH_TRAIN, dtype={c: "string" for c in ID_COLS})
const_cols = get_const_cols(train_raw)
train_df = preprocess(train_raw, const_cols)
val_df   = preprocess(pd.read_csv(PATH_VALIDATION, dtype={c: "string" for c in ID_COLS}), const_cols)
test_df  = preprocess(pd.read_csv(PATH_TEST,      dtype={c: "string" for c in ID_COLS}), const_cols)

# Standard scaling (for features)
def fit_standard_params(s: pd.Series):
    mask = s != -1
    return float(s[mask].mean()), float(s[mask].std(ddof=0) or 1.0)

def standard_transform(s: pd.Series, mean: float, std: float):
    if not is_numeric_dtype(s): return s
    out = s.astype(float).copy()
    mask = out != -1
    out.loc[mask] = (out.loc[mask] - mean) / std
    return out

# Minmax scaling (for targets)
def fit_minmax_params(s: pd.Series):
    mask = s != -1
    vmin = s[mask].min()
    return float(vmin), float(s[mask].max() - vmin or 1.0)

def minmax_transform(s: pd.Series, vmin: float, vrange: float):
    if not is_numeric_dtype(s): return s
    out = s.astype(float).copy()
    mask = out != -1
    out.loc[mask] = (out.loc[mask] - vmin) / vrange
    return out

# Fit scalers on train set only
print("Fitting Standard params …")
std_params = {c: fit_standard_params(train_df[c]) for c in tqdm([c for c in STD_COLS if c in train_df.columns])}

print("Fitting Min-Max params …")
mm_params  = {c: fit_minmax_params(train_df[c])   for c in tqdm(TARGET_COLS)}

SCALER_DIR.mkdir(parents=True, exist_ok=True)
joblib.dump(std_params, SCALER_DIR / "std_params.joblib")
joblib.dump(mm_params,  SCALER_DIR / "mm_params.joblib")

def apply_scaling(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, (m, s) in std_params.items():
        if col in out.columns:
            out[col] = standard_transform(out[col], m, s)
    for col, (vmin, vr) in mm_params.items():
        if col in out.columns:
            out[col] = minmax_transform(out[col], vmin, vr)
    return out

print("Transforming train / val / test …")
train_scaled = apply_scaling(train_df).fillna(-1)
val_scaled   = apply_scaling(val_df).fillna(-1)
test_scaled  = apply_scaling(test_df).fillna(-1)

# Output
print("Writing compressed CSVs …")
OUT_TRAIN.parent.mkdir(parents=True, exist_ok=True)
train_scaled.to_csv(OUT_TRAIN, index=False, na_rep="-1", float_format="%.4f")
val_scaled.to_csv(OUT_VALIDATION, index=False, na_rep="-1", float_format="%.4f")
test_scaled.to_csv(OUT_TEST, index=False, na_rep="-1", float_format="%.4f")

print("\nDone.")
print("Scaled train:     ", OUT_TRAIN)
print("Scaled validation:", OUT_VALIDATION)
print("Scaled test:      ", OUT_TEST)
print("Scaler files saved in:", SCALER_DIR)

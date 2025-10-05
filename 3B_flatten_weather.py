import pandas as pd
from pathlib import Path

""""
NLD_ZH_Rotterdam_TMY_2009-2023.epw
A1_base_2020

MET_DeBilt_TMY_2050.epw
A2_base_2050

MET_DeBilt_TMY_2080.epw
A3_base_2080

NLD_ZH_Rotterdam_TMY_2009-2023.epw
B1_retro_2020

MET_DeBilt_TMY_2050.epw
B2_retro_2050

MET_DeBilt_TMY_2080.epw
B3_retro_2080

"""

# paths
epw_file = Path(r"C:\Users\emily\2B_data_generation\MET_DeBilt_TMY_2080.epw")
output_root = Path(r"C:\Users\emily\4_structure_out\3_flatten_feat\B3_retro_2080")
monthly_csv_out = output_root / "monthly_avgs.csv"

output_root.mkdir(parents=True, exist_ok=True)

# field names (first 14 for relevant cols)
names = [
    "Year","Month","Day","Hour","Minute","DSU",   # 0-5
    "DryBulb",                                    # 6
    "DewPt","RH","Psta",                          # 7-9
    "ETR_H","ETR_DN","IR_Horz","GHI"
] + list(range(14, 35))

# read EPW (skip header)
df = pd.read_csv(epw_file, skiprows=8, header=None, names=names, usecols=["Year", "Month", "Day", "DryBulb", "GHI"])

# calculate daily averages
df["date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
daily = (
    df.groupby(["Year", "Month", "Day"])
      .agg(temp_avg=("DryBulb", "mean"),
           rad_avg=("GHI", lambda x: x.mean() / 1000))  # Wh/m2 -> kWh/m2
      .reset_index()
)

# calculate monthly averages per year
monthly = (
    daily.groupby(["Year", "Month"])
         .agg(temp_avg=("temp_avg", "mean"),
              rad_avg=("rad_avg", "mean"))
         .reset_index()
         .round({"temp_avg": 2, "rad_avg": 4})
)

# write to CSV
monthly.to_csv(monthly_csv_out, index=False, float_format="%.4f")
print(f"Monthly averages written to: {monthly_csv_out}")

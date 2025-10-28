"""
filter jsons files
for adjacency check we collected pand ids that were not on our original list of pand ids
exclude the neighbour pand ids and copy filtered pands to new folder
"""

import pathlib
import shutil
from tqdm import tqdm

# paths

REFERENCE_DIR = pathlib.Path(r"C:\adj_jsons_21")
SOURCE_ROOT = pathlib.Path(r"C:\nb_type_jsons_21")
OUTPUT_ROOT = pathlib.Path(r"C:\filtered_jsons_21")

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# collect all reference pand ids
reference_pids = {fp.stem for fp in REFERENCE_DIR.glob("*.json")}
print(f"Collected {len(reference_pids)} reference Pand IDs.")

# search source subfolders
subfolders = [f for f in SOURCE_ROOT.glob("*_neighbour_ids") if f.is_dir()]
print(f"Found {len(subfolders)} subfolders to process.")

#  copy files without opening 
for subfolder in tqdm(subfolders, desc="Processing subfolders"):
    for json_file in subfolder.glob("*.json"):
        pid = json_file.stem.replace("NL.IMBAG.Pand.", "")
        if pid in reference_pids:
            # Output file path
            out_file = OUTPUT_ROOT / json_file.name
            # Copy raw file
            shutil.copyfile(json_file, out_file)

print(f"JSONs copied to {OUTPUT_ROOT}")

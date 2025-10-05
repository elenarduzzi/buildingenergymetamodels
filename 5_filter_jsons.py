import pathlib
import shutil
from tqdm import tqdm

# files
REFERENCE_DIR = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\0_surface_ADJ_sampled_20k\adj_jsons_21")
SOURCE_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\4_label_adj_json\nb_type_jsons_21")
OUTPUT_ROOT = pathlib.Path(r"C:\thesis\CLEAN_WORKFLOW\2A_adjacency_out\5_filtered_jsons\filtered_jsons_21")

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# collect reference pand ids
reference_pids = {fp.stem for fp in REFERENCE_DIR.glob("*.json")}
print(f"Collected {len(reference_pids)} reference Pand IDs.")

# subfolders
subfolders = [f for f in SOURCE_ROOT.glob("*_neighbour_ids") if f.is_dir()]
print(f"Found {len(subfolders)} subfolders to process.")

# copy files to output folder
for subfolder in tqdm(subfolders, desc="Processing subfolders"):
    for json_file in subfolder.glob("*.json"):
        pid = json_file.stem.replace("NL.IMBAG.Pand.", "")
        if pid in reference_pids:
            # output file path
            out_file = OUTPUT_ROOT / json_file.name
            # copy raw file
            shutil.copyfile(json_file, out_file)

print(f"Done. Raw JSONs copied flat into {OUTPUT_ROOT}")


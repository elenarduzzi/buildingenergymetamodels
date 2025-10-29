import json
from pathlib import Path
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Poly3DCollection
import numpy as np

# paths

INPUT  = Path(r"C:\jsons_6")
OUTPUT = Path(r"C:\adaptive_wwr_6")
MATERIALS_FILE = Path(r"C:\retrofit_NI.json")
ASPECT = 1.6  # window aspect ratio
MIN_WIDTH = 1.5   # min wall width for window [m]
MIN_HEIGHT = 1.5  # min wall height for window [m]

# colours for plots
COLOUR_G = "slategray"
COLOUR_F_EXPOS = "beige"
COLOUR_F_ADIA = "tomato"
COLOUR_R = "black"
COLOUR_WIN = "teal"


# load archetype WWR data 

with open(MATERIALS_FILE, "r") as f:
    material_defs = json.load(f)

def get_archetype_wwr(archetype_id):
    d = material_defs.get(archetype_id, {})
    return float(d.get("WWR", 0.3))  # fallback to 0.3

def load_json(fp: Path):
    with open(fp, "r") as f:
        return json.load(f)

def extract_surfaces_with_labels_and_archetype(data: dict):
    """Returns {pid: (archetype_id, [({rings}, type, boundary), ...])}"""
    out = {}
    for pid, info in data.items():
        archetype = info.get("Archetype ID", None)
        if "Surfaces" not in info or archetype is None:
            continue
        surf_list = []
        for surf in info["Surfaces"]:
            rings = [[tuple(pt) for pt in ring] for ring in surf["Coordinates"]]
            surf_type = surf.get("Type", "")
            boundary = surf.get("BoundaryCondition", "")
            surf_list.append((rings, surf_type, boundary))
        out[pid] = (archetype, surf_list)
    return out

def wall_window_polygon(wall, wwr=0.4, aspect=1.6):
    p = np.array(wall)
    if len(p) != 4:
        return None
    v1 = p[1] - p[0]
    v2 = p[3] - p[0]
    wall_width = np.linalg.norm(v1)
    wall_height = np.linalg.norm(v2)
    win_area = wall_width * wall_height * wwr
    win_height = (win_area / aspect) ** 0.5
    win_width = win_height * aspect
    if win_width > wall_width or win_height > wall_height:
        return None
    offset1 = 0.5 - win_width / (2 * wall_width)
    offset2 = 0.5 - win_height / (2 * wall_height)
    o = p[0] + offset1 * v1 + offset2 * v2
    win_p1 = o
    win_p2 = o + win_width * v1 / wall_width
    win_p3 = o + win_width * v1 / wall_width + win_height * v2 / wall_height
    win_p4 = o + win_height * v2 / wall_height
    return [tuple(win_p1), tuple(win_p2), tuple(win_p3), tuple(win_p4)]

def should_place_window(wall, min_width=MIN_WIDTH, min_height=MIN_HEIGHT):
    p = np.array(wall)
    if len(p) != 4:
        return False
    v1 = p[1] - p[0]
    v2 = p[3] - p[0]
    wall_width = np.linalg.norm(v1)
    wall_height = np.linalg.norm(v2)
    return (wall_width >= min_width) and (wall_height >= min_height)

def plot_buildings_with_windows(buildings):
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for pid, (archetype_id, surfs) in buildings.items():
        wwr = get_archetype_wwr(archetype_id)
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="3d")
        for rings, surf_type, boundary in surfs:
            color = (
                COLOUR_G if surf_type == "G"
                else COLOUR_R if surf_type == "R"
                else COLOUR_F_ADIA if boundary.upper() == "ADIABATIC"
                else COLOUR_F_EXPOS
            )
            poly = Poly3DCollection(rings, color=color, edgecolor="k", alpha=0.5, linewidths=0.7)
            ax.add_collection3d(poly)
            if surf_type == "F" and boundary.upper() == "EXPOSED":
                wall_ring = rings[0]
                if len(wall_ring) == 4 and should_place_window(wall_ring):
                    win_poly = wall_window_polygon(wall_ring, wwr, ASPECT)
                    if win_poly:
                        win_patch = Poly3DCollection([win_poly], color=COLOUR_WIN, edgecolor="b", alpha=0.8, linewidths=2)
                        ax.add_collection3d(win_patch)
        ax.set_xlabel("X"); ax.set_ylabel("Y"); ax.set_zlabel("Z")
        ax.set_box_aspect([1,1,1])
        plt.title(f"Pand.{pid.split('.')[-1]}")
        plt.tight_layout()
        plt.savefig(OUTPUT / f"{pid.split('.')[-1]}.png")
        plt.close()
    print(f"Saved plots to {OUTPUT}")

# run 

json_files = [p for p in INPUT.iterdir() if p.suffix.lower() == ".json"]
if not json_files:
    raise RuntimeError(f"No .json files found in {INPUT}")

buildings = {}
for fp in json_files:
    buildings.update(extract_surfaces_with_labels_and_archetype(load_json(fp)))
if not buildings:
    raise RuntimeError("No building surfaces found")

plot_buildings_with_windows(buildings)

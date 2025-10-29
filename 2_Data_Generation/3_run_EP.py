"""
batch run energyplus simulations
for thousands of input IDF files
use multiprocessing to speed up simulation process
"""

import os, shutil, subprocess, tempfile, time
from multiprocessing import Pool, cpu_count
from pathlib import Path


idf_folder   = Path(r"C:\idf_21")
output_root  = Path(r"C:\sims_21")
log_file     = output_root / "ep_log_21.txt"

eplus_exe    = Path(r"C:\EnergyPlusV24-2-0\energyplus.exe")


"""
# update epw weather file per scenario (2020, 2050, 2080)

inputs files:

NLD_ZH_Rotterdam_TMY_2009-2023.epw
MET_DeBilt_TMY_2050.epw
MET_DeBilt_TMY_2080.epw

"""
# B1 retrofit / 2020 (ROTTERDAM BASE FILE)

epw_path     = Path(r"C:\NLD_ZH_Rotterdam_TMY_2009-2023.epw")

output_root.mkdir(parents=True, exist_ok=True)


"""
ensure all files that are not .eso / .err.
already implemted in IDF generation, but included as safety.
"""

KEEP = {"eplusout.eso", "eplusout.err"}

def keep_only_eso_err(folder: Path) -> None:

    for fp in folder.iterdir():
        if fp.name not in KEEP:
            try:
                fp.unlink()
            except Exception as exc:
                print(f"[WARNING] Could not delete {fp.name}: {exc}")

# run energyplus simulations per building (pand id)                

def run_simulation(idf_file: str) -> int:
    pand_name   = Path(idf_file).stem
    pand_output = output_root / pand_name
    pand_output.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory() as td:
        shutil.copy(idf_file, Path(td, "in.idf"))

        cmd = [
            str(eplus_exe),
            "--weather",    str(epw_path),
            "--output-directory", str(pand_output),
            "--annual",
            "--expandobjects",
            "in.idf",
        ]
        try:
            subprocess.run(cmd, cwd=td, check=True, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True)
            keep_only_eso_err(pand_output)
            return 1  # Success
        except subprocess.CalledProcessError as e:
            (pand_output / "python_subprocess.log").write_text(e.stdout)
            return 0  # Failure

def main() -> None:
    idfs = [str(p) for p in idf_folder.glob("*.idf")]
    if not idfs:
        print("No IDF files found – nothing to simulate.")
        return

    n_workers = max(1, cpu_count()-1)
    print(f"Running {len(idfs)} IDFs using {n_workers} workers …")

    start_time = time.time()
    with Pool(n_workers) as pool:
        results = pool.map(run_simulation, idfs)
    elapsed = time.time() - start_time

    num_success = sum(results)
    num_total = len(results)
    print(f"\nSimulations completed: {num_success} / {num_total} successful")
    print(f"Total time: {elapsed:.1f} seconds ({elapsed/60:.2f} min)")

    # write log file
    with open(log_file, "w") as f:
        f.write("EnergyPlus simulation summary\n")
        f.write(f"Total simulations attempted: {num_total}\n")
        f.write(f"Successful simulations:     {num_success}\n")
        f.write(f"Total time: {elapsed:.1f} seconds ({elapsed/60:.2f} min)\n")

    print(f"Log file written to: {log_file}")

# run

if __name__ == "__main__":
    main()

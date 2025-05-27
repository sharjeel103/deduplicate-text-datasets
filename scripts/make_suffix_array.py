```python
# Modified for Kaggle WSL-compatible suffix array creation
# Edits:
# 1. Forced total_jobs and jobs_at_once to 16 for all data sizes
# 2. Switched binary path to release: target/release/dedup_dataset
# 3. Redirected tmp output to /kaggle/working/tmp (writable)
# 4. Ensured TMP_DIR exists
# 5. Added OUTPUT_TABLE argument and replacement logic

import os
import time
import sys
import multiprocessing as mp
import numpy as np

# Input arguments
#   sys.argv[1]: data file path
#   sys.argv[2]: output table binary path (optional)
DATA_FILE = sys.argv[1]
OUTPUT_TABLE = sys.argv[2] if len(sys.argv) > 2 else f"{DATA_FILE}.table.bin"

# Setup temporary directory in writable Kaggle working path
TMP_DIR = "/kaggle/working/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# Force consistent parallelism
total_jobs = 16
jobs_at_once = 16
HACK = 100000  # overlap bytes to catch boundary duplicates

# Compute data size and chunk size
data_size = os.path.getsize(DATA_FILE)
S = data_size // total_jobs
started = []

# STEP 1: Generate suffix-array parts
for jobstart in range(0, total_jobs, jobs_at_once):
    wait = []
    for i in range(jobstart, min(jobstart + jobs_at_once, total_jobs)):
        s = i * S
        e = min((i + 1) * S + HACK, data_size)
        part_file = f"{DATA_FILE}.part.{s}-{e}"
        cmd = (
            f"./target/release/dedup_dataset make-part"
            f" --data-file {DATA_FILE} --start-byte {s} --end-byte {e}"
        )
        print(cmd)
        started.append((s, e, part_file))
        wait.append(os.popen(cmd))
        if e == data_size:
            break
    print("Waiting for jobs to finish...")
    [p.read() for p in wait]

# STEP 2: Verify parts and rerun failures
def verify_parts(parts):
    failed = []
    for s, e, part_file in parts:
        table_file = f"{part_file}.table.bin"
        if (
            not os.path.exists(part_file)
            or not os.path.exists(table_file)
            or os.path.getsize(table_file) == 0
        ):
            failed.append((s, e, part_file))
    return failed

print("Verifying part files...")
while True:
    failed = verify_parts(started)
    if not failed:
        break
    print(f"Rerunning {len(failed)} failed jobs...")
    wait = []
    for s, e, _ in failed:
        cmd = (
            f"./target/release/dedup_dataset make-part"
            f" --data-file {DATA_FILE} --start-byte {s} --end-byte {e}"
        )
        print(cmd)
        wait.append(os.popen(cmd))
    [p.read() for p in wait]
    time.sleep(1)

# STEP 3: Merge suffix trees
print("Merging suffix trees...")
# Clean previous merges in TMP_DIR
for f in os.listdir(TMP_DIR):
    if f.startswith("out.table.bin"):
        os.remove(os.path.join(TMP_DIR, f))

files = [f"{DATA_FILE}.part.{s}-{e}" for s, e, _ in started]
suffix_arg = " --suffix-path ".join(files)
merge_cmd = (
    f"./target/release/dedup_dataset merge"
    f" --output-file {os.path.join(TMP_DIR, 'out.table.bin')}"
    f" --suffix-path {suffix_arg} --num-threads {mp.cpu_count()}"
)
print(merge_cmd)
pipe = os.popen(merge_cmd)
if pipe.close() is not None:
    print("Merge failed. Ensure ulimit -Sn is sufficient.")
    sys.exit(1)

# STEP 4: Finalize and move table
print("Finalizing table binary...")
final_tmp = os.path.join(TMP_DIR, 'out.table.bin')
os.replace(final_tmp, OUTPUT_TABLE)

# STEP 5: Validate
if not os.path.exists(OUTPUT_TABLE) or os.path.getsize(OUTPUT_TABLE) % data_size != 0:
    print("Error: Output table binary invalid.")
    sys.exit(1)

print(f"Suffix array built successfully: {OUTPUT_TABLE}")
```

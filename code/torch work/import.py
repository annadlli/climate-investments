import os
from datetime import datetime
import deweydatapy as ddp

# -------------------------
# API key
# -------------------------
apikey = "akv1_rmWv0DXQ-2jsoB2tLp9csaYBx7X_xIWBYg6"

# -------------------------
# API endpoints
# -------------------------
#attom = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_ks3rkz8mtfkruiio"
#builty = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_garggg9vj69evxb3"

apis = {
    "as":    "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_kzrvefep8ocd7anv",
    "fl":    "https://api.deweydata.io/api/v1/external/data/prj_yztqvhz7__cdst_iwgbxnjzqsqarvog",
    "fllee": "https://api.deweydata.io/api/v1/external/data/prj_yztqvhz7__cdst_aeyicdusvevj69md",
    "ctok":  "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_v8ncwsgi9oa7prnx",
}

# -------------------------
# Base directory (HPC)
# -------------------------
base_dir = "/scratch/adl9602/tx"
data_dir = os.path.join(base_dir, "data")

# -------------------------
# Create run folder
# -------------------------
run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
run_dir = os.path.join(data_dir, run_id)

print(f"Run directory: {run_dir}")

# -------------------------
# Download each API into its own subfolder
# -------------------------
for name, url in apis.items():
    out_dir = os.path.join(run_dir, name)
    os.makedirs(out_dir, exist_ok=True)

    print(f"Fetching {name} file list...")
    files = ddp.get_file_list(apikey, url, print_info=True)

    print(f"Downloading {name} files to {out_dir}...")
    ddp.download_files(files, out_dir)

print("All downloads complete.")

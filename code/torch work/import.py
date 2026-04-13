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
attom = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_ks3rkz8mtfkruiio"
builty = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_garggg9vj69evxb3"

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

attom_dir = os.path.join(run_dir, "attom")
builty_dir = os.path.join(run_dir, "builty")

os.makedirs(attom_dir, exist_ok=True)
os.makedirs(builty_dir, exist_ok=True)

print(f"Run directory: {run_dir}")

# -------------------------
# Download ATTOM
# -------------------------
print("Fetching ATTOM file list...")
attom_files = ddp.get_file_list(apikey, attom, print_info=True)

print("Downloading ATTOM files...")
ddp.download_files(attom_files, attom_dir)

# # -------------------------
# # Download BUILTY
# # -------------------------
# print("Fetching BUILTY file list...")
# builty_files = ddp.get_file_list(apikey, builty, print_info=True)

# print("Downloading BUILTY files...")
# ddp.download_files(builty_files, builty_dir)

# print("All downloads complete.")
# setup
import numpy as np
import pandas as pd
import scipy as scipy
import matplotlib.pyplot as plt
import deweydatapy as ddp
import re
import os

apikey = "akv1_rmWv0DXQ-2jsoB2tLp9csaYBx7X_xIWBYg6"

# la = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_g3u7q96qptfdkkbp"
# va = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_garggg9vj69evxb3"
# mo = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_4vhkmikfsqyhcgbj"
# nc = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_wcgxgkigfbaqiiwg"
#nj = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_gkawycobzibu8cnc"
tx = "https://api.deweydata.io/api/v1/external/data/prj_hgemyv4u__cdst_e6hfaj8ebh8k8ogp"

for state_name, endpoint in [("tx", tx)]:  # ("nj", nj), ("la", la), ("va", va), ("mo", mo), ("nc", nc)
    files_df = ddp.get_file_list(apikey, endpoint, print_info=True)

    output_dir = os.path.join("data", state_name)
    os.makedirs(output_dir, exist_ok=True)
    print(output_dir)

    ddp.download_files(files_df, output_dir)
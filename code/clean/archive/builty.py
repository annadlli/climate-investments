# setup
import numpy as np
import pandas as pd
import scipy as scipy
import matplotlib.pyplot as plt
import deweydatapy as ddp
import re
import os

apikey = "DEWEY_API_KEY_PLACEHOLDER"

# la = "DEWEY_ENDPOINT_URL_PLACEHOLDER"
# va = "DEWEY_ENDPOINT_URL_PLACEHOLDER"
# mo = "DEWEY_ENDPOINT_URL_PLACEHOLDER"
# nc = "DEWEY_ENDPOINT_URL_PLACEHOLDER"
#nj = "DEWEY_ENDPOINT_URL_PLACEHOLDER"
tx = "DEWEY_ENDPOINT_URL_PLACEHOLDER"

for state_name, endpoint in [("tx", tx)]:  # ("nj", nj), ("la", la), ("va", va), ("mo", mo), ("nc", nc)
    files_df = ddp.get_file_list(apikey, endpoint, print_info=True)

    output_dir = os.path.join("data", state_name)
    os.makedirs(output_dir, exist_ok=True)
    print(output_dir)

    ddp.download_files(files_df, output_dir)
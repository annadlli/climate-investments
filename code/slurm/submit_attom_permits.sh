#!/bin/bash -l
#
# Authors: Anna Li
# Date: 2026-10-04
#
# Cluster wrapper for the ATTOM building-permit stage, every state: download from Dewey
# and combine, extract the elevation candidates, count permits for coverage, screen to
# home elevations, geocode the screened addresses, and print the counts by state.
# Claude change 10-04: new file.
# Claude change 10-12: the manifest now carries the four permit status-history endpoints
# (table = status). FROM_STEP=1 reruns the whole chain: step 1 skips the two permit tables
# already combined and downloads only the status tables; steps 2-5 rebuild with the new
# status columns (date_filed, date_finaled, ... on permits_raw/, year_* and completed on
# permit_elevations.dta).
#
# Copy these into ${PROJECT_ROOT} (flat, next to this script), then: sbatch submit_attom_permits.sh
#   submit_attom_permits.sh        code/slurm/
#   import_attom_permits.py        code/prepare/
#   extract_attom_permits.py       code/prepare/
#   clean_permit_coverage.py       code/clean/
#   clean_permits.do               code/clean/
#   geocode_permits.py             code/clean/
#   attom_permits_endpoints.csv    anna_private/   (licensed Dewey URLs, not in git)
#   api_keys.env                   already on the cluster; must define DEWEY_API_KEY
#   data/clean/cpi.dta             from Dropbox clean/ (clean_permits.do deflates job values)
#
# Each step is skipped when its output exists, so a failed job resumes where it stopped.
# FROM_STEP=N sbatch submit_attom_permits.sh reruns steps N.. (1 import, 2 extract,
# 3 coverage, 4 screen, 5 geocode). STATES="FL LA TX" restricts steps 2-4 (default ALL).
# Step 4 needs Stata: set STATA_MODULE (e.g. stata/17.0, from "module spider stata") and, if
# needed, STATA to the executable; without Stata the job stops after step 3 and clean_permits.do can be run
# elsewhere on data/clean/permits_raw/.

#SBATCH --job-name=attom_permits
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=48:00:00
#SBATCH --mem=96GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_attom_permits.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_attom_permits.err

set -euo pipefail
export PYTHONUNBUFFERED=1

ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA="${DATA_ROOT:-${ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
CODE_DIR="${CODE_DIR:-${SLURM_SUBMIT_DIR:-${ROOT}}}"
STATES="${STATES:-ALL}"
FROM_STEP="${FROM_STEP:-}"
MEMORY="${DUCKDB_MEMORY:-72GB}"
THREADS="${SLURM_CPUS_PER_TASK:-8}"
TMP="${ROOT}/tmp/attom_permits/${SLURM_JOB_ID:-manual}"

if [[ -f "${ROOT}/api_keys.env" ]]; then
    set -a; source "${ROOT}/api_keys.env"; set +a
fi
mkdir -p "${DATA}/raw/attom_permits" "${DATA}/clean/permits_raw" "${DATA}/clean/permit_states" "${TMP}"

step_needed() {   # number, output that marks the step done
    if [[ -e "$2" && ( -z "${FROM_STEP}" || "$1" -lt "${FROM_STEP}" ) ]]; then
        echo "[$1/5] skip -- output exists: $2"; return 1
    fi
    return 0
}

# 1. Download every endpoint in the manifest and combine each into one parquet (monthly
#    files deleted after); permit tables -> attom_permits_<name>, status tables -> attom_permit_status_<name>
if step_needed 1 "${DATA}/raw/attom_permits/.import_done"; then
    echo "[1/5] Dewey download and combine"
    "${PYTHON}" "${CODE_DIR}/import_attom_permits.py" --data "${DATA}" \
        --manifest "${CODE_DIR}/attom_permits_endpoints.csv" \
        --memory "${MEMORY}" --threads "${THREADS}" --tmp "${TMP}"
    touch "${DATA}/raw/attom_permits/.import_done"
fi

# 2. Elevation candidates, one CSV per state; the log carries all-permit counts by state
if step_needed 2 "${DATA}/clean/permits_raw/.extract_done"; then
    echo "[2/5] Extract elevation candidates (${STATES})"
    "${PYTHON}" "${CODE_DIR}/extract_attom_permits.py" --data "${DATA}" --states "${STATES}" --memory "${MEMORY}"
    touch "${DATA}/clean/permits_raw/.extract_done"
fi

# 3. All-permit counts by county, ZIP and city x year (coverage)
if step_needed 3 "${DATA}/clean/permit_coverage_county.dta"; then
    echo "[3/5] Permit coverage (${STATES})"
    "${PYTHON}" "${CODE_DIR}/clean_permit_coverage.py" --data "${DATA}" --states "${STATES}" --memory "${MEMORY}"
fi

# 4. Screen to home elevations and collapse to one row per address (Stata)
if step_needed 4 "${DATA}/clean/permit_elevations.dta"; then
    # STATA_MODULE names the module when the cluster needs a version, e.g. STATA_MODULE=stata/17.0
    module load "${STATA_MODULE:-stata}" 2>/dev/null || true
    STATA="${STATA:-$(command -v stata-mp || command -v stata-se || command -v stata || true)}"
    if [[ -z "${STATA}" ]]; then
        echo "[4/5] no Stata on this node: set STATA=/path/to/stata-mp and resubmit, or run clean_permits.do elsewhere" >&2
        exit 3
    fi
    [[ -s "${DATA}/clean/cpi.dta" ]] || { echo "[4/5] missing ${DATA}/clean/cpi.dta" >&2; exit 2; }
    # the states with a candidate file, upper case, as clean_permits.do expects
    SCREEN_STATES="$(cd "${DATA}/clean/permits_raw" && ls *.csv | sed 's/\.csv$//' | tr '[:lower:]' '[:upper:]' | tr '\n' ' ')"
    echo "[4/5] Screen (${SCREEN_STATES})"
    mkdir -p "${TMP}/stata"
    # Stata splits a batch argument that contains spaces, so the call goes through a do-file
    echo "do \"${CODE_DIR}/clean_permits.do\" \"${DATA}\" \"${SCREEN_STATES% }\"" > "${TMP}/stata/run_clean_permits.do"
    (cd "${TMP}/stata" && "${STATA}" -b do run_clean_permits.do)
    cp "${TMP}/stata/run_clean_permits.log" "${ROOT}/logs_out/${SLURM_JOB_ID:-manual}_clean_permits.log"
    # Stata exits 0 on an error in batch mode; the log is the only signal
    if grep -q "^r([0-9]*);" "${TMP}/stata/run_clean_permits.log"; then
        echo "[4/5] clean_permits.do failed, see ${ROOT}/logs_out/${SLURM_JOB_ID:-manual}_clean_permits.log" >&2
        exit 4
    fi
fi

# 5. ZIP fill, block group and coordinates from the Census geocoder (needs internet)
if step_needed 5 "${DATA}/clean/permit_elevations_zipfilled.dta"; then
    echo "[5/5] Census geocode of the screened addresses"
    "${PYTHON}" "${CODE_DIR}/geocode_permits.py" --data "${DATA}"
fi

# Counts by state, largest first
"${PYTHON}" - "${DATA}/clean/permit_elevations.dta" <<'PY'
import sys
import pandas as pd
d = pd.read_stata(sys.argv[1], columns=["state", "retrofit", "new_construction", "attomid", "n_permits"])
d["with_attomid"] = d.attomid.astype(str).str.strip().ne("")
t = d.groupby("state").agg(permits=("n_permits", "sum"), addresses=("retrofit", "size"), retrofit_addresses=("retrofit", "sum"),
                           new_construction=("new_construction", "sum"), with_attomid=("with_attomid", "sum"))
print(t.sort_values("retrofit_addresses", ascending=False).to_string())
print(t.sum().to_string())
PY
echo "=== done: ${DATA}/clean/permit_elevations_zipfilled.dta ==="

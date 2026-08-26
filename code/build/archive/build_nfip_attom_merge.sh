#!/bin/bash -l

#SBATCH --job-name=nfip_attom
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00
#SBATCH --mem=128GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_nfip_attom.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_nfip_attom.err

# Merge geocoded ATTOM value cells onto the NFIP policy universe, per state.
# Thin cluster wrapper around build_nfip_attom_merge.py, which master.do runs
# locally with the same arguments.
#
# Test one state first:
#   STATES="VT" sbatch build_nfip_attom_merge.sh
# Then all of them, one state per array task (states are independent, and TX/FL
# are large enough that a serial loop risks the wall clock):
#   sbatch --array=0-19 build_nfip_attom_merge.sh
# Submit selected array indices:
#   sbatch --array=17,19 build_nfip_attom_merge.sh   # TX + VA
#
# Without --array the script falls back to looping over STATES in one job.
#
# Needs clean/nfip_policies_state/{st}.dta on the cluster (clean_nfip_policies.do
# runs in Stata, which the cluster does not have - upload the .dta files).

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
TMP_DIR="${TMP_DIR:-${PROJECT_ROOT}/tmp}"
THREADS="${SLURM_CPUS_PER_TASK:-4}"
MEMORY="${DUCKDB_MEMORY:-96GB}"
MAX_TEMP="${DUCKDB_MAX_TEMP:-800GB}"
BUILD_SCRIPT="${BUILD_SCRIPT:-${PROJECT_ROOT}/build_nfip_attom_merge.py}"
# Block-group vintage on the ATTOM side and the ATTOM property-type screen.
BG_VINTAGE="${BG_VINTAGE:-current}"
USE_CODES="${USE_CODES:-385}"
# Space-separated 2-letter codes; override e.g. STATES="VT" sbatch ...
STATES="${STATES:-AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA}"

# Under --array, each task takes one state by index; otherwise loop over them all.
read -r -a STATE_LIST <<< "${STATES}"
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    STATE_LIST=("${STATE_LIST[${SLURM_ARRAY_TASK_ID}]}")
fi

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${TMP_DIR}" "${DATA_ROOT}/build"

for STATE in "${STATE_LIST[@]}"; do
    STATE_LOWER="$(echo "${STATE}" | tr '[:upper:]' '[:lower:]')"

    ATTOM="${DATA_ROOT}/build/${STATE_LOWER}_attom_geocoded.parquet"
    if [[ ! -f "${ATTOM}" ]]; then
        echo "Skipping ${STATE}: no geocoded ATTOM panel at ${ATTOM}"
        continue
    fi

    NFIP="${DATA_ROOT}/clean/nfip_policies_state/${STATE_LOWER}.dta"
    if [[ ! -f "${NFIP}" ]]; then
        echo "Skipping ${STATE}: no cleaned NFIP policy file at ${NFIP}"
        continue
    fi

    OUT="${DATA_ROOT}/build/${STATE_LOWER}_nfip_attom.parquet"
    echo "Processing state: ${STATE}"
    if ! "${PYTHON}" "${BUILD_SCRIPT}" \
        --state "${STATE}" \
        --data "${DATA_ROOT}" \
        --nfip "${NFIP}" \
        --attom "${ATTOM}" \
        --out "${OUT}" \
        --bg-vintage "${BG_VINTAGE}" \
        --use-codes "${USE_CODES}" \
        --tmp "${TMP_DIR}" \
        --threads "${THREADS}" \
        --memory "${MEMORY}" \
        --max-temp "${MAX_TEMP}"; then
        echo "FAILED ${STATE} — continuing with remaining states"
    fi
done

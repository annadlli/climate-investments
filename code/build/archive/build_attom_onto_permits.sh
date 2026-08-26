#!/bin/bash -l

#SBATCH --job-name=attom_permits
#SBATCH --array=0-19%3
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=12:00:00
#SBATCH --mem=128GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%A_%a_attom_permits.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%A_%a_attom_permits.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
TMP_DIR="${TMP_DIR:-${PROJECT_ROOT}/tmp}"
THREADS="${SLURM_CPUS_PER_TASK:-4}"
MEMORY="${DUCKDB_MEMORY:-96GB}"
MAX_TEMP="${DUCKDB_MAX_TEMP:-800GB}"
BUILD_SCRIPT="${BUILD_SCRIPT:-${PROJECT_ROOT}/build_attom_onto_permits.py}"
PERMITS="${PERMITS:-${DATA_ROOT}/build/builty_elevations_zipfilled.dta}"
OUT_DIR="${OUT_DIR:-${DATA_ROOT}/build/builty_attom}"

# One state per array task. Existing five-state outputs are skipped unless
# FORCE=1. Array indices follow this fixed order.
STATES=(AL CT DE FL GA LA MA MD ME MS NC NH NJ NY PA RI SC TX VA VT)
STATE="${STATES[${SLURM_ARRAY_TASK_ID:-0}]}"
FORCE="${FORCE:-0}"

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${TMP_DIR}" "${OUT_DIR}"

STATE_LOWER="$(echo "${STATE}" | tr '[:upper:]' '[:lower:]')"
STATE_TMP="${TMP_DIR}/builty_attom/${STATE_LOWER}"
OUT="${OUT_DIR}/${STATE_LOWER}_attom_permits_final.parquet"
DIAGNOSTICS="${OUT_DIR}/${STATE_LOWER}_attom_permits_final_diagnostics.csv"
mkdir -p "${STATE_TMP}"

ATTOM="${DATA_ROOT}/build/${STATE_LOWER}_attom_geocoded.parquet"
[[ -f "${BUILD_SCRIPT}" ]] || { echo "Missing script: ${BUILD_SCRIPT}" >&2; exit 2; }
[[ -f "${PERMITS}" ]] || { echo "Missing canonical Builty file: ${PERMITS}" >&2; exit 2; }
[[ -f "${ATTOM}" ]] || { echo "Missing geocoded ATTOM file: ${ATTOM}" >&2; exit 2; }

if [[ "${FORCE}" == "0" && -s "${OUT}" ]]; then
    echo "Skipping ${STATE}: ${OUT} already exists"
    exit 0
fi

echo "Processing state: ${STATE}"
"${PYTHON}" "${BUILD_SCRIPT}" \
    --state "${STATE}" \
    --data "${DATA_ROOT}" \
    --permits "${PERMITS}" \
    --attom "${ATTOM}" \
    --out "${OUT}" \
    --diagnostics "${DIAGNOSTICS}" \
    --tmp "${STATE_TMP}" \
    --threads "${THREADS}" \
    --memory "${MEMORY}" \
    --max-temp "${MAX_TEMP}"

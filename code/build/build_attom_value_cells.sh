#!/bin/bash -l

#SBATCH --job-name=attom_values
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=8:00:00
#SBATCH --mem=220GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_attom_values.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_attom_values.err

set -euo pipefail

STATE="${1:-TX}"
STATE_LOWER="$(echo "${STATE}" | tr '[:upper:]' '[:lower:]')"

# Put the synced project, data folders, logs, and temp files under this one
# Torch directory. Override on submission with PROJECT_ROOT=... if needed.
PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
ATTOM_PATH="${ATTOM_PATH:-${DATA_ROOT}/${STATE_LOWER}/attom_${STATE_LOWER}.parquet}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
TMP_DIR="${TMP_DIR:-${PROJECT_ROOT}/tmp}"
THREADS="${SLURM_CPUS_PER_TASK:-4}"
MEMORY="${DUCKDB_MEMORY:-200GB}"
MAX_TEMP="${DUCKDB_MAX_TEMP:-500GB}"

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${TMP_DIR}" "${DATA_ROOT}/build"

"${PYTHON}" "${PROJECT_ROOT}/code/build/build_attom_value_cells.py" \
  --state "${STATE}" \
  --data "${DATA_ROOT}" \
  --attom "${ATTOM_PATH}" \
  --tmp "${TMP_DIR}" \
  --threads "${THREADS}" \
  --memory "${MEMORY}" \
  --max-temp "${MAX_TEMP}"

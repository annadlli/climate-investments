#!/bin/bash -l

#SBATCH --job-name=attom_values
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --time=4:00:00
#SBATCH --mem=64GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_attom_values.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_attom_values.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
TMP_DIR="${TMP_DIR:-${PROJECT_ROOT}/tmp}"
THREADS="${SLURM_CPUS_PER_TASK:-2}"
MEMORY="${DUCKDB_MEMORY:-48GB}"
MAX_TEMP="${DUCKDB_MAX_TEMP:-800GB}"
BUILD_SCRIPT="${PROJECT_ROOT}/build_attom_value_cells.py"

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${TMP_DIR}" "${DATA_ROOT}/build"

# Discover states from subdirectories that contain a matching parquet file
for STATE_DIR in "${DATA_ROOT}"/*/; do
    STATE_LOWER="$(basename "${STATE_DIR}")"
    PARQUET="${STATE_DIR}attom_${STATE_LOWER}.parquet"
    if [[ ! -f "${PARQUET}" ]]; then
        echo "Skipping ${STATE_LOWER}: no attom parquet found at ${PARQUET}"
        continue
    fi
    echo "Processing state: ${STATE_LOWER}"
    "${PYTHON}" "${BUILD_SCRIPT}" \
        --state "${STATE_LOWER}" \
        --data "${DATA_ROOT}" \
        --tmp "${TMP_DIR}" \
        --threads "${THREADS}" \
        --memory "${MEMORY}" \
        --max-temp "${MAX_TEMP}"
done

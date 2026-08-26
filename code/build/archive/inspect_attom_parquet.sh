#!/bin/bash -l

#SBATCH --job-name=inspect_attom
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --time=1:00:00
#SBATCH --mem=64GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_inspect_attom.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_inspect_attom.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
INSPECT_SCRIPT="${PROJECT_ROOT}/inspect_attom_parquet.py"

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err"

for STATE_DIR in "${DATA_ROOT}"/*/; do
    STATE_LOWER="$(basename "${STATE_DIR}")"
    PARQUET="${STATE_DIR}attom_${STATE_LOWER}.parquet"
    if [[ ! -f "${PARQUET}" ]]; then
        echo "Skipping ${STATE_LOWER}: no parquet found at ${PARQUET}"
        continue
    fi
    echo "Inspecting: ${PARQUET}"
    "${PYTHON}" "${INSPECT_SCRIPT}" --parquet "${PARQUET}"
done

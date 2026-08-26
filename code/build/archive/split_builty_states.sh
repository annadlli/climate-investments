#!/bin/bash -l

#SBATCH --job-name=split_builty
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --time=1:00:00
#SBATCH --mem=32GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_split_builty.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_split_builty.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
SPLIT_SCRIPT="${SPLIT_SCRIPT:-${PROJECT_ROOT}/build_split_builty_states.py}"
TMP_DIR="${TMP_DIR:-${PROJECT_ROOT}/tmp}"

# One elevation tier: the filtered national file out of filter_builty_strict.do.
INPUT="${INPUT:-${DATA_ROOT}/build/all_builty_elevations_strict.dta}"
OUT_DIR="${OUT_DIR:-${DATA_ROOT}/builty-states}"
STATES="${STATES:-AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA}"

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${TMP_DIR}" "${OUT_DIR}"

echo "Input:   ${INPUT}"
echo "Output:  ${OUT_DIR}"
echo "States:  ${STATES}"

"${PYTHON}" "${SPLIT_SCRIPT}" \
    --data "${DATA_ROOT}" \
    --input "${INPUT}" \
    --out-dir "${OUT_DIR}" \
    --states ${STATES} \
    --filename-pattern "builty_elevations_{state_lower}.dta" \
    --tmp "${TMP_DIR}"

echo "Done. Per-state elevation files in ${OUT_DIR}/"

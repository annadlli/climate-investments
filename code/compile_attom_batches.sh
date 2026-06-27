#!/bin/bash -l

#SBATCH --job-name=compile_attom
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=8:00:00
#SBATCH --mem=96GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j_compile_attom.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j_compile_attom.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"
MANIFEST="${MANIFEST:-${PROJECT_ROOT}/dewey_manifest_wagner_template.csv}"
RUN_ID="${1:-${RUN_ID:-}}"
TMP_DIR="${TMP_DIR:-${PROJECT_ROOT}/tmp}"
THREADS="${SLURM_CPUS_PER_TASK:-4}"
MEMORY="${DUCKDB_MEMORY:-80GB}"

if [[ -z "${RUN_ID}" ]]; then
  echo "Usage: sbatch compile_attom_batches.sh run_20260625_115712" >&2
  echo "   or: RUN_ID=run_20260625_115712 sbatch compile_attom_batches.sh" >&2
  exit 2
fi

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${TMP_DIR}"

"${PYTHON}" "${PROJECT_ROOT}/compile_attom_batches.py" \
  --data "${DATA_ROOT}" \
  --manifest "${MANIFEST}" \
  --run-id "${RUN_ID}" \
  --tmp "${TMP_DIR}" \
  --threads "${THREADS}" \
  --memory "${MEMORY}"

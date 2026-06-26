#!/bin/bash -l

#SBATCH --job-name=importdewey
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=24:00:00
#SBATCH --mem=28GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j.err

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"

if [[ -f "${PROJECT_ROOT}/api_keys.env" ]]; then
  set -a
  source "${PROJECT_ROOT}/api_keys.env"
  set +a
fi

EXTRA_ARGS=("$@")
if [[ " ${EXTRA_ARGS[*]} " != *" --manifest "* && -f "${PROJECT_ROOT}/dewey_manifest_wagner_template.csv" ]]; then
  EXTRA_ARGS=(--manifest "${PROJECT_ROOT}/dewey_manifest_wagner_template.csv" "${EXTRA_ARGS[@]}")
fi

"${PYTHON}" "${PROJECT_ROOT}/import_dewey.py" --data "${DATA_ROOT}" "${EXTRA_ARGS[@]}"

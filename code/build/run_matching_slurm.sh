#!/bin/bash -l
#
# Authors: Anna Li
# Date: 2026-08-25
#
# Cluster wrapper around run_matching.sh. It supplies the TORCH paths and the
# memory the big states need, then hands off; the five steps and their flags
# live in run_matching.sh so there is only one place to change them.
#
#   sbatch code/build/run_matching_slurm.sh
#
# The array index picks the state, so all 20 run from one submission (two at a
# time, to stay inside the ATTOM I/O budget). These directives are read by
# sbatch before anything executes, so they must stay literal and above the
# first command -- without --array, SLURM_ARRAY_TASK_ID is unset, line 17
# falls back to index 0, and only Alabama runs.

#SBATCH --job-name=nfip_attom_matching
#SBATCH --array=0-19%2
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=72:00:00
#SBATCH --mem=96GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%A_%a_matching.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%A_%a_matching.err

set -euo pipefail

ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA="${DATA_ROOT:-${ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"

STATES=(al ct de fl ga la ma md me ms nc nh nj ny pa ri sc tx va vt)
STATE="${STATES[${SLURM_ARRAY_TASK_ID:-0}]}"

mkdir -p "${ROOT}/logs_out" "${ROOT}/logs_err"

# Keep DuckDB's cap below the SLURM allocation: it spills to --tmp past this
# point, whereas overshooting the allocation gets the job killed outright.
exec bash "$(dirname "${BASH_SOURCE[0]}")/run_matching.sh" \
    --state "${STATE}" \
    --data "${DATA}" \
    --python "${PYTHON}" \
    --out-root "${DATA}/build/nfip_attom_pipeline_v2" \
    --memory "${DUCKDB_MEMORY:-80GB}" \
    --threads "${SLURM_CPUS_PER_TASK:-4}" \
    --tmp "${ROOT}/tmp/matching/${STATE}/${SLURM_JOB_ID:-manual}"

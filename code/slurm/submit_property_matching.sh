#!/bin/bash -l
#
# Authors: Anna Li
# Date: 2026-08-25
#
# Cluster wrapper around run_property_matching.sh. It supplies the TORCH paths and the
# memory the big states need, then hands off; the five steps and their flags
# live in the matching driver so there is only one place to change them.
#
#   sbatch code/slurm/submit_property_matching.sh
#09-06 change: rerun select states only -> pick array indices and pass FROM_STEP so it doesn't rerun everything including the geocode and NFHL join
#Indices: al=0 ct=1 de=2 fl=3 ga=4 la=5 ma=6 md=7 me=8 ms=9 nc=10 nh=11 nj=12  ny=13 pa=14 ri=15 sc=16 tx=17 va=18 vt=19

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

# SLURM copies this wrapper into its spool directory before running it, so the
# driver is looked up from the submission directory, not from this file (09-07)
CODE_DIR="${CODE_DIR:-${SLURM_SUBMIT_DIR:-${ROOT}}}"

# Keep DuckDB's cap below the SLURM allocation: it spills to --tmp past this
# point, whereas overshooting the allocation gets the job killed outright.
exec bash "${CODE_DIR}/run_property_matching.sh" \
    --state "${STATE}" \
    --data "${DATA}" \
    --python "${PYTHON}" \
    --out-root "${DATA}/build/nfip_attom_pipeline_v2" \
    --memory "${DUCKDB_MEMORY:-80GB}" \
    --threads "${SLURM_CPUS_PER_TASK:-4}" \
    ${FROM_STEP:+--from "${FROM_STEP}"} \
    ${NFHL_ROOT:+--nfhl-root "${NFHL_ROOT}"} \
    --tmp "${ROOT}/tmp/matching/${STATE}/${SLURM_JOB_ID:-manual}"

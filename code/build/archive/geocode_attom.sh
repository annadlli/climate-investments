#!/bin/bash -l

#SBATCH --job-name=geocode_attom
#SBATCH --array=0-11%3
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=48:00:00
#SBATCH --mem=64GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%A_%a_geocode_attom.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%A_%a_geocode_attom.err

# Torch/SLURM wrapper for the two-step ATTOM geocode chain, one array task per
# state: geocode_attom.py (extract addresses + Census batch geocode) then
# build_attom_geocoded.py (fan block groups to properties + join onto ATTOM).
# The geocode step is network-bound and resume-safe: if a task hits the time
# limit, resubmit and completed batches are skipped. Keep WORKERS modest - the
# Census geocoder is a shared public service; run states in parallel, not more
# workers per state.
#
# Submit:        sbatch geocode_attom.sh                       (all states in STATES)
# One state:     sbatch --array=0 geocode_attom.sh             (first state only)
# Cluster pilot: sbatch --export=ALL,SAMPLE=100 geocode_attom.sh

set -euo pipefail

# Unbuffered stdout so progress prints reach the .out log as they happen.
export PYTHONUNBUFFERED=1

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/adl9602/tx}"
DATA_ROOT="${DATA_ROOT:-${PROJECT_ROOT}/data}"
PYTHON="${PYTHON:-/scratch/adl9602/venvs/py311/bin/python}"

# States to geocode; keep --array in sync with this list length.
STATES=(al ct de fl ga me ms nc nj ny pa sc)

SAMPLE="${SAMPLE:-0}"
# One upload at a time per state; with a few states active this avoids the
# Census-side 502 wave seen at four workers each.
WORKERS="${WORKERS:-1}"
# Keep chunk size fixed per state once batches accumulate - changing it
# repartitions the chunks and invalidates that state's cached results.
CHUNK_SIZE="${CHUNK_SIZE:-10000}"
DUCKDB_MEMORY="${DUCKDB_MEMORY:-24GB}"
STATE="${STATES[${SLURM_ARRAY_TASK_ID:-0}]}"

mkdir -p "${PROJECT_ROOT}/logs_out" "${PROJECT_ROOT}/logs_err" "${DATA_ROOT}/build"

if [[ ! -f "${DATA_ROOT}/${STATE}/attom_${STATE}.parquet" \
   && ! -f "${DATA_ROOT}/raw/attom/attom_${STATE}.parquet" ]]; then
    echo "Skipping ${STATE}: no attom parquet"
    exit 0
fi

echo "Geocoding state: ${STATE} (sample=${SAMPLE}, workers=${WORKERS})"

# Step 1: extract addresses + geocode. set -e stops the chain if this exits
# nonzero (e.g. batches still missing), so step 2 never runs on partial data.
"${PYTHON}" "${PROJECT_ROOT}/geocode_attom.py" \
    --state "${STATE}" --data "${DATA_ROOT}" --sample "${SAMPLE}" \
    --workers "${WORKERS}" --chunk-size "${CHUNK_SIZE}" --memory "${DUCKDB_MEMORY}"

# Step 2: fan block groups to properties + join onto ATTOM.
"${PYTHON}" "${PROJECT_ROOT}/build_attom_geocoded.py" \
    --state "${STATE}" --data "${DATA_ROOT}" --sample "${SAMPLE}" \
    --memory "${DUCKDB_MEMORY}"

echo "Done ${STATE}: ${DATA_ROOT}/build/${STATE}_attom_geocoded.parquet"

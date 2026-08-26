#!/bin/bash
#
# Authors: Anna Li
# Date: 2026-08-25
#
# Runs the five matching steps that link Builty elevation permits and ATTOM
# property records onto the NFIP property universe, for one state.

set -euo pipefail
export PYTHONUNBUFFERED=1

# -----------------------------------------------------------------------------
# Defaults. Every one can be overridden, which is what lets the cluster wrapper
# reuse this file rather than keeping a second copy of the same five commands.
# -----------------------------------------------------------------------------
DATA="/Users/anna/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
PYTHON="/opt/anaconda3/bin/python"
STATE=""
OUT_ROOT=""
MEMORY="24GB"
THREADS="4"
TMP=""
FORCE=0

usage() {
    cat >&2 <<'USAGE'
Usage: run_matching.sh --state ST [options]

  --state ST        Two-letter state code (required; case-insensitive)
  --data DIR        Data root (default: the Dropbox Empirical/Data folder)
  --out-root DIR    Matching output root (default: {data}/build/nfip_attom_pipeline_v2)
  --python PATH     Python interpreter (default: /opt/anaconda3/bin/python)
  --memory SIZE     DuckDB memory cap, e.g. 80GB (default: 24GB)
  --threads N       DuckDB threads (default: 4)
  --tmp DIR         Spill directory (default: {out-root}/tmp/{st})
  --force           Rerun every step even where the output already exists
USAGE
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --state)    STATE="$2";    shift 2 ;;
        --data)     DATA="$2";     shift 2 ;;
        --out-root) OUT_ROOT="$2"; shift 2 ;;
        --python)   PYTHON="$2";   shift 2 ;;
        --memory)   MEMORY="$2";   shift 2 ;;
        --threads)  THREADS="$2";  shift 2 ;;
        --tmp)      TMP="$2";      shift 2 ;;
        --force)    FORCE=1;       shift 1 ;;
        -h|--help)  usage ;;
        *) echo "run_matching: unknown argument '$1'" >&2; usage ;;
    esac
done

[[ -n "${STATE}" ]] || { echo "run_matching: --state is required" >&2; usage; }

# Lower-case for file names, upper-case for the arguments the Python scripts
# expect. Doing it here means the caller can pass either.
ST="$(echo "${STATE}" | tr '[:upper:]' '[:lower:]')"
STU="$(echo "${STATE}" | tr '[:lower:]' '[:upper:]')"

CODE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_ROOT="${OUT_ROOT:-${DATA}/build/nfip_attom_pipeline_v2}"
TMP="${TMP:-${OUT_ROOT}/tmp/${ST}}"

# -----------------------------------------------------------------------------
# Paths. Inputs first, then the output of each step, so the dependency chain
# below reads in the same order it runs.
# -----------------------------------------------------------------------------
RAW_ATTOM="${DATA}/raw/attom/attom_${ST}.parquet"
# build_attom_nfhl.py globs <root>/<state-fips>/*.gdb, so the root is the folder
# holding the numbered state directories -- one level below raw/nfhl.
NFHL_ROOT="${DATA}/raw/nfhl/nfhl"
GEOCODE_WORK="${DATA}/build/attom_geocode/${ST}_addr"
PERMITS="${DATA}/build/builty_elevations_zipfilled.dta"
PROPERTIES="${DATA}/clean/nfip_policies_property.dta"
STATE_POLICIES="${DATA}/clean/nfip_policies_state/${ST}.dta"

GEOCODED="${OUT_ROOT}/geocoded/${ST}_attom_geocoded.parquet"
BLOCKGROUPS="${OUT_ROOT}/geocoded/${ST}_attom_blockgroups"
NFHL="${OUT_ROOT}/nfhl_matches/${ST}_attom_nfhl.parquet"
BUILTY="${OUT_ROOT}/builty_attom/${ST}_attom_permits.parquet"
ENRICHED="${OUT_ROOT}/attom_nfhl_builty/${ST}_attom_nfhl_builty.parquet"
FINAL="${OUT_ROOT}/nfip_attom_property/${ST}_nfip_attom_property.parquet"

DIAG="${OUT_ROOT}/diagnostics"
BUILTY_DIAG="${DIAG}/${ST}_builty_attom.csv"
ENRICH_DIAG="${DIAG}/${ST}_attom_nfhl_builty.csv"
TIER_DIAG="${DIAG}/${ST}_tier_diagnostics.csv"
CELL_DIAG="${DIAG}/${ST}_cell_diagnostics.csv"

mkdir -p "${OUT_ROOT}/geocoded" "${OUT_ROOT}/nfhl_matches" \
         "${OUT_ROOT}/builty_attom" "${OUT_ROOT}/attom_nfhl_builty" \
         "${OUT_ROOT}/nfip_attom_property" "${DIAG}" "${TMP}"

# -----------------------------------------------------------------------------
# Helpers.
#
# Steps are skipped when their output already exists. That is what makes a rerun
# cheap, but it matters for a second reason: the raw Census geocoder results
# that step 1 consumes live only on the cluster, while step 1's *output* is
# synced to Dropbox. Locally, step 1 is therefore skipped and steps 2-5 run off
# the synced panel. Pass --force only where the inputs are actually present.
# -----------------------------------------------------------------------------
step_needed() {
    local label="$1" target="$2"
    if [[ "${FORCE}" -eq 0 && -s "${target}" ]]; then
        echo "[${label}] skip -- output exists: ${target}"
        return 1
    fi
    return 0
}

require() {
    # Fail before doing any work, naming the file that is missing. A step that
    # starts without its input wastes hours and then writes a partial result.
    local missing=0
    for path in "$@"; do
        [[ -e "${path}" ]] || { echo "run_matching: missing input: ${path}" >&2; missing=1; }
    done
    [[ "${missing}" -eq 0 ]] || exit 2
}

echo "=== ${STU}: matching pipeline ==="
echo "    data     ${DATA}"
echo "    output   ${OUT_ROOT}"

# -----------------------------------------------------------------------------
# 1. ATTOM geocoded panel.
#
# Fans the cached Census block-group results back onto ATTOM properties.
# --sample 0 uses the full state rather than the pilot subset. The crosswalk is
# written as parquet only; there is no Stata copy, and nothing downstream wants one.
# -----------------------------------------------------------------------------
if step_needed "1/5" "${GEOCODED}"; then
    require "${RAW_ATTOM}" \
            "${GEOCODE_WORK}/attomid_xwalk.parquet" \
            "${GEOCODE_WORK}/blockgroups_by_address.parquet"
    echo "[1/5] ATTOM geocoded panel"
    "${PYTHON}" "${CODE}/build_attom_geocoded.py" \
        --state "${STU}" --data "${DATA}" --sample 0 \
        --out "${GEOCODED}" --blockgroups-out "${BLOCKGROUPS}" \
        --tmp "${TMP}/geocoded" --memory "${MEMORY}"
fi

# -----------------------------------------------------------------------------
# 2. ATTOM x NFHL spatial join.
#
# Attaches the flood zone and NFIP community each geocoded property sits in.
# All state map vintages are 2018 except Vermont, which is 2016.
# -----------------------------------------------------------------------------
if step_needed "2/5" "${NFHL}"; then
    require "${GEOCODED}" "${NFHL_ROOT}"
    echo "[2/5] ATTOM--NFHL spatial join"
    "${PYTHON}" "${CODE}/build_attom_nfhl.py" \
        --state "${STU}" --nfhl "${NFHL_ROOT}" \
        --points "${GEOCODED}" --out "${NFHL}"
fi

# -----------------------------------------------------------------------------
# 3. Builty permits -> ATTOM, by address.
#
# Matched against the FULL RAW ATTOM file, not the geocoded panel. This is the
# correction that motivated the rebuild: matching against the geocoded panel
# made every permit whose property failed to geocode unmatchable before the
# match began, which silently shrank the treated sample.
# -----------------------------------------------------------------------------
if step_needed "3/5" "${BUILTY}"; then
    require "${RAW_ATTOM}" "${PERMITS}"
    echo "[3/5] Builty--ATTOM address match against raw ATTOM"
    "${PYTHON}" "${CODE}/build_attom_onto_permits.py" \
        --state "${STU}" --data "${DATA}" --permits "${PERMITS}" \
        --attom "${RAW_ATTOM}" --out "${BUILTY}" --diagnostics "${BUILTY_DIAG}" \
        --tmp "${TMP}/builty" --memory "${MEMORY}" --threads "${THREADS}"
fi

# -----------------------------------------------------------------------------
# 4. Builty flags onto the ATTOM--NFHL universe.
#
# Left join on the ATTOM master, so every property survives and only the
# elevation columns are filled in where a permit matched.
# -----------------------------------------------------------------------------
if step_needed "4/5" "${ENRICHED}"; then
    require "${NFHL}" "${BUILTY}"
    echo "[4/5] Left merge Builty onto the ATTOM--NFHL universe"
    "${PYTHON}" "${CODE}/build_attom_nfhl_builty.py" \
        --attom-nfhl "${NFHL}" --builty-attom "${BUILTY}" \
        --out "${ENRICHED}" --diagnostics "${ENRICH_DIAG}"
fi

# -----------------------------------------------------------------------------
# 5. Assign ATTOM properties to NFIP properties.
#
# The identification step: one ATTOM property per NFIP property, down a ladder
# of successively looser cells. --use-codes restricts the candidate pool to
# single-family homes. --add-tier-15 appends a final block-group x flood-zone
# tier for properties with no recorded construction year, which every earlier
# tier requires and would otherwise drop.
# -----------------------------------------------------------------------------
if step_needed "5/5" "${FINAL}"; then
    require "${PROPERTIES}" "${STATE_POLICIES}" "${GEOCODED}" "${ENRICHED}"
    echo "[5/5] Assign SFR ATTOM candidates to NFIP properties"
    "${PYTHON}" "${CODE}/assign_attom_to_nfip_property.py" \
        --state "${STU}" --properties "${PROPERTIES}" \
        --state-policies "${STATE_POLICIES}" --attom "${GEOCODED}" \
        --attom-nfhl-builty "${ENRICHED}" \
        --use-codes 376,380,382,383,385,386 \
        --add-tier-15 \
        --out "${FINAL}" --tier-diagnostics "${TIER_DIAG}" \
        --cell-diagnostics "${CELL_DIAG}" --tmp "${TMP}/assignment" \
        --memory "${MEMORY}" --threads "${THREADS}"
fi

echo "=== ${STU}: done -> ${FINAL} ==="

#!/bin/bash
#
# Authors: Anna Li
# Date: 2026-08-25
#
# Runs the five matching steps that link Builty elevation permits and ATTOM
# property records onto the NFIP property universe, for one state.

set -euo pipefail
export PYTHONUNBUFFERED=1

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
Usage: run_property_matching.sh --state ST [options]

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
        *) echo "run_property_matching: unknown argument '$1'" >&2; usage ;;
    esac
done

[[ -n "${STATE}" ]] || { echo "run_property_matching: --state is required" >&2; usage; }

# Lower-case for file names, upper-case for the arguments the Python scripts
# expect. Doing it here means the caller can pass either.
ST="$(echo "${STATE}" | tr '[:upper:]' '[:lower:]')"
STU="$(echo "${STATE}" | tr '[:lower:]' '[:upper:]')"

CODE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../build" && pwd)"
OUT_ROOT="${OUT_ROOT:-${DATA}/build/nfip_attom_pipeline_v2}"
TMP="${TMP:-${OUT_ROOT}/tmp/${ST}}"

# Paths. Inputs first, then the output of each step, so the dependency chain below reads in the same order it runs.
RAW_ATTOM="${DATA}/raw/attom/attom_${ST}.parquet"
# attom_nfhl.py globs <root>/<state-fips>/*.gdb, so the root is the folder
# holding the numbered state directories -- one level below raw/nfhl.
NFHL_ROOT="${DATA}/raw/nfhl/nfhl"
GEOCODE_WORK="${DATA}/build/attom_geocode/${ST}_addr"
PERMITS="${DATA}/build/builty_elevations_zipfilled.dta"
PROPERTIES="${DATA}/clean/nfip_policies_property.dta"
STATE_POLICIES="${DATA}/clean/nfip_policies_state/${ST}.dta"

GEOCODED="${OUT_ROOT}/geocoded/${ST}_attom_geocoded.parquet"
BLOCKGROUPS="${OUT_ROOT}/geocoded/${ST}_attom_blockgroups"
NFHL="${OUT_ROOT}/nfhl_matches/${ST}_attom_nfhl.parquet"
PERMITS_OUT="${OUT_ROOT}/attom_builty/${ST}_attom_permits.parquet"
BUILTY="${OUT_ROOT}/attom_builty/${ST}_attom_builty.parquet"
FINAL="${OUT_ROOT}/nfip_attom_property/${ST}_nfip_attom_property.parquet"


# Steps are skipped when their output already exists. That is what makes a rerun

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
        [[ -e "${path}" ]] || { echo "run_property_matching: missing input: ${path}" >&2; missing=1; }
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
# -----------------------------------------------------------------------------
if step_needed "1/4" "${GEOCODED}"; then
    require "${RAW_ATTOM}" \
            "${GEOCODE_WORK}/attomid_xwalk.parquet" \
            "${GEOCODE_WORK}/blockgroups_by_address.parquet"
    echo "[1/4] ATTOM geocoded panel"
    "${PYTHON}" "${CODE}/attom_geocode.py" \
        --state "${STU}" --data "${DATA}" --sample 0 \
        --out "${GEOCODED}" --blockgroups-out "${BLOCKGROUPS}" \
        --tmp "${TMP}/geocoded" --memory "${MEMORY}"
fi

# -----------------------------------------------------------------------------
# 2. ATTOM x NFHL spatial join.
#
# Attaches the flood zone and NFIP community each geocoded property sits in.
# All state map vintages are 2018 except Vermont, which is 2016. Comes from Wagner.
# -----------------------------------------------------------------------------
if step_needed "2/4" "${NFHL}"; then
    require "${GEOCODED}" "${NFHL_ROOT}"
    echo "[2/4] ATTOM--NFHL spatial join"
    "${PYTHON}" "${CODE}/attom_nfhl.py" \
        --state "${STU}" --nfhl "${NFHL_ROOT}" \
        --points "${GEOCODED}" --out "${NFHL}"
fi

# -----------------------------------------------------------------------------
# 3. Builty permits -> ATTOM.
#
# Permits are address-matched against the FULL RAW ATTOM file, not the geocoded
# panel: matching against the panel made every permit whose property failed to
# geocode unmatchable, which silently shrank the treated sample. The matched
# permits are then collapsed to property level and left-joined onto the
# ATTOM--NFHL master, so every property survives.
# -----------------------------------------------------------------------------
if step_needed "3/4" "${BUILTY}"; then
    require "${RAW_ATTOM}" "${PERMITS}" "${NFHL}"
    echo "[3/4] Builty--ATTOM address match, joined onto the ATTOM--NFHL universe"
    "${PYTHON}" "${CODE}/attom_builty.py" \
        --state "${STU}" --data "${DATA}" --permits "${PERMITS}" \
        --attom "${RAW_ATTOM}" --attom-nfhl "${NFHL}" \
        --out "${BUILTY}" --permits-out "${PERMITS_OUT}" \
        --tmp "${TMP}/builty" --memory "${MEMORY}" --threads "${THREADS}"
fi

# -----------------------------------------------------------------------------
# 4. Assign ATTOM properties to NFIP properties.
#
# The identification step: one ATTOM property per NFIP property, down a ladder
# of successively looser cells. --use-codes restricts the candidate pool to
# single-family homes. --add-tier-15 appends a final block-group x flood-zone
# tier for properties with no recorded construction year, which every earlier
# tier requires and would otherwise drop.
# -----------------------------------------------------------------------------
if step_needed "4/4" "${FINAL}"; then
    require "${PROPERTIES}" "${STATE_POLICIES}" "${GEOCODED}" "${BUILTY}"
    echo "[4/4] Assign SFR ATTOM candidates to NFIP properties"
    "${PYTHON}" "${CODE}/nfip_attom.py" \
        --state "${STU}" --properties "${PROPERTIES}" \
        --state-policies "${STATE_POLICIES}" --attom "${GEOCODED}" \
        --attom-nfhl-builty "${BUILTY}" \
        --use-codes 376,380,382,383,385,386 \
        --add-tier-15 \
        --out "${FINAL}" --tmp "${TMP}/assignment" \
        --memory "${MEMORY}" --threads "${THREADS}"
fi

echo "=== ${STU}: done -> ${FINAL} ==="

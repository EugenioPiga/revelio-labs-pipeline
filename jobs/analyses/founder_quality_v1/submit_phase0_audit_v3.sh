#!/usr/bin/env bash
set -eo pipefail
ROOT=/home/epiga/revelio_labs
SCRIPT="$ROOT/jobs/founder_quality_v1/founder_quality_phase0_audit_v3.sbatch"
mkdir -p "$ROOT/logs/founder_quality_v1/phase0_v3" "$ROOT/output/founder_quality_v1/audit_v3"
[ -f "$SCRIPT" ] || { echo "[ERROR] Missing $SCRIPT" >&2; exit 1; }
JOB=$(sbatch --parsable "$SCRIPT")
echo "Submitted V3-only Phase-0 audit job: $JOB"
echo "Log: $ROOT/logs/founder_quality_v1/phase0_v3/fqV3Audit_${JOB}.out"
echo "Error log: $ROOT/logs/founder_quality_v1/phase0_v3/fqV3Audit_${JOB}.err"
echo "Results: $ROOT/output/founder_quality_v1/audit_v3/run_*"

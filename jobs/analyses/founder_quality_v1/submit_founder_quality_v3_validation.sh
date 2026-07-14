#!/bin/bash
set -eo pipefail

ROOT=/home/epiga/revelio_labs
SCRIPT="$ROOT/jobs/founder_quality_v1/founder_quality_v3_validation.sbatch"
LOGDIR="$ROOT/logs/founder_quality_v1/validation_v3"
OUTROOT="$ROOT/output/founder_quality_v1/validation_v3"

mkdir -p "$LOGDIR" "$OUTROOT"

JOB=$(sbatch "$SCRIPT" | awk '{print $4}')

echo "Submitted V3 founder-output validation job: $JOB"
echo "Log: $LOGDIR/fqV3Validate_${JOB}.out"
echo "Error log: $LOGDIR/fqV3Validate_${JOB}.err"
echo "Results: $OUTROOT/run_*"

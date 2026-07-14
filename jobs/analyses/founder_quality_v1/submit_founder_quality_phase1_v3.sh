#!/bin/bash
set -eo pipefail

PROJECT=/home/epiga/revelio_labs
mkdir -p "$PROJECT/logs/founder_quality_v1/phase1_v3"
mkdir -p "$PROJECT/output/founder_quality_v1/phase1_v3"

VALIDATION_RUN=$(ls -dt "$PROJECT"/output/founder_quality_v1/validation_v3/run_* 2>/dev/null | head -1)
if [ -z "$VALIDATION_RUN" ] || [ ! -f "$VALIDATION_RUN/VALIDATION_REPORT.md" ]; then
  echo "ERROR: no completed validation_v3 run with VALIDATION_REPORT.md found." >&2
  exit 2
fi

JOB=$(sbatch --parsable "$PROJECT/jobs/founder_quality_v1/founder_quality_phase1_v3.sbatch")
echo "Submitted V3 founder-quality Phase-1 analysis job: $JOB"
echo "Log: $PROJECT/logs/founder_quality_v1/phase1_v3/fqV3Phase1_${JOB}.out"
echo "Error log: $PROJECT/logs/founder_quality_v1/phase1_v3/fqV3Phase1_${JOB}.err"
echo "Results: $PROJECT/output/founder_quality_v1/phase1_v3/run_*"

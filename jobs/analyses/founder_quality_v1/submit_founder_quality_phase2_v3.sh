#!/bin/bash
set -eo pipefail
cd /home/epiga/revelio_labs
mkdir -p logs/founder_quality_v1/phase2_v3 output/founder_quality_v1/phase2_v3
JOB=$(sbatch --parsable jobs/founder_quality_v1/founder_quality_phase2_v3.sbatch)
echo "Submitted V3 founder-quality Phase-2 job: ${JOB}"
echo "Log: /home/epiga/revelio_labs/logs/founder_quality_v1/phase2_v3/fqV3Phase2_${JOB}.out"
echo "Error log: /home/epiga/revelio_labs/logs/founder_quality_v1/phase2_v3/fqV3Phase2_${JOB}.err"
echo "Results: /home/epiga/revelio_labs/output/founder_quality_v1/phase2_v3/run_*"

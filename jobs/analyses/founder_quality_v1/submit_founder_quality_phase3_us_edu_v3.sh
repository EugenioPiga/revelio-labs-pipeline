#!/bin/bash
set -eo pipefail
cd /home/epiga/revelio_labs
mkdir -p /home/epiga/revelio_labs/logs/founder_quality_v1/phase3_us_edu_v3
JOB=$(sbatch jobs/founder_quality_v1/founder_quality_phase3_us_edu_v3.sbatch | awk '{print $4}')
echo "Submitted U.S.-year / U.S.-educated founder-quality job: ${JOB}"
echo "Log: /home/epiga/revelio_labs/logs/founder_quality_v1/phase3_us_edu_v3/fqV3USEdu_${JOB}.out"
echo "Error log: /home/epiga/revelio_labs/logs/founder_quality_v1/phase3_us_edu_v3/fqV3USEdu_${JOB}.err"
echo "Results: /home/epiga/revelio_labs/output/founder_quality_v1/phase3_us_edu_v3/run_*"

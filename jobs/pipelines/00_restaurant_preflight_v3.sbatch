#!/bin/bash
#SBATCH --job-name=restaurant_preflight
#SBATCH --partition=general
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --output=/home/epiga/innovation_linkedin/logs/restaurant_preflight_%j.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/restaurant_preflight_%j.err
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT
#SBATCH --mail-user=epiga@ucsd.edu

set -eo pipefail
source "$HOME/miniconda/bin/activate" revelio
ROOT="$HOME/innovation_linkedin"
SCRATCH="/labs/khanna/linkedin_202507/scratch/restaurant_preflight_${SLURM_JOB_ID}"
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
mkdir -p "$TMPDIR" "$SCRATCH/warehouse"
trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT
spark-submit --master "local[${SLURM_CPUS_PER_TASK}]" --driver-memory 24g \
  "$ROOT/src/pipelines/extract_us_restaurant_histories_v3.py" --mode preflight --threads "$SLURM_CPUS_PER_TASK" --tmpdir "$SCRATCH"

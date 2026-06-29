#!/usr/bin/env bash
# Shared runtime environment for all V3 Spark jobs.
# Expects JOB_LABEL, SPARK_DRIVER_MEMORY, and SPARK_EXECUTOR_MEMORY to be set.

set -euo pipefail

mkdir -p /home/epiga/revelio_labs/logs

set +u
source "$HOME/miniconda/bin/activate" revelio
set -u

echo "[INFO] Using python: $(which python)"
python -V

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
    echo "ERROR: Java 17 was not found at $JAVA_HOME/bin/java" >&2
    exit 1
fi

echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

SCRATCH_BASE="${SCRATCH_BASE:-/labs/khanna/linkedin_202507/scratch/v3_spell_integrity}"

TASK_SUFFIX=""
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    TASK_SUFFIX="_${SLURM_ARRAY_TASK_ID}"
fi

export SCRATCH="${SCRATCH_BASE}/${JOB_LABEL}_${SLURM_JOB_ID}${TASK_SUFFIX}"

mkdir -p "$SCRATCH"/{tmp,local,warehouse}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"
export SPARK_SQL_WAREHOUSE_DIR="$SCRATCH/warehouse"

export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-200g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-200g}"

export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# Ensures the requested memory is passed when PySpark starts its JVM.
export PYSPARK_SUBMIT_ARGS="--driver-memory ${SPARK_DRIVER_MEMORY} \
--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
--conf spark.local.dir=${SPARK_LOCAL_DIRS} \
--conf spark.sql.warehouse.dir=${SPARK_SQL_WAREHOUSE_DIR} \
pyspark-shell"

echo "[SCRATCH] $SCRATCH"
echo "[SPARK] driver=${SPARK_DRIVER_MEMORY}; executor=${SPARK_EXECUTOR_MEMORY}"

trap 'rm -rf "$SCRATCH" || true' EXIT

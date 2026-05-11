#!/usr/bin/env bash
#SBATCH --job-name=step6_colleague
#SBATCH --output=logs/step6_colleague_%j.out
#SBATCH --error=logs/step6_colleague_%j.err
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=500G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

mkdir -p logs

echo "============================================================"
echo "[INFO] Step 6: colleague citation and collaboration measures"
echo "[INFO] Job ID: ${SLURM_JOB_ID}"
echo "[INFO] Node: ${SLURMD_NODENAME:-unknown}"
echo "[INFO] Started at: $(date)"
echo "============================================================"

# ============================================================
# 1) Environment
# ============================================================
source "$HOME/miniconda/bin/activate" revelio
echo "[INFO] Using Python from: $(which python)"
python --version

# ============================================================
# 2) Java 17
# ============================================================
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# ============================================================
# 3) Scratch on /labs
# ============================================================
SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_step6_colleague_links"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local,warehouse,checkpoints}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM / Spark tuning
export SPARK_DRIVER_MEMORY=420g
export SPARK_EXECUTOR_MEMORY=420g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# ============================================================
# 4) Paths
# ============================================================
CODE_DIR="$HOME/revelio_labs"

INPUT_PANEL="/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2_onet_naics"
PATENT_MATCHES="/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
CITATIONS_TSV="/labs/khanna/USPTO_202507/g_us_patent_citation.tsv"

# Final output name requested: _v2
OUT_DIR="/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"

SCRIPT="$CODE_DIR/step6_colleague_citation_collaboration_measures.py"

echo "[PATH] CODE_DIR       = $CODE_DIR"
echo "[PATH] SCRIPT         = $SCRIPT"
echo "[PATH] INPUT_PANEL    = $INPUT_PANEL"
echo "[PATH] PATENT_MATCHES = $PATENT_MATCHES"
echo "[PATH] CITATIONS_TSV  = $CITATIONS_TSV"
echo "[PATH] OUT_DIR        = $OUT_DIR"
echo "[PATH] SCRATCH        = $SCRATCH"

# ============================================================
# 5) Pre-checks
# ============================================================
if [ ! -d "$CODE_DIR" ]; then
  echo "[ERROR] CODE_DIR does not exist: $CODE_DIR"
  exit 1
fi

if [ ! -f "$SCRIPT" ]; then
  echo "[ERROR] Step 6 script not found: $SCRIPT"
  exit 1
fi

if [ ! -d "$INPUT_PANEL" ]; then
  echo "[ERROR] INPUT_PANEL does not exist: $INPUT_PANEL"
  exit 1
fi

if [ ! -d "$PATENT_MATCHES" ]; then
  echo "[ERROR] PATENT_MATCHES does not exist: $PATENT_MATCHES"
  exit 1
fi

if [ ! -f "$CITATIONS_TSV" ]; then
  echo "[ERROR] CITATIONS_TSV does not exist: $CITATIONS_TSV"
  exit 1
fi

cd "$CODE_DIR"

# ============================================================
# 6) Run Step 6
# ============================================================
python -u "$SCRIPT" \
  --input-panel "$INPUT_PANEL" \
  --patent-matches "$PATENT_MATCHES" \
  --citations-tsv "$CITATIONS_TSV" \
  --out-dir "$OUT_DIR" \
  --tmpdir "$SCRATCH/local" \
  --checkpoint-dir "$SCRATCH/checkpoints" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 4000 \
  --out-partitions 300

# ============================================================
# 7) Post-check
# ============================================================
echo "[INFO] Step 6 finished. Checking final output..."

if [ ! -d "$OUT_DIR" ]; then
  echo "[ERROR] Final output not found: $OUT_DIR"
  exit 1
fi

du -sh "$OUT_DIR"

python - <<'PY'
import pyarrow.dataset as ds

path = "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
d = ds.dataset(path, format="parquet")
cols = set(d.schema.names)

wanted = [
    # citation link counts
    "n_cite_links_total",
    "n_cite_links_same_parent",
    "n_cite_links_same_parent_more_senior",
    "n_cite_links_same_parent_less_equal_senior",
    "n_cite_links_same_parent_metro",
    "n_cite_links_same_parent_metro_more_senior",
    "n_cite_links_same_parent_metro_less_equal_senior",

    # citation patent counts
    "n_cited_patents_total",
    "n_cited_patents_same_parent",
    "n_cited_patents_same_parent_more_senior",
    "n_cited_patents_same_parent_less_equal_senior",
    "n_cited_patents_same_parent_metro",
    "n_cited_patents_same_parent_metro_more_senior",
    "n_cited_patents_same_parent_metro_less_equal_senior",

    # collaboration link counts
    "n_collab_links_total",
    "n_collab_links_same_parent",
    "n_collab_links_same_parent_more_senior",
    "n_collab_links_same_parent_less_equal_senior",
    "n_collab_links_same_parent_metro",
    "n_collab_links_same_parent_metro_more_senior",
    "n_collab_links_same_parent_metro_less_equal_senior",

    # collaboration patent counts
    "n_patents_with_any_coinventor",
    "n_patents_with_same_parent_collab",
    "n_patents_with_same_parent_more_senior_collab",
    "n_patents_with_same_parent_less_equal_senior_collab",
    "n_patents_with_same_parent_metro_collab",
    "n_patents_with_same_parent_metro_more_senior_collab",
    "n_patents_with_same_parent_metro_less_equal_senior_collab",

    # key pre-existing O*NET / NAICS variables should still exist
    "current_onet_code",
    "current_naics3",
    "current_role_k150_v2",
]

print("Number of columns:", len(d.schema.names))
for c in wanted:
    print(f"{c:60s}", "YES" if c in cols else "NO")
PY

# ============================================================
# 8) Cleanup
# ============================================================
rm -rf "$SCRATCH" || true

echo "============================================================"
echo "[INFO] Finished Step 6 at: $(date)"
echo "[INFO] Final output:"
echo "       $OUT_DIR"
echo "============================================================"

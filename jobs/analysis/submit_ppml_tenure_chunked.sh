#!/bin/bash
set -euo pipefail

ARRAY_SBATCH="/home/epiga/revelio_labs/ppml_tenure_ladder_chunk_array.sbatch"
MERGE_SBATCH="/home/epiga/revelio_labs/ppml_tenure_ladder_merge.sbatch"

jid=$(sbatch --parsable "$ARRAY_SBATCH")
echo "Submitted array job: $jid"
merge_jid=$(sbatch --parsable --dependency=afterok:$jid "$MERGE_SBATCH")
echo "Submitted merge job: $merge_jid (afterok:$jid)"

#!/bin/bash
#SBATCH --job-name=crosswalk_employer_assignee
#SBATCH --output=/home/epiga/revelio_labs/logs/crosswalk_employer_assignee_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/crosswalk_employer_assignee_%j.err
#SBATCH --time=06:00:00
#SBATCH --mem=250G
#SBATCH --cpus-per-task=56
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

# ===========================================
# Environment setup
# ===========================================
echo "[INFO] Job started on $(hostname) at $(date)"
source ~/.bashrc
source ~/.venvs/revelio/bin/activate

mkdir -p /home/epiga/revelio_labs/output/crosswalk_shards
mkdir -p /home/epiga/revelio_labs/logs

# ===========================================
# Run Python script
# ===========================================
PY_SCRIPT=/home/epiga/revelio_labs/check_similarity_assignee_employer.py
python $PY_SCRIPT


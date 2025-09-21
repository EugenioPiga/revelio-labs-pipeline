#!/bin/bash
#SBATCH --job-name=uspto_bulk_dl
#SBATCH --output=logs/uspto_bulk_dl_%j.out
#SBATCH --error=logs/uspto_bulk_dl_%j.err
#SBATCH --time=08:00:00
#SBATCH --partition=general
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G

# Email notifications
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL

# Create log folder
mkdir -p logs

# Activate Revelio virtual environment
source ~/.venvs/revelio/bin/activate

# Run download script
python download_uspto.py

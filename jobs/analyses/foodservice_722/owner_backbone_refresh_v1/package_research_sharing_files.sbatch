#!/bin/bash
#SBATCH --job-name=fs722_package_share
#SBATCH --partition=general
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_owner_backbone_refresh_v1/logs/fs722_package_share_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_owner_backbone_refresh_v1/logs/fs722_package_share_%j.err

set -eo pipefail

/home/epiga/revelio_labs/foodservice_722_owner_backbone_refresh_v1/package_research_sharing_files.sh

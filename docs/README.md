# Innovation & LinkedIn Pipeline

This repository builds and analyzes inventor–firm datasets by merging patents, education, and LinkedIn data.  
It is organized into **data**, **jobs**, **src**, and supporting folders.

---

## Repository Structure

### `artifacts/`
- Placeholder for generated artifacts (figures, reports, exports not tracked elsewhere).

---

### `data/`
Raw, processed, and example datasets.

- **`examples/`**
  Small CSV examples for testing and validation.  
  - `rcid_location_examples.csv`: sample RCID–location mapping.

- **`raw/`**  
  Temporary raw or scratch data during downloads (`scratch/`).

- **`processed/`**  
  Cleaned and pipeline-ready datasets.  
  - `intermediate/`  
    Contains pipeline intermediate datasets:  
    - `inventor_merged_with_user`: merged inventor–user mappings.  
    - `inventors_matched_education_spark/`: inventor × education matches (sharded).  
    - `inventors_matched_positions_spark/`: inventor × position matches (sharded).  
    - `inventors_matched_users/`: inventor–user matches (parquet parts).  
    - `merged_parts/`, `users_intersect_inventors/`, `patent_inventor_size/`, `tabs_stats/`: intermediate stats and utilities.
  - `parquet/`  
    Final inventor- and firm-level datasets in Parquet format.  
    - `inventor_level_file/`: inventor-level aggregates (patents, education, first/last patent, etc).  
    - `assignees_per_patent_rcid.parquet`, `company_patent_stats.parquet`, etc.: other analysis-ready tables.

---

### `docs/`
Documentation and reports.
- `README.md`: project documentation (this file).  
- `report/`: draft reports, papers, or notes.

---

### `jobs/`
Slurm batch scripts for HPC execution.

- **`analysis/`**: one-off analysis jobs (e.g., patent assignee analysis, size distributions).  
- **`pipelines/`**: main ETL pipeline jobs.  
  - `step1_inventors_matched_users.sbatch`: build inventor–user base.  
  - `step2_inventors_matched_positions*.sbatch`: link inventors to positions.  
  - `step3_inventors_matched_education*.sbatch`: link inventors to education.  
  - `enrich_inventor_education.sbatch`: enrich inventor records with education.  
  - `inventor_level_file.sbatch`: aggregate into inventor-level dataset.  
  - `submit_split_pipeline.sbatch`, `submit_steps_2_3.sbatch`: pipeline orchestrators.  
- **`sync/`**: data sync and Revelio download scripts.

---

### `notebooks/`
Exploratory Jupyter notebooks for quick analysis, prototyping, and visualization.

---

### `output/`
Final outputs for dissemination (lightweight).
- `figures/`: plots (e.g., concentration over time, cumulative patents).  
- `tables/`: LaTeX/CSV tables, reports.

---

### `src/`
Python source code (organized by purpose).

- **`analysis/`**: scripts for statistical analysis and reporting.  
  - `analyze_patents.py`, `patent_assignee_analysis.py`, etc.  
- **`pipeline/`**: main pipeline steps (mirrors `jobs/pipelines/`).  
  - `step1_inventors_matched_users.py`  
  - STILL IN PROGRESS: `step2_inventors_matched_positions.py`, `step2_inventors_matched_positions_spark.py`  
  - `step3_inventors_matched_education.py`, `step3_inventors_matched_education_spark.py`  
  - `enrich_inventor_education.py`  
  - `inventor_level_file.py`  
  - `master_merge_pipeline.py`   
- **`utils/`**: shared utility scripts.  
  - `check_missing.py`, `check_rcid_location.py`, `paths.py`, etc.  

---

## Pipeline Overview

1. **Step 1 – Inventor ↔ User Matching**  
   (`step1_inventors_matched_users.py`)  
   Produces base inventor–user dataset.  

2. **STILL IN PROGRESS: Step 2 – Inventor ↔ Position Matching**  
   (`step2_inventors_matched_positions.py`)  
   Links inventors to LinkedIn positions, generating `inventors_matched_positions_spark`.  

3. **Step 3 – Inventor ↔ Education Matching**  
   (`step3_inventors_matched_education*.py`)  
   Enriches inventor records with education history.  

4. **Enrichment**  
   (`enrich_inventor_education.py`)  
   Combines education and position data.  

5. **Inventor-level Aggregation**  
   (`inventor_level_file.py`)  
   Builds final inventor-level dataset with:  
   - number of patents  
   - first/last patent date  
   - education info (degrees, first field, first university)  

---

## Notes
- Large datasets under `data/processed/` are **ignored in GitHub** via `.gitignore`.  
- To run the pipeline on HPC, submit jobs from `jobs/pipelines/`.  
- For local testing, use small samples in `data/examples/`.  

---

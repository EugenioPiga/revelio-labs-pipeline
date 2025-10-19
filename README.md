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
  Temporary raw or scratch data used during downloads (`scratch/`).

- **`processed/`**  
  Cleaned and analysis-ready datasets.  
  - **`intermediate/`**  
    Contains pipeline intermediates such as:  
    - `inventors_matched_education_spark/`: inventor × education matches (sharded).  
    - `inventors_matched_positions_spark/`: inventor × position matches (sharded).  
    - `inventors_matched_users/`: inventor–user mappings.  
    - `merged_parts/`, `users_intersect_inventors/`, `patent_inventor_size/`, `tabs_stats/`: aggregated statistics.  
  - **`parquet/`**  
    Final inventor- and firm-level datasets in Parquet format.  
    - `inventor_level_file/`: inventor-level aggregates (patents, education, timing).  
    - `assignees_per_patent_rcid.parquet`, `company_patent_stats.parquet`, etc.: analysis-ready tables.

---

### `docs/`
Documentation and reports.  
- `README.md`: project documentation (this file).  
- `report/`: drafts, figures, or paper notes.

---

### `jobs/`
Slurm batch scripts for HPC execution.

- **`analysis/`**  
  Regression and decomposition jobs (PPML, OLS, event studies).  
  - `ppml_patents_baseline.sbatch`, `ppml_patents_covariates.sbatch`, etc.  
  - `combine_event_study_shards_*.sbatch`: combine and summarize regression outputs.  

- **`pipelines/`**  
  Core ETL pipeline jobs (**now complete**).  
  - `step1_inventors_matched_users.sbatch`: build inventor–user base.  
  - `step2_inventors_matched_positions*.sbatch`: link inventors to positions.  
  - `step3_inventors_matched_education*.sbatch`: link inventors to education.  
  - `enrich_inventor_education.sbatch`: merge education and positions.  
  - `inventor_level_file.sbatch`: aggregate into inventor-level dataset.  
  - `submit_split_pipeline.sbatch`, `submit_steps_2_3.sbatch`: orchestrators.

- **`sync/`**  
  Data synchronization and Revelio download scripts.

---

### `notebooks/`
Exploratory Jupyter notebooks for diagnostics, visualization, and sanity checks.

---

### `output/`
Final outputs for dissemination (lightweight).  
- **`figures/`**: plots (e.g., concentration over time, cumulative patents, immigrant shares).  
- **`tables/`**: text-based summaries or LaTeX tables.

---

### `src/`
Python and R source code (organized by purpose).

- **`analysis/`**  
  Statistical analysis and regression scripts.  
  - `ppml_patents_baseline.R`, `ppml_patents_covariates.R`, `combine_event_study_shards_*.R`: PPML and OLS analyses.  
  - `analyze_patents.py`, `patent_assignee_analysis.py`, `city_decomposition.R`, etc.  

- **`pipeline/`**  
  Core data construction pipeline (**fully implemented**).  
  - `step1_inventors_matched_users.py`  
  - `step2_inventors_matched_positions.py`, `step2_inventors_matched_positions_spark.py`  
  - `step3_inventors_matched_education.py`, `step3_inventors_matched_education_spark.py`  
  - `enrich_inventor_education.py`  
  - `inventor_level_file.py`  
  - `master_merge_pipeline.py`  

- **`utils/`**  
  Shared diagnostics, validation, and helper scripts.  
  - `check_missing.py`, `check_rcid_location.py`, `paths.py`, `check_patent_position_shard.py`, etc.

---

## Pipeline Overview

1. **Inventor ↔ User Matching**  
   (`step1_inventors_matched_users.py`)  
   Produces base inventor–user mappings.

2. **Inventor ↔ Position Matching**  
   (`step2_inventors_matched_positions*.py`)  
   Links inventors to LinkedIn position histories.

3. **Inventor ↔ Education Matching**  
   (`step3_inventors_matched_education*.py`)  
   Adds education records (degrees, fields, universities).

4. **Education Enrichment**  
   (`enrich_inventor_education.py`)  
   Merges education and position information.

5. **Inventor-level Aggregation**  
   (`inventor_level_file.py`)  
   Builds inventor-level tables with:  
   - number of patents  
   - first/last patent date  
   - educational background  
   - firm–inventor match statistics

6. **Statistical & PPML Regressions**  
   (`ppml_patents_baseline.R`, `ppml_patents_covariates.R`)  
   Estimate firm- and city-level regressions to quantify the effect of inventor mobility and education on innovation outcomes.

---

## Notes
- Large intermediate data (under `data/processed/`) are **excluded from GitHub** via `.gitignore`.  
- Run the pipeline on HPC using jobs in `jobs/pipelines/`.  
- For local testing, use small samples in `data/examples/`.  
- The main data pipeline is **complete** — ongoing work focuses on **PPML estimation, decomposition, and event studies**.

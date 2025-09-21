"""
Centralized project paths configuration.

Change BASE_PATH to your own folder once,
and all scripts will work everywhere.
"""

from pathlib import Path

# Change this to your base directory
BASE_PATH = Path("/home/epiga/innovation_linkedin")

# Data folders
DATA_RAW = BASE_PATH / "data" / "raw"
DATA_PROCESSED = BASE_PATH / "data" / "processed"
DATA_EXAMPLES = BASE_PATH / "data" / "examples"

# Output folders
OUTPUT = BASE_PATH / "output"
FIGURES = OUTPUT / "figures"
TABLES = OUTPUT / "tables"

# Docs
DOCS = BASE_PATH / "docs"
REPORT = DOCS / "report"

# Source folders
SRC = BASE_PATH / "src"
SRC_ANALYSIS = SRC / "analysis"
SRC_PIPELINE = SRC / "pipeline"
SRC_UTILS = SRC / "utils"

# Jobs
JOBS = BASE_PATH / "jobs"
JOBS_PIPELINES = JOBS / "pipelines"
JOBS_ANALYSIS = JOBS / "analysis"
JOBS_SYNC = JOBS / "sync"


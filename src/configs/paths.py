import os

# Racine projet depuis notebook Databricks
PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))

DATA_ROOT = f"{PROJECT_ROOT}/data"

# src/config/paths.py

SILVER_MAIN = "/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean"
SILVER_DELTA = "/Volumes/workspace/ipsldata/capstoneipsl/data/silver/main_clean_delta"

REPORTS_BENCHMARKS = "/Workspace/Repos/TON_REPO/src/reports/benchmarks"


REPORTS_BENCHMARKS = os.path.join(
    PROJECT_ROOT, "src", "reports", "benchmarks"
)

# ============================================================
# India Data Platform
# File    : dags/pipeline_runner.py
# Purpose : Orchestrates the full daily pipeline in order.
#           Step 1 — ingest from API       (Bronze)
#           Step 2 — clean and validate    (Silver)
#           Step 3 — generate insights     (Gold)
#           Stops immediately if any step fails.
#           This becomes an Airflow DAG in production.
# Author  : Kevin Josh
# ============================================================

import logging
import sys
import time
from datetime import datetime, date

# Import our pipeline scripts as modules
sys.path.append(".")
from lambdas.api_ingest       import run_ingestion
from spark_jobs.bronze_to_silver import run_transformation
from spark_jobs.silver_to_gold   import run_gold_layer

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# PIPELINE STEP RUNNER
# ============================================================

def run_step(step_number: int, step_name: str, func, *args) -> bool:
    """
    Runs one pipeline step safely.
    Returns True if successful, False if failed.

    This pattern is used in every real orchestration tool —
    Airflow, Step Functions, Prefect all work this way:
    run step → check success → continue or stop
    """
    logger.info("-" * 55)
    logger.info(f"STEP {step_number} STARTED  : {step_name}")
    logger.info("-" * 55)

    start_time = time.time()

    try:
        func(*args)
        elapsed = round(time.time() - start_time, 2)
        logger.info(f"STEP {step_number} COMPLETED: {step_name} ({elapsed}s)")
        return True

    except Exception as e:
        elapsed = round(time.time() - start_time, 2)
        logger.error(f"STEP {step_number} FAILED   : {step_name} ({elapsed}s)")
        logger.error(f"Error: {e}")
        logger.error("Pipeline stopped — fix this step before retrying")
        return False


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline(process_date: str = None) -> None:
    """
    Runs the full daily pipeline end to end.
    Stops at first failure — no partial gold data.
    """
    if process_date is None:
        process_date = str(date.today())

    pipeline_start = time.time()

    logger.info("=" * 55)
    logger.info("INDIA DATA PLATFORM — DAILY PIPELINE START")
    logger.info(f"Process date    : {process_date}")
    logger.info(f"Started at      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    # ── Step 1: Bronze — Ingest from API ──────────────────
    success = run_step(
        1,
        "API Ingest → Bronze Layer",
        run_ingestion
    )
    if not success:
        logger.error("PIPELINE FAILED at Step 1")
        sys.exit(1)

    # ── Step 2: Silver — Clean and Validate ───────────────
    success = run_step(
        2,
        "Bronze → Silver Transformation",
        run_transformation,
        process_date
    )
    if not success:
        logger.error("PIPELINE FAILED at Step 2")
        sys.exit(1)

    # ── Step 3: Gold — Generate Business Insights ─────────
    success = run_step(
        3,
        "Silver → Gold Aggregation",
        run_gold_layer,
        process_date
    )
    if not success:
        logger.error("PIPELINE FAILED at Step 3")
        sys.exit(1)

    # ── Pipeline Complete ──────────────────────────────────
    total_time = round(time.time() - pipeline_start, 2)

    logger.info("=" * 55)
    logger.info("DAILY PIPELINE COMPLETE")
    logger.info(f"  Process date  : {process_date}")
    logger.info(f"  Total time    : {total_time} seconds")
    logger.info(f"  Steps run     : 3 of 3")
    logger.info(f"  Status        : SUCCESS")
    logger.info("=" * 55)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_pipeline()
"""Orchestrates all Phase 1 setup steps for the Employee ETL pipeline."""
import logging
import sys
import time

from src.config import Config
from src import s3_setup, iam_setup, glue_setup, dynamodb_setup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _run_step(step_name: str, fn, *args, **kwargs):
    """Execute a pipeline step, logging the name and re-raising on failure."""
    logger.info(">>> Starting step: %s", step_name)
    try:
        result = fn(*args, **kwargs)
        logger.info("<<< Completed step: %s", step_name)
        return result
    except Exception as exc:
        logger.error("!!! Step '%s' failed: %s", step_name, exc)
        raise


def run() -> None:
    """Execute all Phase 1 pipeline steps in order."""

    # ── Step 1: Load Config ──────────────────────────────────────────────────
    config = _run_step("Load Config", Config)

    # ── Step 2: Create S3 Bucket ─────────────────────────────────────────────
    _run_step("Create S3 Bucket", s3_setup.create_bucket, config)

    # ── Step 3: Configure Bucket Security ────────────────────────────────────
    _run_step("Configure Bucket Security", s3_setup.configure_bucket_security, config)

    # ── Step 4: Upload CSV to S3 ──────────────────────────────────────────────
    s3_key = config.S3_RAW_PREFIX + "employee_data.csv"
    s3_uri = _run_step(
        "Upload CSV to S3",
        s3_setup.upload_file,
        config,
        config.LOCAL_DATA_FILE,
        s3_key,
    )

    # ── Step 5: Verify Upload ─────────────────────────────────────────────────
    verified = _run_step("Verify Upload", s3_setup.verify_upload, config, s3_key)
    assert verified, (
        f"Upload verification failed for s3://{config.S3_BUCKET_NAME}/{s3_key}"
    )

    # ── Step 6: Create Glue IAM Role ──────────────────────────────────────────
    role_arn = _run_step("Create Glue IAM Role", iam_setup.create_glue_role, config)

    # ── Step 7: Attach Managed Policies ───────────────────────────────────────
    _run_step(
        "Attach Glue Managed Policies",
        iam_setup.attach_glue_policies,
        config,
        config.GLUE_IAM_ROLE_NAME,
    )

    # ── Step 8: Put S3 Inline Policy ──────────────────────────────────────────
    _run_step(
        "Put S3 Inline Policy",
        iam_setup.put_s3_inline_policy,
        config,
        config.GLUE_IAM_ROLE_NAME,
    )

    # ── Step 9: Create Glue Database ──────────────────────────────────────────
    _run_step("Create Glue Database", glue_setup.create_database, config)

    # IAM roles take ~10s to propagate before Glue can assume them
    logger.info("Waiting 15s for IAM role propagation...")
    time.sleep(15)

    # ── Step 10: Create Glue Crawler ──────────────────────────────────────────
    resolved_role_arn = _run_step("Get Role ARN", iam_setup.get_role_arn, config)
    _run_step(
        "Create Glue Crawler",
        glue_setup.create_crawler,
        config,
        resolved_role_arn,
    )

    # ── Step 11: Run Crawler (blocking) ───────────────────────────────────────
    _run_step("Run Glue Crawler", glue_setup.run_crawler, config)

    # ── Step 12: Get Catalog Table ────────────────────────────────────────────
    catalog_table = _run_step(
        "Get Catalog Table",
        glue_setup.get_catalog_table,
        config,
        "employees",
    )
    col_count = len(
        catalog_table.get("StorageDescriptor", {}).get("Columns", [])
    )

    # ── Step 13: Create DynamoDB Table ────────────────────────────────────────
    _run_step("Create DynamoDB Table", dynamodb_setup.create_table, config)

    # ── Step 14: Wait for DynamoDB Table Active ───────────────────────────────
    _run_step(
        "Wait for DynamoDB Table Active",
        dynamodb_setup.wait_for_table_active,
        config,
    )

    # ── Step 15: Summary Report ───────────────────────────────────────────────
    account_id = config.aws_account_id
    print(f"""
=== Phase 1 Setup Complete ===

S3 Bucket     : s3://employee-data-glue-{account_id}
File Uploaded : s3://employee-data-glue-{account_id}/raw/employees/employee_data.csv
IAM Role ARN  : arn:aws:iam::{account_id}:role/AWSGlueRole-EmployeeETL
Glue Database : employee_db
Catalog Table : employee_db.employees ({col_count} columns catalogued)
DynamoDB Table: Employees (PAY_PER_REQUEST, ACTIVE)

Ready for Phase 2: PySpark ETL job implementation.
""")


if __name__ == "__main__":
    run()

"""
HR ETL PySpark Job — Phase 2
Joins employees, departments, and managers CSVs, computes business metrics,
and sinks results to DynamoDB (single-table) and S3 Parquet (for Athena).
"""
import sys
from decimal import Decimal, ROUND_HALF_UP

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Job initialisation ───────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "raw_bucket", "parquet_bucket", "dynamo_table"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BASE = f"s3://{args['raw_bucket']}"
PARQUET_BASE = f"s3://{args['parquet_bucket']}"
DYNAMO_TABLE = args["dynamo_table"]

# ── Step 1: Read source CSVs ─────────────────────────────────────────────────
employees = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_BASE}/employee_data_updated.csv")
)

departments = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_BASE}/departments_data.csv")
)

managers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_BASE}/managers_data.csv")
)

# ── Step 2: Normalise join keys to string (avoids int/string type mismatch) ──
employees = (
    employees
    .withColumn("DeptID", F.col("DeptID").cast("string"))
    .withColumn("ManagerID", F.col("ManagerID").cast("string"))
    .withColumn("Salary", F.col("Salary").cast("double"))
)

departments = (
    departments
    .withColumn("DeptID", F.col("DeptID").cast("string"))
    .withColumn("MaxSalaryRange", F.col("MaxSalaryRange").cast("double"))
    .withColumn("MinSalaryRange", F.col("MinSalaryRange").cast("double"))
)

managers = managers.withColumn("ManagerID", F.col("ManagerID").cast("string"))

# ── Step 3: Joins ─────────────────────────────────────────────────────────────
# employees LEFT JOIN departments on DeptID → brings in MaxSalaryRange etc.
enriched = employees.join(
    departments.select(
        "DeptID", "DepartmentName", "MaxSalaryRange", "MinSalaryRange", "Budget"
    ),
    on="DeptID",
    how="left",
)

# result LEFT JOIN managers on ManagerID → brings in IsActive, Level
enriched = enriched.join(
    managers.select("ManagerID", "ManagerName", "IsActive", "Level"),
    on="ManagerID",
    how="left",
)

# ── Step 4: Window function — HighestTitleSalary ──────────────────────────────
title_window = Window.partitionBy("JobTitle")
enriched = enriched.withColumn(
    "HighestTitleSalary",
    F.max(F.col("Salary")).over(title_window),
)

# ── Step 5: Business logic ────────────────────────────────────────────────────
enriched = enriched.withColumn(
    "CompaRatio",
    F.round(F.col("Salary") / F.col("MaxSalaryRange"), 2),
)

# IsActive is stored as the string "True" / "False" in the CSV — not a boolean
enriched = enriched.withColumn(
    "RequiresReview",
    (F.col("CompaRatio") > F.lit(1.0)) | (F.col("IsActive") == F.lit("False")),
)

# ── Step 6: DynamoDB sink via foreachPartition ────────────────────────────────
def _to_decimal(val) -> Decimal:
    """Convert a numeric value to Decimal — boto3 rejects raw Python floats."""
    if val is None:
        return Decimal("0")
    return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def write_partition_to_dynamo(rows, table_name: str) -> None:
    """Write a Spark partition to DynamoDB using batch_writer (max 25 items/call)."""
    import boto3  # imported inside closure — boto3 not available on driver path in all envs
    from decimal import Decimal, ROUND_HALF_UP

    def to_dec(val):
        if val is None:
            return Decimal("0")
        return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows:
            d = row.asDict()
            emp_id = str(d.get("EmployeeID", ""))
            item = {
                "PK": f"EMP#{emp_id}",
                "SK": "PROFILE",
                "EmployeeID": emp_id,
                "FirstName": str(d.get("FirstName") or ""),
                "LastName": str(d.get("LastName") or ""),
                "Email": str(d.get("Email") or ""),
                "Department": str(d.get("Department") or ""),
                "DepartmentName": str(d.get("DepartmentName") or ""),
                "JobTitle": str(d.get("JobTitle") or ""),
                "HireDate": str(d.get("HireDate") or ""),
                "City": str(d.get("City") or ""),
                "State": str(d.get("State") or ""),
                "EmploymentStatus": str(d.get("EmploymentStatus") or ""),
                "Manager": str(d.get("Manager") or ""),
                "ManagerName": str(d.get("ManagerName") or ""),
                "ManagerLevel": str(d.get("Level") or ""),
                "Salary": to_dec(d.get("Salary")),
                "CompaRatio": to_dec(d.get("CompaRatio")),
                "HighestTitleSalary": to_dec(d.get("HighestTitleSalary")),
                "RequiresReview": bool(d.get("RequiresReview", False)),
            }
            batch.put_item(Item=item)


# Only pass the table name (primitive) into the closure — not boto3 objects
dynamo_table_name = DYNAMO_TABLE
enriched.foreachPartition(
    lambda rows: write_partition_to_dynamo(rows, dynamo_table_name)
)

# ── Step 7: Parquet sink for Athena ──────────────────────────────────────────
(
    enriched.write
    .mode("overwrite")
    .parquet(f"{PARQUET_BASE}/employees/")
)

# REQUIRED: without this Glue marks the job run as failed
job.commit()

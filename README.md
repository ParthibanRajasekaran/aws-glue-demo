# aws-glue-demo — Serverless HR ETL Pipeline

A production-style serverless ETL pipeline on AWS, built in two phases:

- **Phase 1** — Infrastructure bootstrapped with boto3 (eu-west-2, manual)
- **Phase 2** — Full CDK stack with PySpark ETL, DynamoDB Single-Table, and Lambda API (us-east-1)

---

## Architecture

```
data/ (3 CSVs)
    │
    ▼  deploy.sh
S3 Raw Bucket ──► Glue ETL Job (PySpark) ──► S3 Parquet Bucket ──► Athena
                        │
                        ▼
               DynamoDB Single-Table
               (PK: EMP#<id>, SK: PROFILE)
                        │
                        ▼
                  Lambda API Handler
```

| Resource | Name / Type |
|---|---|
| S3 (raw input) | CDK-generated, holds 3 CSV files |
| S3 (Parquet output) | CDK-generated, queryable via Athena |
| Glue Job | PySpark ETL, Glue 4.0, 2×G.1X workers |
| Glue Catalog | `hr_analytics` database, `hr-parquet-crawler` |
| DynamoDB | `aws-glue-demo-single-table` (PAY_PER_REQUEST) |
| Lambda | Python 3.12 — employee profile API |

---

## Single-Table Design

All employee records live in one DynamoDB table with a composite key:

| Attribute | Value | Description |
|---|---|---|
| `PK` | `EMP#<EmployeeID>` | Partition key |
| `SK` | `PROFILE` | Sort key |

**Item attributes written by ETL:**

| Field | Type | Description |
|---|---|---|
| `Name` | String | Full name |
| `Department` | String | From departments join |
| `JobTitle` | String | Raw from CSV |
| `Manager` | String | From managers join |
| `Salary` | Decimal | Raw salary |
| `CompaRatio` | Decimal | `Salary / MaxSalaryRange` |
| `HighestTitleSalary` | Decimal | Max salary for that job title across all employees |
| `RequiresReview` | Boolean | `CompaRatio > 1.0` OR `Manager.IsActive == False` |

---

## Source Data

| File | Rows | Key Fields |
|---|---|---|
| `employee_data_updated.csv` | 1,000 | EmployeeID, DeptID, ManagerID, Salary |
| `departments_data.csv` | 6 | DeptID, MaxSalaryRange, MinSalaryRange |
| `managers_data.csv` | 100 | ManagerID, IsActive |

---

## Prerequisites

- AWS CLI configured (`aws configure`)
- Node.js >= 18
- Python >= 3.10
- Java 11+ (for local Spark tests)
- `jq` (`brew install jq`)

---

## Deploy

```bash
bash deploy.sh
```

This will:
1. Install CDK CLI (if missing)
2. Install Python CDK dependencies
3. Bootstrap CDK in `us-east-1`
4. Deploy the CloudFormation stack
5. Upload the 3 CSVs to the raw S3 bucket
6. Start the Glue ETL job

Monitor the Glue job (~3-5 min):
```bash
aws glue get-job-run \
  --job-name $(jq -r '.HrPipelineStack.GlueJobName' outputs.json) \
  --run-id <run-id-from-deploy-output> \
  --region us-east-1 \
  --query 'JobRun.JobRunState'
```

---

## Verify

```bash
bash verify_api.sh           # employee 1001 (default)
bash verify_api.sh 1042      # any employee ID
```

Expected response:
```json
{
  "EmployeeID": "1001",
  "Name": "Patricia Martinez",
  "Department": "Sales",
  "JobTitle": "Sales AE",
  "Manager": "Jennifer Jones",
  "Salary": 121131.0,
  "CompaRatio": 0.79,
  "HighestTitleSalary": 139838.0,
  "RequiresReview": false
}
```

---

## Athena Queries

After running the Glue crawler (`hr-parquet-crawler`) in the AWS Console:

```sql
-- All employees
SELECT * FROM hr_analytics.employees LIMIT 10;

-- Employees flagged for review
SELECT employeeid, firstname, lastname, jobtitle, comparatio, requiresreview
FROM hr_analytics.employees
WHERE requiresreview = true
ORDER BY comparatio DESC;

-- Average salary by department
SELECT departmentname, ROUND(AVG(salary), 0) AS avg_salary, COUNT(*) AS headcount
FROM hr_analytics.employees
GROUP BY departmentname
ORDER BY avg_salary DESC;
```

---

## Running Tests

### Unit tests (no AWS required)
```bash
# Lambda handler tests
pytest tests/unit/test_handler.py -v

# ETL logic tests (requires pyspark)
pip install pyspark
pytest tests/unit/test_etl_logic.py -v
```

### Phase 1 unit tests (moto-based, no AWS required)
```bash
pytest tests/ -m unit -v
```

### E2E tests (requires deployed stack + completed Glue job)
```bash
pytest tests/e2e/ -v -m e2e
```

---

## Teardown

```bash
cdk destroy HrPipelineStack
```

All resources have `REMOVAL_POLICY.DESTROY` — the stack deletes cleanly with no orphans.

---

## Project Structure

```
aws-glue-demo/
├── data/
│   ├── employee_data_updated.csv
│   ├── departments_data.csv
│   └── managers_data.csv
├── infrastructure/
│   ├── app.py                    # CDK entry point
│   ├── infrastructure_stack.py   # Full CDK stack definition
│   └── requirements.txt          # CDK Python dependencies
├── src/
│   ├── glue/
│   │   └── etl_job.py            # PySpark ETL (join -> window -> DynamoDB + Parquet)
│   ├── lambda/
│   │   └── handler.py            # Employee profile API
│   └── ...                       # Phase 1 boto3 modules
├── tests/
│   ├── unit/
│   │   ├── test_handler.py       # Lambda unit tests (mocked boto3)
│   │   └── test_etl_logic.py     # Spark transformation tests (local SparkSession)
│   └── e2e/
│       └── test_pipeline_e2e.py  # Live AWS E2E tests (requires deployed stack)
├── deploy.sh                     # Full deployment automation
├── verify_api.sh                 # Lambda invocation test
└── cdk.json                      # CDK app manifest
```

---

## Security

- No credentials are hardcoded anywhere in the codebase
- `outputs.json` and `.env` are gitignored
- All IAM policies use least-privilege CDK grants (`grant_read`, `grant_write_data`)
- DynamoDB: Lambda has read-only access; Glue job has write-only access
- All resources use `REMOVAL_POLICY.DESTROY` for clean teardown

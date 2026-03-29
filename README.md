# aws-glue-demo — Serverless HR Analytics ETL Pipeline

A production-grade, serverless ETL pipeline on AWS demonstrating Zero-Trust configuration, FinOps cost controls, observability, and data governance — built with AWS CDK (Python), PySpark, and AWS Glue 4.0.

---

## Architecture

```mermaid
flowchart LR
    subgraph Ingestion
        CSV["📄 Raw CSVs\n(S3 / raw bucket)"]
    end

    subgraph Transform ["AWS Glue ETL (PySpark · G.1X × 2)"]
        direction TB
        CAT["Glue Data Catalog\nhr_analytics database"]
        ETL["etl_job.py\nSilver layer → Broadcast joins\nWindow fn → DQ Gate → Circuit Breaker"]
        CAT --> ETL
    end

    subgraph Serve
        DDB["DynamoDB\nSingle-Table\nPK: EMP#id · SK: PROFILE"]
        PQ["S3 Parquet\nHive-partitioned\nyear / month / dept"]
        API["Lambda\nPython 3.12\nEmployee Profile API"]
        ATH["Amazon Athena\nhr_analytics_wg\n100 MB scan cap"]
    end

    subgraph Observe
        CW["CloudWatch\nDashboard + Alarm"]
        SNS["SNS Topic\nhr-pipeline-alerts"]
    end

    CSV -->|"deploy.sh\naws s3 cp"| CAT
    ETL -->|"DynamicFrame\nconnector"| DDB
    ETL -->|"partitionBy\nyear/month/dept"| PQ
    DDB --> API
    PQ --> ATH
    ETL -->|"numFailedTasks > 0"| CW
    CW -->|"ALARM state"| SNS
```

---

## FinOps Strategy

Cost was a first-class design constraint. Every decision has a price tag.

| Decision | Rationale | Monthly savings vs. default |
|---|---|---|
| **G.1X workers (2×)** | Minimum viable worker size for batch PySpark ETL. G.2X doubles DPU cost with no throughput benefit at 1,000-row scale. | ~$0.11/run |
| **`MaxRetries = 0`** | Runaway retries on a misconfigured job would silently double or triple DPU spend. Fail fast; alert; fix. | Up to 2× DPU savings |
| **`Timeout = 5 min`** | Hard kill at 5 minutes prevents a hung Spark stage from accumulating DPU-hours. | Unbounded protection |
| **Athena 100 MB scan cap** | `BytesScannedCutoffPerQuery = 104857600` on `hr_analytics_wg`. Ad-hoc queries against the wrong (unpartitioned) table cannot scan the full dataset. | Depends on query; cap prevents surprises |
| **Hive partitioning (year/month/dept)** | Athena prunes partitions at query time. A query scoped to one department and one month reads ~1/72 of the dataset. | Up to 98% Athena cost reduction |
| **Static Glue Catalog (no Crawler)** | Crawlers run on a schedule, consume DPUs, and cost ~$0.44/hour. Schema authority lives in CDK `CfnTable` definitions — free at synth time. | ~$0.44/crawl eliminated |
| **CloudWatch Alarm (~$0.10/month)** | Early detection prevents a silent pipeline failure from persisting for days, which would require a costly backfill run. | Pays for itself on first incident |

---

## Security

### Encryption at rest and in transit

All data stores are encrypted with a **Customer-Managed KMS Key** (annual rotation enabled):

| Resource | Encryption |
|---|---|
| S3 raw bucket | SSE-KMS (pipeline key) |
| S3 Parquet bucket | SSE-KMS (pipeline key) |
| S3 assets bucket (Glue TempDir) | SSE-KMS (pipeline key) |
| DynamoDB single-table | AWS-managed KMS via CDK `TableEncryption.AWS_MANAGED` |

### Zero-Trust configuration discovery

No resource name, ARN, or secret appears in source code or in environment variables. All runtime configuration is fetched from **SSM Parameter Store** at startup:

| SSM Path | Consumer | Purpose |
|---|---|---|
| `/hr-pipeline/dynamodb-table-name` | Glue ETL, Lambda | DynamoDB table for sink and read API |
| `/hr-pipeline/parquet-bucket-name` | Glue ETL | S3 output path for Parquet sink |
| `/hr-pipeline/raw-bucket-name` | Ops runbooks | Raw CSV source bucket |
| `/hr-pipeline/kms-key-arn` | Audit tooling | KMS key for encryption verification |

See [docs/ADR/001-configuration-management.md](docs/ADR/001-configuration-management.md) for the full decision record.

### IAM least-privilege

No `Resource: "*"` exists in any custom IAM statement.

| Principal | Actions | Scoped to |
|---|---|---|
| Glue job role | `s3:GetObject`, `s3:ListBucket` | `raw_bucket/raw/*` prefix only |
| Glue job role | `s3:PutObject`, `s3:DeleteObject` | `parquet_bucket/employees/*` only |
| Glue job role | `dynamodb:PutItem`, `BatchWriteItem` | `aws-glue-demo-single-table` only |
| Glue job role | `kms:GenerateDataKey*`, `kms:Decrypt` | Specific pipeline KMS key ARN |
| Glue job role | `ssm:GetParameter`, `ssm:GetParameters` | `/hr-pipeline/*` prefix |
| Lambda role | `dynamodb:GetItem` | `aws-glue-demo-single-table` only |
| Lambda role | `ssm:GetParameter` | `/hr-pipeline/dynamodb-table-name` only |

---

## Data Quality & Governance

The pipeline enforces a two-tier quality strategy:

**Tier 1 — Rule-level gate (fail-fast):**
`EvaluateDataQuality` checks three rules before any write. A single failure aborts the job — no partial data reaches DynamoDB or Parquet.

```
IsComplete "EmployeeID"
ColumnValues "Salary" > 0
IsUnique "EmployeeID"
```

**Tier 2 — Row-level circuit breaker:**
Even when aggregate rules pass, individual rows with a `RowOutcome=Error` (null `EmployeeID` or non-positive `Salary`) are quarantined. They are logged to CloudWatch (`/aws-glue/jobs/output`) and excluded from the DynamoDB write. Clean records proceed normally.

---

## Observability

| Signal | Implementation |
|---|---|
| **CloudWatch Dashboard** `hr-pipeline-observability` | Glue succeeded/failed task counts + Athena bytes scanned per query |
| **CloudWatch Alarm** `hr-pipeline-glue-job-failed` | Fires within 1 minute when `numFailedTasks > 0` |
| **SNS Topic** `hr-pipeline-alerts` | Alarm action target — subscribe an email or PagerDuty endpoint to receive alerts |
| **Glue DQ Results** | Published to the Glue Data Quality console under context `hr_etl_dq` |
| **CloudWatch Logs** `/aws-glue/jobs/output` | Circuit breaker quarantine logs (1-day retention) |

---

## Single-Table Design

All employee records live in one DynamoDB table with a composite key:

| Attribute | Value |
|---|---|
| `PK` | `EMP#<EmployeeID>` |
| `SK` | `PROFILE` |

**Key computed fields:**

| Field | Formula |
|---|---|
| `CompaRatio` | `ROUND(Salary / MaxSalaryRange, 2)` |
| `HighestTitleSalary` | `MAX(Salary) OVER (PARTITION BY JobTitle)` |
| `RequiresReview` | `CompaRatio > 1.0 OR IsActive == "False"` |

---

## Source Data

| File | Rows | Key Fields |
|---|---|---|
| `employee_data_updated.csv` | 1,000 | EmployeeID, DeptID, ManagerID, Salary |
| `departments_data.csv` | 6 | DeptID, MaxSalaryRange, MinSalaryRange, Budget |
| `managers_data.csv` | 100 | ManagerID, IsActive, Level |

---

## Prerequisites

- AWS CLI configured (`aws configure`)
- Node.js >= 18
- Python >= 3.10
- `jq` (`brew install jq`)

---

## Deploy

```bash
bash deploy.sh
```

Steps performed automatically:
1. Install CDK CLI (if missing)
2. Install Python CDK dependencies
3. Bootstrap CDK in `us-east-1`
4. Deploy the CloudFormation stack (S3, Glue, DynamoDB, Lambda, KMS, SSM, CloudWatch, SNS)
5. Upload the 3 CSVs to the raw S3 bucket
6. Start the Glue ETL job

Monitor the Glue job (~3–5 min):
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

```sql
-- All employees
SELECT * FROM hr_analytics.employees LIMIT 10;

-- Employees flagged for review
SELECT employeeid, firstname, lastname, jobtitle, comparatio
FROM hr_analytics.employees
WHERE requiresreview = true
ORDER BY comparatio DESC;

-- Average salary by department
SELECT departmentname, ROUND(AVG(salary), 0) AS avg_salary, COUNT(*) AS headcount
FROM hr_analytics.employees
GROUP BY departmentname
ORDER BY avg_salary DESC;

-- Reconciliation: raw row count vs enriched row count
SELECT data_source, row_count
FROM hr_analytics.v_etl_reconciliation;
```

---

## Running Tests

```bash
# Lambda handler (no AWS required)
pytest tests/unit/test_handler.py -v

# E2E (requires deployed stack + completed Glue job)
pytest tests/e2e/ -v -m e2e
```

---

## Teardown

```bash
cdk destroy HrPipelineStack
```

All resources have `REMOVAL_POLICY.DESTROY`. The KMS key enters a 7-day scheduled deletion window after stack destroy.

---

## Project Structure

```
aws-glue-demo/
├── data/
│   ├── employee_data_updated.csv
│   ├── departments_data.csv
│   └── managers_data.csv
├── docs/
│   └── ADR/
│       └── 001-configuration-management.md
├── infrastructure/
│   ├── app.py
│   ├── infrastructure_stack.py
│   └── requirements.txt
├── src/
│   ├── glue/
│   │   └── etl_job.py
│   └── lambda/
│       └── handler.py
├── tests/
│   ├── unit/
│   │   └── test_handler.py
│   └── e2e/
│       └── test_pipeline_e2e.py
├── README.md
├── README_TECHNICAL.md        ← data contract & technical spec
├── deploy.sh
└── verify_api.sh
```

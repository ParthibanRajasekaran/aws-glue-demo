# Phase 1 Specification (Baseline)

This document defines the Phase 1 contract for the employee Glue setup pipeline.

## Required components
- `src/config.py` resolves AWS account ID via STS and centralizes resource names.
- `src/s3_setup.py` provisions one raw bucket, enables versioning, blocks public access, and uploads CSV input.
- `src/iam_setup.py` creates a Glue service role, attaches only required managed policies, and applies scoped S3 inline access.
- `src/glue_setup.py` creates Glue database and crawler, runs crawler synchronously with polling, and validates catalog table availability.
- `src/dynamodb_setup.py` provisions `Employees` table using `PAY_PER_REQUEST` and rejects `PROVISIONED` billing mode.
- `src/pipeline_runner.py` orchestrates modules in-order and prints a setup summary.

## Idempotency expectations
- Bucket, role, database, crawler, and table creation functions must be safe on repeated runs.
- Existing crawler configuration must be reconciled to desired state.
- Existing DynamoDB billing mode must be validated before run continuation.

## Security expectations
- S3 bucket must have versioning, public access block, and default SSE enabled.
- IAM permissions must follow least privilege (no broad full-access policies).

## Testing expectations
- Unit tests cover happy-path and error-path branches for S3, IAM, Glue, and DynamoDB setup modules.
- Integration tests validate live AWS behavior using explicit table-name constants.

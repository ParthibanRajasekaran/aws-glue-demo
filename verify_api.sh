#!/bin/bash
# verify_api.sh — Invoke the Lambda and print the employee profile response
# Usage: bash verify_api.sh [employee_id]
# Default employee_id: 1001
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

EMPLOYEE_ID="${1:-1001}"
RESPONSE_FILE="/tmp/lambda_response.json"

if [ ! -f outputs.json ]; then
    echo "ERROR: outputs.json not found. Run deploy.sh first."
    exit 1
fi

LAMBDA_FN=$(jq -r '.HrPipelineStack.LambdaFunctionName' outputs.json)

echo "Invoking Lambda: $LAMBDA_FN"
echo "Employee ID    : $EMPLOYEE_ID"
echo ""

aws lambda invoke \
    --function-name "$LAMBDA_FN" \
    --payload "{\"employee_id\": \"${EMPLOYEE_ID}\"}" \
    --cli-binary-format raw-in-base64-out \
    --region us-east-1 \
    "$RESPONSE_FILE" \
    --query 'StatusCode' \
    --output text | xargs -I{} echo "Lambda HTTP status: {}"

echo ""
echo "Response:"
cat "$RESPONSE_FILE" | jq '.body | fromjson'

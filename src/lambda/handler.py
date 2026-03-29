"""
Lambda API Handler — Phase 2
Returns a clean employee profile JSON from DynamoDB single-table.
"""
import json
import os
from decimal import Decimal

import boto3

# Module-level client — reused across warm Lambda invocations
_dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    table_name = os.environ["DYNAMO_TABLE"]
    table = _dynamodb.Table(table_name)

    emp_id = event.get("employee_id")
    if not emp_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "employee_id is required"}),
        }

    response = table.get_item(
        Key={"PK": f"EMP#{emp_id}", "SK": "PROFILE"}
    )
    item = response.get("Item")

    if not item:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": f"Employee {emp_id} not found"}),
        }

    # DynamoDB returns Decimal for numeric fields — convert to float for JSON
    payload = {
        "EmployeeID": item.get("EmployeeID"),
        "Name": f"{item.get('FirstName', '')} {item.get('LastName', '')}".strip(),
        "Email": item.get("Email"),
        "Department": item.get("DepartmentName") or item.get("Department"),
        "JobTitle": item.get("JobTitle"),
        "Manager": item.get("ManagerName") or item.get("Manager"),
        "ManagerLevel": item.get("ManagerLevel"),
        "HireDate": item.get("HireDate"),
        "City": item.get("City"),
        "State": item.get("State"),
        "EmploymentStatus": item.get("EmploymentStatus"),
        "Salary": float(item["Salary"]) if item.get("Salary") else None,
        "CompaRatio": float(item["CompaRatio"]) if item.get("CompaRatio") else None,
        "HighestTitleSalary": (
            float(item["HighestTitleSalary"]) if item.get("HighestTitleSalary") else None
        ),
        "RequiresReview": item.get("RequiresReview"),
    }

    return {
        "statusCode": 200,
        # default=str handles any remaining Decimal or unexpected types
        "body": json.dumps(payload, default=str),
    }

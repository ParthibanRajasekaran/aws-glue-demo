#!/usr/bin/env python3
import os
import sys

# Ensure infrastructure/ is on the path so infrastructure_stack is importable
sys.path.insert(0, os.path.dirname(__file__))

import aws_cdk as cdk
from infrastructure_stack import InfrastructureStack

app = cdk.App()
InfrastructureStack(
    app,
    "HrPipelineStack",
    env=cdk.Environment(region="us-east-1"),
)
app.synth()

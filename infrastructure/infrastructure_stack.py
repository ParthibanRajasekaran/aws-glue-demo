import os

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
)
import aws_cdk.aws_glue_alpha as glue_alpha
from constructs import Construct


class InfrastructureStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── S3 Buckets ──────────────────────────────────────────────────────────
        raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        parquet_bucket = s3.Bucket(
            self,
            "AthenaParquetBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        assets_bucket = s3.Bucket(
            self,
            "AssetsBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ── DynamoDB Single-Table ────────────────────────────────────────────────
        table = dynamodb.Table(
            self,
            "SingleTable",
            table_name="aws-glue-demo-single-table",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── IAM Role for Glue ETL Job ────────────────────────────────────────────
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        # Least-privilege bucket grants (AWSGlueServiceRole only covers aws-glue-* buckets)
        raw_bucket.grant_read(glue_role)
        parquet_bucket.grant_read_write(glue_role)
        assets_bucket.grant_read(glue_role)
        # DynamoDB write (PutItem, BatchWriteItem, etc.)
        table.grant_write_data(glue_role)

        # ── Glue ETL Job ─────────────────────────────────────────────────────────
        glue_job = glue_alpha.PySparkEtlJob(
            self,
            "HrEtlJob",
            script=glue_alpha.Code.from_asset(
                os.path.join(
                    os.path.dirname(__file__), "..", "src", "glue", "etl_job.py"
                )
            ),
            glue_version=glue_alpha.GlueVersion.V4_0,
            worker_type=glue_alpha.WorkerType.G_1X,
            number_of_workers=2,
            default_arguments={
                "--raw_bucket": raw_bucket.bucket_name,
                "--parquet_bucket": parquet_bucket.bucket_name,
                "--dynamo_table": table.table_name,
                "--TempDir": f"s3://{assets_bucket.bucket_name}/temp/",
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
            },
            role=glue_role,
        )

        # ── Glue Data Catalog ─────────────────────────────────────────────────────
        cfn_database = glue.CfnDatabase(
            self,
            "HrAnalyticsDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="hr_analytics",
                description="HR analytics database — Phase 2 ETL pipeline",
            ),
        )

        # Separate crawler role: read-only on parquet bucket
        crawler_role = iam.Role(
            self,
            "CrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        parquet_bucket.grant_read(crawler_role)

        cfn_crawler = glue.CfnCrawler(
            self,
            "HrParquetCrawler",
            name="hr-parquet-crawler",
            role=crawler_role.role_arn,
            database_name="hr_analytics",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{parquet_bucket.bucket_name}/employees/"
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
        )
        # Ensure database exists before crawler is created
        cfn_crawler.add_dependency(cfn_database)

        # ── Lambda API Handler ────────────────────────────────────────────────────
        lambda_fn = lambda_.Function(
            self,
            "HrApiHandler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                os.path.join(
                    os.path.dirname(__file__), "..", "src", "lambda"
                )
            ),
            environment={
                "DYNAMO_TABLE": table.table_name,
            },
            timeout=Duration.seconds(30),
        )
        # Least-privilege: GetItem, BatchGetItem, Query, Scan only
        table.grant_read_data(lambda_fn)

        # ── Stack Outputs ─────────────────────────────────────────────────────────
        cdk.CfnOutput(
            self, "RawBucketName",
            value=raw_bucket.bucket_name,
            description="S3 bucket for raw CSV input files",
        )
        cdk.CfnOutput(
            self, "ParquetBucketName",
            value=parquet_bucket.bucket_name,
            description="S3 bucket for Parquet ETL output (Athena)",
        )
        cdk.CfnOutput(
            self, "AssetsBucketName",
            value=assets_bucket.bucket_name,
            description="S3 bucket for Glue temp files",
        )
        cdk.CfnOutput(
            self, "GlueJobName",
            value=glue_job.job_name,
            description="Glue ETL job name",
        )
        cdk.CfnOutput(
            self, "LambdaFunctionName",
            value=lambda_fn.function_name,
            description="Lambda API handler function name",
        )
        cdk.CfnOutput(
            self, "DynamoTableName",
            value=table.table_name,
            description="DynamoDB single-table name",
        )

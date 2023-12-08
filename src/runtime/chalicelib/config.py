import os
import json
from aws_lambda_powertools import Logger, Tracer


class Config:
    """App configuration & environment variables."""

    app_name = "dynamodb-redshift"
    logger = Logger(service=app_name, log_uncaught_exceptions=True)
    tracer = Tracer(service=app_name)

    # General config
    DEFAULT_REGION = os.environ.get("DEFAULT_REGION", "us-east-1")
    AWS_STAGE_ENV = os.environ.get("AWS_STAGE_ENV").lower()

    # S3 config
    S3_BUCKET = os.environ.get("S3_BUCKET", "dev-ecocart-data-lake")
    S3_BUCKET_PREFIX = os.environ.get("S3_BUCKET_PREFIX", "dev/")

    # Reshift config
    REDSHIFT_CLUSTER = os.environ.get("REDSHIFT_CLUSTER")
    REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE")
    REDSHIFT_TARGET_SCHEMA = os.environ.get("REDSHIFT_TARGET_SCHEMA")
    REDSHIFT_USERNAME = os.environ.get("REDSHIFT_USERNAME")
    REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")
    REDSHIFT_HOSTNAME = os.environ.get("REDSHIFT_HOSTNAME")
    REDSHIFT_PORT = os.environ.get("REDSHIFT_PORT")
    # REDSHIFT_SECRET_ID = os.environ.get("REDSHIFT_SECRET_ID", "secret_name_in_secret_manager")
    # REDSHIFT_IAM_ROLE = os.environ.get("REDSHIFT_IAM_ROLE", "better to use IAM role vs username/password")

    # Dynamo DB config
    with open(os.path.join(os.path.dirname(__file__), "dynamodb_exports.json")) as f:
        dynamodb_exports = json.load(f)

    TABLE_ARN_PREFIX = os.environ.get("TABLE_ARN_PREFIX")
    FULL_EXPORT_TABLES = dynamodb_exports["full_export"][AWS_STAGE_ENV]
    INCREMENTAL_EXPORT_TABLES = dynamodb_exports["incremental_export"][AWS_STAGE_ENV]

    # Mapping config
    with open(os.path.join(os.path.dirname(__file__), "table_mapping.json")) as f:
        table_mapping = json.load(f)

    TABLE_DETAILS = table_mapping

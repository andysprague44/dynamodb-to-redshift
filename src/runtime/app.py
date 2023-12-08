from datetime import datetime
import json
from contextlib import contextmanager
import boto3
from chalice import Chalice, Rate
import psycopg2
from chalice.app import ConvertToMiddleware
from datetime import datetime
from urllib.parse import unquote

try:
    from chalicelib.config import Config
    from chalicelib.exceptions import RedshiftQueryException
    from chalicelib import (
        dynamodb_export_handler,
        redshift_manifest_handler,
        redshift_upsert_handler,
    )
except:
    # Not ideal but required for pytest
    from .chalicelib.config import Config
    from .chalicelib.exceptions import RedshiftQueryException
    from .chalicelib import (
        dynamodb_export_handler,
        redshift_manifest_handler,
        redshift_upsert_handler,
    )

app = Chalice(app_name="dynamo-redshift")

app.register_middleware(ConvertToMiddleware(Config.logger.inject_lambda_context))
app.register_middleware(ConvertToMiddleware(Config.tracer.capture_lambda_handler))

_SECRETSMANAGER_CLIENT = None
_S3_CLIENT = None
_DYNAMODB_CLIENT = None


def get_secretsmanager_client():
    """For production use, store your redshift credentials in secrets manager"""
    global _SECRETSMANAGER_CLIENT
    if _SECRETSMANAGER_CLIENT is None:
        _SECRETSMANAGER_CLIENT = boto3.client(
            service_name="secretsmanager",
            region_name=Config.DEFAULT_REGION,
        )
    return _SECRETSMANAGER_CLIENT


def get_dynamodb_client():
    global _DYNAMODB_CLIENT
    if _DYNAMODB_CLIENT is None:
        _DYNAMODB_CLIENT = boto3.client("dynamodb")
    return _DYNAMODB_CLIENT


def get_s3_client():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


def get_credentials():
    secretsmanager_client = get_secretsmanager_client()
    credentials_response = secretsmanager_client.get_secret_value(
        SecretId=Config.REDSHIFT_SECRET_ID
    )
    credentials = json.loads(credentials_response["SecretString"])
    return credentials


@contextmanager  # this is to ensure each connection is 'use once'
def get_redshift_connection(credentials=None):
    """Get a single use psycopg2.connect object for redshift queries.

    Example
    -------
    with get_redshift_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM SOME_TABLE LIMIT 1")
        results = cur.fetchall()
    """
    try:
        credentials = credentials or get_credentials()
        conn = psycopg2.connect(
            database=Config.REDSHIFT_DATABASE,
            user=credentials.get("username"),
            password=credentials.get("password"),
            host=credentials.get("host"),
            port=credentials.get("port"),
        )
    except Exception as ex:
        raise Exception("Unable to establish a new redshift connection: {ex}") from ex

    try:
        yield conn
    except Exception as ex:
        raise RedshiftQueryException(str(ex)) from ex
    finally:
        conn.close()


@app.route("/health")
def index():
    Config.logger.debug(f"health check for {Config.app_name}")
    return {"status": "ok", "datetime": str(datetime.now())}


@app.schedule(Rate(1, Rate.DAYS))
# @app.route("/full-export")  # should not check in this line uncommented, use for manual testing only!
def dynamodb_full_export(event=None):
    """
    Do full export from dynamodb tables to s3.
    """
    response = dynamodb_export_handler.handle(
        dynamodb_client=get_dynamodb_client(),
        s3_client=get_s3_client(),
        tables=Config.FULL_EXPORT_TABLES,
        is_incremental=False,
    )
    return json.dumps(response, default=str)


@app.schedule(Rate(15, Rate.MINUTES))
# @app.route("/incremental-export")  # should not check in this line uncommented, use for manual testing only!
def dynamodb_incremental_export(event=None):
    """
    Do incremental export from dynamodb tables to s3.
    """
    response = dynamodb_export_handler.handle(
        dynamodb_client=get_dynamodb_client(),
        s3_client=get_s3_client(),
        tables=Config.INCREMENTAL_EXPORT_TABLES,
        is_incremental=True,
    )
    return json.dumps(response, default=str)


@app.on_s3_event(
    bucket=Config.S3_BUCKET,
    events=["s3:ObjectCreated:*"],
    prefix=Config.S3_BUCKET_PREFIX + "dynamodb-export/",
    suffix="manifest-summary.json",
)
def redshift_manifest_creation(event=None):
    manifest_summary_file = event.key

    Config.logger.info("file received " + manifest_summary_file)

    response = redshift_manifest_handler.handle(
        get_s3_client(),
        manifest_summary_file,
    )
    return response


@app.on_s3_event(
    bucket=Config.S3_BUCKET,
    events=["s3:ObjectCreated:*"],
    prefix=Config.S3_BUCKET_PREFIX + "dynamodb-export/",
    suffix="redshift.manifest",
)
def redshift_upsert(event=None):
    manifest_summary_file = event.key

    Config.logger.info("file received " + manifest_summary_file)

    response = redshift_upsert_handler.handle(
        get_s3_client(),
        get_credentials(),
        get_redshift_connection,  # intentionally not calling this function
        manifest_summary_file,
    )
    return response

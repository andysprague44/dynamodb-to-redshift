from chalice.test import Client
from botocore.stub import Stubber
import json
import os
import pytest
import boto3
from moto import mock_s3
from src.runtime.chalicelib import dynamodb_export_handler
from datetime import datetime, timedelta
from src.runtime.chalicelib.config import Config, s3_utils
from src.runtime import app

Config.AWS_STAGE_ENV = "test"
Config.S3_BUCKET = "my-test-bucket"
Config.S3_BUCKET_PREFIX = "test/"


TEST_REDSHIFT_CREDENTIALS = {
    "host": "test_host",
    "database": "test_database",
    "port": "test_port",
    "username": "test_username",
    "password": "test_password",
}


@pytest.fixture
def test_client():
    with Client(app.app) as client:
        yield client


@pytest.fixture
def s3_stub():
    client = app.get_s3_client()
    stub = Stubber(client)
    with stub:
        yield stub


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_mock(aws_credentials):
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        conn.create_bucket(Bucket=Config.s3_bucket)
        yield conn


@pytest.fixture
def secretsmanager_stub():
    client = app.get_secretsmanager_client()
    stub = Stubber(client)
    with stub:
        yield stub


@pytest.fixture
def redshift_connection_mock(mocker):
    """mocker.patch for 'psycopg2.connect'"""
    mock_connect = mocker.patch("psycopg2.connect")
    yield mock_connect


@pytest.fixture
def redshift_data_stub():
    """Stubber for boto3('redshift-data')"""
    client = app.get_redshift_data_client()
    stub = Stubber(client)
    with stub:
        yield stub


@pytest.mark.skip(reason="manual testing only, it runs a real export")
def test_run_full_export_manual():
    response = dynamodb_export_handler.handle(
        dynamodb_client=app.get_dynamodb_client(),
        s3_client=app.get_s3_client(),
        tables="TableToTest",
        is_incremental=False,
        export_time=datetime.now(),
    )
    assert response is not None


@pytest.mark.skip(reason="manual testing only, it runs a real export")
def test_run_incremental_export_manual():
    """
    This manual test runs stage 1, a dynamodb export to s3.
    """
    now = datetime.now()
    response = dynamodb_export_handler.handle(
        dynamodb_client=app.get_dynamodb_client(),
        s3_client=app.get_s3_client(),
        tables="TableToTest",
        is_incremental=True,
        export_time=now,
        export_from_datetime=now - timedelta(minutes=15),
    )
    assert response is not None


@pytest.mark.ignore("manual test only, it uses a real redshift cluster")
def test_upsert_handle_manual(test_client):
    """
    This manual test runs stages 2 and 3.
    First, run an export from dynamodb to s3, and then update the `manifest_summary_file` s3 path below.
    """
    manifest_summary_file = "path/to/manifest-summary.json"
    result = test_client.lambda_.invoke(
        function_name="redshift_manifest_creation",
        payload=test_client.events.generate_s3_event(
            bucket=Config.S3_BUCKET,
            key=manifest_summary_file,
        ),
    )
    redshift_manifest_file = result.payload
    result = test_client.lambda_.invoke(
        function_name="redshift_upsert",
        payload=test_client.events.generate_s3_event(
            bucket=Config.S3_BUCKET,
            key=redshift_manifest_file,
        ),
    )
    assert result is not None


@pytest.mark.skip(reason="manual integration test")
def test_load_data_captured_in_full_export():
    # First, run the export and ensure the export has completed before running this test (wait 15 mins or so)
    s3_client = app.get_s3_client()
    s3_bucket = Config.s3_bucket
    table_s3_prefix = "dev/backup/dev-financial-orchestration-layer-devfinorchlayertable3811453F-1659MMDFU9LOO"

    # read exported data from s3
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=table_s3_prefix,
    )
    manifest_files = [
        obj
        for obj in response["Contents"]
        if obj["Key"].endswith("manifest-files.json")
    ]
    latest_manifest_file = max(manifest_files, key=lambda o: o["LastModified"])
    data_files = s3_utils.read_json_from_s3(
        s3_client,
        s3_bucket,
        latest_manifest_file["Key"],
    )
    files = [x["dataFileS3Key"] for x in data_files if x["dataFileS3Key"]]
    data = [s3_utils.read_json_from_s3(s3_client, s3_bucket, f, 3) for f in files]
    data = [item for sublist in data for item in sublist]  # flatten list of lists

    assert len(data) > 0

    # write to file
    for d in data:
        d["Item"]["inputs"] = "..."
        d["Item"]["outputs"] = "..."

    file = os.path.join(os.path.dirname(__file__), "fixtures", "full-export.json")
    with open(file, "w") as f:
        json.dump(data, f, indent=4)


@pytest.mark.skip(reason="manual integration test")
def test_load_data_captured_in_incremental_export():
    # First, run the export after manual changes to the data in the table.
    # Ensure the export has completed before running this test.
    # For example, delete a row, update a row, add a row, run export, wait 15 mins, run this test.
    s3_client = app.get_s3_client()
    s3_bucket = Config.s3_bucket
    table_s3_prefix = "dev/incremental-export/dev-financial-orchestration-layer-devfinorchlayertable3811453F-1659MMDFU9LOO"

    # read exported data from s3
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=table_s3_prefix,
    )
    manifest_files = [
        obj
        for obj in response["Contents"]
        if obj["Key"].endswith("manifest-files.json")
    ]
    latest_manifest_file = max(manifest_files, key=lambda o: o["LastModified"])
    data_files = s3_utils.read_json_from_s3(
        s3_client,
        s3_bucket,
        latest_manifest_file["Key"],
    )
    files = [x["dataFileS3Key"] for x in data_files]
    data = [s3_utils.read_json_from_s3(s3_client, Config.s3_bucket, f) for f in files]
    data = [item for sublist in data for item in sublist]  # flatten list of lists

    assert len(data) > 0

    # write to file
    for d in data:
        if "NewImage" in d:
            d["NewImage"]["inputs"] = "..."
            d["NewImage"]["outputs"] = "..."
    sort_keys_of_interest = {
        "00012ec0-2ea0-4f20-9dcc-379593a0c846": "update",
        "00012ec0-2ea0-4f20-9dcc-379593a0c847": "new row",
        "0001ad06-49c2-468c-ab5d-2013c130c21e": "deleted",
    }
    data = [x for x in data if x["Keys"]["SK"]["S"] in sort_keys_of_interest.keys()]
    file = os.path.join(
        os.path.dirname(__file__), "fixtures", "incremental-export.json"
    )
    with open(file, "w") as f:
        json.dump(data, f, indent=4)


def test_index(test_client):
    result = test_client.http.get("/health")
    assert result.json_body.get("status") == "ok"


def test_redshift_boto3_stub(redshift_data_stub):
    # Assemble
    expected = {"Id": "test_id", "ClusterIdentifier": "test_cluster_identifier"}
    redshift_data_stub.add_response("execute_statement", expected)
    redshift_data_stub.activate()
    redshift_data_client = redshift_data_stub.client

    # Act
    response = redshift_data_client.execute_statement(
        ClusterIdentifier="test_cluster_identifier",
        Database="test_database",
        SecretArn=Config.REDSHIFT_CLUSTER,
        Sql="SELECT * FROM SHOULD_BE_MOCKED",
    )

    # Assert
    assert response == expected

    redshift_data_stub.assert_no_pending_responses()


def test_redshift_psycopg2_mock(secretsmanager_stub, redshift_connection_mock):
    # Assemble
    expected = [["fake", "row", 1], ["fake", "row", 2]]
    mock_cursor = redshift_connection_mock.return_value.cursor
    mock_cursor.return_value.fetchall.return_value = expected

    secret = {"SecretString": json.dumps(TEST_REDSHIFT_CREDENTIALS)}
    secretsmanager_stub.add_response("get_secret_value", secret)
    secretsmanager_stub.activate()
    secretsmanager_client = secretsmanager_stub.client

    def _fake_redshift_query():
        "In this test, we are 'testing' this function, just to assert the mocking is set-up correctly"
        with app.get_redshift_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM SHOULD_BE_MOCKED")
            cur.execute("SELECT * FROM STILL_SHOULD_BE_MOCKED")
            data = cur.fetchall()
        return data

    # Act
    data = _fake_redshift_query()

    # Assert
    assert data == expected

    secretsmanager_stub.assert_no_pending_responses()

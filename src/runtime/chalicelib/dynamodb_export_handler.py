from datetime import datetime, timedelta
import time
from aws_lambda_powertools import Logger
from typing import List, Union, Any
from . import s3_utils
from .config import Config

logger = Logger()


def handle(
    dynamodb_client: Any,
    s3_client: Any,
    tables: Union[str, List[str]],
    is_incremental: bool = False,
    s3_bucket: str = Config.s3_bucket,
    export_time: datetime = None,
    export_from_datetime: datetime = None,
) -> Union[Any, List[Any]]:
    """
    Handle backup, or incremental export, of tables from dynamodb to s3.

    Args:
        - dynamodb_client: boto3 dynamodb client
        - s3_client: boto3 s3 client
        - tables: list of tables to export, or single table
        - is_incremental: whether to export incrementally
        - s3_bucket: s3 bucket to export to
        - export_time: datetime to export to, defaults to now
        - export_from_datetime: datetime to export from (incremental mode only), if None looks it up from s3

    Returns:
        - list of responses from dynamodb export, or single reponse if single table

    Raises:
        - ValueError: if no tables to export
        - Exception: if any error found backing up tables
    """
    export_time = export_time or datetime.now()

    if not tables:
        raise ValueError("No tables to export")

    is_single_table = not isinstance(tables, list)
    if is_single_table:
        tables = [tables]

    errors = []
    responses = []

    for table in tables:
        try:
            if not is_incremental:
                logger.info(f"backing up table {table}")
                table_s3_prefix = (
                    f"{Config.s3_bucket_prefix}/dynamodb-export/full-export/{table}"
                )
                response = dynamodb_client.export_table_to_point_in_time(
                    S3Bucket=s3_bucket,
                    S3Prefix=table_s3_prefix,
                    TableArn=Config.table_arn_prefix + table,
                    ExportTime=export_time,
                    S3SseAlgorithm="AES256",
                    ExportFormat="DYNAMODB_JSON",
                    ExportType="FULL_EXPORT",
                )
                responses.append(response)

            else:
                logger.info(f"incrementally exporting table {table}")
                table_s3_prefix = f"{Config.s3_bucket_prefix}/dynamodb-export/incremental-export/{table}"
                last_export_s3_path = f"{table_s3_prefix}/last-export-time.txt"
                last_export_to_datetime = _get_last_export_to_datetime(
                    s3_client=s3_client,
                    s3_bucket=s3_bucket,
                    last_export_s3_path=last_export_s3_path,
                )
                export_from_datetime = export_from_datetime or last_export_to_datetime

                specs = _get_incremental_export_specifications(
                    from_time=export_from_datetime,
                    to_time=export_time,
                )

                for spec in specs:
                    response = dynamodb_client.export_table_to_point_in_time(
                        S3Bucket=s3_bucket,
                        S3Prefix=table_s3_prefix,
                        TableArn=Config.table_arn_prefix + table,
                        ExportTime=export_time,
                        S3SseAlgorithm="AES256",
                        ExportFormat="DYNAMODB_JSON",
                        ExportType="INCREMENTAL_EXPORT",
                        IncrementalExportSpecification=spec,
                    )
                    responses.append(response)

                    # update the last export time
                    if export_time > last_export_to_datetime:
                        s3_client.put_object(
                            Bucket=s3_bucket,
                            Key=last_export_s3_path,
                            Body=export_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        )

                    # If multiple specs, add a pause to ensure ordering
                    if len(specs) > 1:
                        time.sleep(10)

        except Exception as ex:
            # don't throw here, attempt other tables first
            logger.error(f"Error when backing up or exporting table {table}: {ex}")
            errors.append({"Table": table, "Exception": ex})

    if errors:
        raise Exception(
            f"1 or more errors found backing up tables from DynamoDB to s3 (incremental={is_incremental}). See logs for more details."
        ) from errors[0]["Exception"]

    return responses[0] if is_single_table else responses


def _get_last_export_to_datetime(
    s3_client: Any,
    s3_bucket: str,
    last_export_s3_path: str,
):
    """Get the datetime to export from, based on the last `export to` time in s3, or 24 hours ago if no previous export."""
    if not s3_utils.exists(s3_client, s3_bucket, last_export_s3_path):
        export_from_datetime = datetime.now() - timedelta(days=1)
        return export_from_datetime

    date_str = s3_utils.read_contents_from_s3(s3_client, s3_bucket, last_export_s3_path)
    export_from_datetime = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return export_from_datetime


def _get_incremental_export_specifications(
    from_time: datetime,
    to_time: datetime,
) -> List[dict]:
    """
    Get specifications for incremental export, from `from_time` to `to_time` (normally 'now').

    In normal operation, returns a single spec from the time of last export to `to_time` (e.g. 15 mins ago to now)

    If a job has been paused or broken for more than 24 hours (noting incremental mode doesn't
    support a period larger than 24 hours), we will get multiple specs for each 24 hour period.
    For example, if the previous export was 2.5 days ago, we will perform 3 exports:
        - 2.5 -> 1.5 days ago
        - 1.5 -> 0.5 days ago
        - 0.5 -> now
    """
    incremental_export_specifications = [
        {
            "ExportFromTime": from_time + timedelta(days=x),
            "ExportToTime": min(from_time + timedelta(days=x + 1), to_time),
            "ExportViewType": "NEW_AND_OLD_IMAGES",
        }
        for x in range((to_time - from_time - timedelta(milliseconds=1)).days + 1)
    ]
    return incremental_export_specifications

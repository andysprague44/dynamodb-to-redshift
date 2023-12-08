import os
import json
import gzip
from typing import Any, Union
from . import s3_utils, redshift_utils
from .config import Config


def handle(
    s3_client: Any,
    manifest_summary_file: str,
):
    """
    Processes s3 exports into a format Redshift can ingest.
    Creates a new manifest file that redshift COPY command can act on.

    Args:
        s3_client (Any): boto3 s3 client
        manifest_summary_file (str): s3 path to json manifest summary file

    Returns:
        str: s3 path to redshift manifest file
    """
    Config.logger.info("file received " + manifest_summary_file)

    manifest_summary = s3_utils.read_json_from_s3(
        s3_client,
        Config.S3_BUCKET,
        manifest_summary_file,
    )
    is_incremental = manifest_summary.get("exportType", None) == "INCREMENTAL_EXPORT"
    table_s3_prefix = manifest_summary["s3Prefix"]
    dynamodb_table_name = table_s3_prefix.split("/")[-1]
    export_s3_directory = os.path.dirname(manifest_summary_file)

    processed_files = []

    manifest_files_path = manifest_summary["manifestFilesS3Key"]
    manifest_files_content = s3_utils.read_json_from_s3(
        s3_client,
        Config.S3_BUCKET,
        manifest_files_path,
    )
    data_files = [x["dataFileS3Key"] for x in manifest_files_content]

    if is_incremental:
        # For incremental; deletes, updates, inserts can be in the same file
        # and we need to do some work upfront for redshift
        processed_files = [
            _process_data_file(s3_client, table_s3_prefix, file) for file in data_files
        ]
    else:
        # For full export, no processing required
        processed_files = data_files

    processed_files = [f for f in processed_files if f is not None]

    # If all processed files are None, then they are all empty, and there is nothing to process
    if not processed_files:
        Config.logger.info(f"All files are empty, skipping")
        empty_marker_path = f"{export_s3_directory}/processed_no_data.txt"
        s3_client.put_object(Bucket=Config.S3_BUCKET, Key=empty_marker_path, Body="")
        return None

    # add required info to the redshift manifest file, so the COPY command can use it
    dynamodb_table_name = table_s3_prefix.split("/")[-1]
    table_details = Config.TABLE_DETAILS.get(dynamodb_table_name, None)
    if table_details is None:
        raise Exception(
            f"Unable to find table details for {dynamodb_table_name} in table_mapping.json"
        )
    json_paths = table_details["jsonpaths"]
    redshift_table = (
        Config.REDSHIFT_TARGET_SCHEMA + "." + table_details["redshift_table"]
    )
    partition_key = table_details["partition_key"]
    sort_key = table_details.get("sort_key", None)
    format_time = table_details["format_time"]

    redshift_manifest = {
        "entries": [
            {"url": f"s3://{Config.S3_BUCKET}/{p}", "mandatory": True}
            for p in processed_files
        ],
        "dynamodb_table_name": dynamodb_table_name,
        "is_incremental": is_incremental,
        "redshift_table": redshift_table,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "format_time": format_time,
        "jsonpaths": json_paths,
    }

    # write the manifest file to s3
    redshift_manifest_path = f"{export_s3_directory}/redshift.manifest"
    Config.logger.info(f"Saving redshift manifest to {redshift_manifest_path}")
    s3_client.put_object(
        Bucket=Config.S3_BUCKET,
        Key=redshift_manifest_path,
        Body=json.dumps(redshift_manifest, indent=4),
    )

    return redshift_manifest_path


def _process_data_file(
    s3_client: Any,
    table_s3_prefix: str,
    file: str,
) -> Union[str, None]:
    """
    Processes a single data file from dynamodb export into a format Redshift can ingest.

    Args:
        s3_client (Any): boto3 s3 client
        table_s3_prefix (str): s3 path to dynamodb table
        file (str): s3 path to data file

    Returns:
        str: s3 path to processed data file, or None if the file is empty
    """
    Config.logger.info(f"Processing file {file}")
    contents = s3_utils.read_contents_from_s3(
        s3_client,
        Config.S3_BUCKET,
        file,
    )
    if not contents:
        Config.logger.info(f"File {file} is empty, skipping")
        return None

    lines = contents.splitlines()

    def process_row(line):
        line = json.loads(line)
        if "NewImage" not in line:
            # handle deletion
            line["Item"] = line.pop("OldImage")
            line["Item"]["is_active"] = {"BOOL": False}
        else:
            # handle new item and update item
            line["Item"] = line.pop("NewImage")
            line["Item"]["is_active"] = {"BOOL": True}
            line.pop("OldImage", None)  # ignore old image in the case of update
        return json.dumps(line)

    results = [process_row(line) for line in lines]
    processed_contents = "\n".join(list(results))
    compressed_contents = gzip.compress(bytes(processed_contents, "utf-8"))
    processed_dir = f"{table_s3_prefix}/AWSDynamoDB/processed"
    processed_file_path = f"{processed_dir}/{file.split('/')[-1]}"

    Config.logger.info(f"Saving processed file to {processed_file_path}")
    s3_client.put_object(
        Bucket=Config.S3_BUCKET,
        Key=processed_file_path,
        Body=compressed_contents,
    )
    return processed_file_path

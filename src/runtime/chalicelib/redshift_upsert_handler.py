from typing import Any, Callable
from . import s3_utils, redshift_utils
from .config import Config


def handle(
    s3_client: Any,
    credentials: dict,
    get_redshift_connection_callback: Callable[..., Any],
    redshift_manifest_file: str,
):
    """
    Handle the Redshift UPSERT for the table described by the
    redshift.mainfest file that triggered the lambda.
    """
    Config.logger.info("redshift manifest received " + redshift_manifest_file)

    redshift_manifest = s3_utils.read_json_from_s3(
        s3_client,
        Config.S3_BUCKET,
        redshift_manifest_file,
    )
    dynamodb_table_name = redshift_manifest["dynamodb_table_name"]
    is_incremental = redshift_manifest["is_incremental"]
    target = redshift_manifest["redshift_table"]
    source = f"#{target}_staging"  # temp table
    partition_key = redshift_manifest["partition_key"]
    sort_key = redshift_manifest.get("sort_key", None)
    format_time = redshift_manifest["format_time"]
    # no need to load jsonpaths, they are only used in the COPY

    Config.logger.info(f"Upserting from {dynamodb_table_name} to {target}")
    with get_redshift_connection_callback(credentials) as conn:
        try:
            conn.rollback()  # ensure no transaction is open
            cur = conn.cursor()

            # create a temp table to load s3 json files to
            Config.logger.info(f"Executing create temp table")
            create_source_table_command = f"""
            DROP TABLE IF EXISTS {source};
            CREATE TABLE {source} (LIKE {target});
            ALTER TABLE {source} ADD COLUMN is_active BOOLEAN; -- for incremental loads
            """
            cur.execute(create_source_table_command)

            # load s3 files into temp table
            Config.logger.info(f"Executing COPY from s3 to temp table")
            copy_command = f"""
            COPY {source} FROM 's3://{Config.S3_BUCKET}/{redshift_manifest_file}' 
            credentials 'aws_access_key_id={credentials['aws_access_key_id']};aws_secret_access_key={credentials['aws_secret_access_key']}'
            json 's3://{Config.S3_BUCKET}/{redshift_manifest_file}'
            gzip
            {format_time}
            MANIFEST;
            """
            cur.execute(copy_command)

            if is_incremental:
                # Delete records that are deleted in DynamoDB
                Config.logger.info(f"Executing DELETE from temp table to {target}")
                delete_command = f"""
                DELETE FROM {target} USING {source} AS source
                WHERE {target}.{partition_key} = source.{partition_key}
                {(f'AND {target}.{sort_key} = source.{sort_key}' if sort_key else '')}
                AND source.is_active = FALSE;
                """
                cur.execute(delete_command)

                # Upsert (MERGE) the remaining records
                # See https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html#sub-examples-merge
                Config.logger.info(f"Executing MERGE from temp table to {target}")
                merge_command = f"""
                DELETE FROM {source} WHERE is_active = FALSE;
                ALTER TABLE {source} DROP COLUMN is_active; --columns must match target for merge
                SELECT * INTO {source}_active FROM {source}; --but dropping the column doesn't work in the same transaction
                MERGE INTO {target} USING {source}_active AS source
                ON {target}.{partition_key} = source.{partition_key}
                {(f'AND {target}.{sort_key} = source.{sort_key}' if sort_key else '')}
                REMOVE DUPLICATES;
                """
                cur.execute(merge_command)

            else:
                Config.logger.info(f"Executing REPLACE from temp table to {target}")
                replace_command = f"""
                ALTER TABLE {source} DROP COLUMN is_active; -- all records are active in full export
                TRUNCATE TABLE {target};
                INSERT INTO {target} SELECT * FROM {source};"""
                cur.execute(replace_command)

            # Clean up
            cur.execute(f"DROP TABLE IF EXISTS {source};")
            cur.execute(f"DROP TABLE IF EXISTS {source}_active;")

            # Commit transaction
            conn.commit()

        except:
            conn.rollback()
            raise
    return target

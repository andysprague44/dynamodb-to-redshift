import json
import gzip
from typing import Any


def read_json_from_s3(
    s3_client: Any,
    s3_bucket: str,
    s3_file_path: str,
) -> dict:
    """
    Read json file from s3 and convert to dict.
    - Can handle gzip files.
    - Can handle files with multiple lines, where each line is a
      json object but the file is not (which is the case for dynamoDB exports).
    """
    contents = read_contents_from_s3(
        s3_client,
        s3_bucket,
        s3_file_path,
    )

    try:
        json_contents = json.loads(contents)
        return json_contents

    except Exception as ex:
        # try reading by line
        try:
            json_contents = json.loads(f"[{','.join(contents.splitlines())}]")
            return json_contents

        except Exception:
            # throw original exception
            raise Exception(
                f"Error reading s3 file as json '{s3_bucket}:{s3_file_path}'"
            ) from ex


def read_contents_from_s3(
    s3_client: Any,
    s3_bucket: str,
    s3_file_path: str,
) -> str:
    """
    Read file from s3 and return as a string. Can handle gzip files.
    """
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_path)
    if s3_file_path.endswith(".gz"):
        with gzip.GzipFile(fileobj=obj["Body"]) as gzipfile:
            return gzipfile.read().decode("utf-8")
    else:
        return obj["Body"].read().decode("utf-8")

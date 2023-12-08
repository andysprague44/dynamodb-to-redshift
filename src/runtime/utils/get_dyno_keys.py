import boto3
import json
import os

"""
This helper script scrawls dynamo db and extracts the primary and sort keys.
Must have boto3 installed and configured with AWS credentials. See:
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
"""

dynamodb_client = boto3.client("dynamodb")
tables = dynamodb_client.list_tables()["TableNames"]

table_mapping = {}
for table in tables:
    print(f'Getting partion and sort keys for table "{table}"')
    desc_table = dynamodb_client.describe_table(TableName=table)

    keys = desc_table["Table"]["KeySchema"]

    partition_key = [x["AttributeName"] for x in keys if x["KeyType"] == "HASH"][0]

    # sort key optional in dynamodb, if None the table is unique by PK
    sort_key = next([x["AttributeName"] for x in keys if x["KeyType"] == "RANGE"], None)

    # Get 'attribute type' of keys
    atr = desc_table["Table"]["AttributeDefinitions"]
    pk_type = next(
        [x["AttributeType"] for x in atr if x["AttributeName"] == partition_key]
    )
    sk_type = next(
        [x["AttributeType"] for x in atr if x["AttributeName"] == sort_key], None
    )

    table_mapping[table] = {
        "partition_key": partition_key,
        "sort_key": sort_key,
        "pk_type": pk_type,
        "sk_type": sk_type,
    }


os.makedirs(os.path.join(os.path.dirname(__file__), "out"), exist_ok=True)
with open("out/dynamodb_pk_sk.json", "w") as f:
    json.dump(table_mapping, f, indent=4)

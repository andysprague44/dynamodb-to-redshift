# DynamoDB to Redshift incremental refresh

## Introduction

This application lays out a low cost approach to syncing incrementally (and/or full export) between a DynamoDB table and Redshift*, using S3 AWS Lambda, and API Gateway, with [Chalice](https://aws.github.io/chalice/) used as the framework for defining the appliction code. It uses python 3.9.

What it is not inteneded for:
- Streaming use-cases, for this I'd suggest using `Amazon Kinesis Data Streams for DynamoDB` and `AWS Glue`**
- Handling schema migrations and other maintainance considerations, if you need this look at out-of-the-box data ETL solutions 
like FiveTran, or move to an all-in solution like Databricks or Snowflake.
- Complex data transformations, for example breaking a list column in dynamo to multiple rows, I'd suggest adding `AWS Glue` if you need this.

*- * you could probably switch this for another data warehouse like `clickhouse` without too much effort.*

*- ** you could hook into a [chalice dynamodb-events trigger](https://aws.github.io/chalice/topics/events.html#dynamodb-events) such as `Chalice.on_kinesis_record()` and this approach would become stream compatible, if you try this please let me know how you get on!*

## Getting started

Copy the `.env.example` file and save as `.env` and update the values within. Note, this is not loaded automatically unless you use dev containers (you could add `python-dotenv` library if you want to).

Update the info in `src/runtime/chalicelib/dynamodb_exports.json` and in `src/runtime/chalicelib/table_mapping.json` to describe your tables.

### Run locally

Create a virtual env or conda env
```
conda env create -f environment.yml
conda activate dynamo-redshift
```

Run the app locally
```
cd src/runtime
chalice local --stage=dev
```

!! To do anything useful locally, you'll want to kick off exports from a REST API call. To do so, go into the `app.py` file and change the `dynamodb_full_export` and `dynamodb_incremental_export` method decorators to API gateway (rather than a schedule). Then you can trigger with e.g. `curl http://127.0.0.1:8000/incremental-export`

There are also manual tests in `tests/test_app.py` that will allow you to call each lamdba separately. These point to real AWS resources, so make sure your config points at a test env first!

### Debugging
If you use VS Code, you can run `Chalice: Local (conda)` or `Chalice: Local (venv)` configs, which does the above but allows debugging.


## Logic Overview

There are 3 steps, each a lamdba function.

### 1. Export DynamoDB to s3

The first lamdba `export_to_s3` exports from each table you want to sync, either a FULL_EXPORT or an INCREMENTAL_EXPORT (both cases are handled, full export might be better suited for slowly moving dimension tables).

In the incremental export mode, it uses a file stored in s3 `last-export-time.txt`, which is written to at the end of the lambda run. The next scheduled run can read this and use as the start time for the next period, to ensure no data is lost. On first run, it only takes a 24 hour period, so you should always run a full export for a new table first.

Note, the lambda function returns immediately, but the export runs async from the dynamo DB side, and takes 5 plus minutes depending on table size. The export is ultimately complete when a file `manifest-summary.json` is written to s3.

### 2. Process in s3

The next lambda `redshift_manifest_creation`, listens for creation of this `manifest-summary.json`, and, 
- a. pre-processes the data if required*, and 
- b. creates a `redshift.manifest` that redshift can use to ingest the data

This manifest file contains:
- a list of urls, which point to the data files to ingest, e.g. <s3://bucket-name/path/to/data/file.json>
- a jsonpaths object, that defines mapping between dynamodb nested json and the redshift table. Note, THE COLUMN ORDERS MUST MATCH.
- some additional meta-data required by our next lambda (i.e. the dynamodb table name, time format, is incremental)

*- ** For INCREMENTAL_EXPORT, there is a need to pre-process the files, so redshift can handle deletes, updates, and inserts gracefully**

### 3. Import to Redshift

The next lambda `redshift_upsert` listens for the creation of the `redshift.manifest` file in step 1, and uses it to upsert data to redshift.

- For incremental export mode, it uses a MERGE operation.
- For full export mode, it replaces the contents of the target table.


## Contact

<andy.sprague44@gmail.com>

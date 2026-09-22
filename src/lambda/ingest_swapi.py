"""Bronze-layer ingestion: fetch the SWAPI people endpoint and land the raw
JSON payload in S3 under ``raw/``.

Environment variables
---------------------
S3_BUCKET        target bucket (required)
RAW_PREFIX       filename prefix for the object, e.g. ``crew_data`` (required)
GLUE_JOB_NAME    when set, the Glue transform job is triggered after the upload
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SWAPI_URL = "https://swapi.info/api/people"

RAW_PREFIX = os.environ.get("RAW_PREFIX", "crew_data")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "")


def fetch_swapi_data():
    """Return the SWAPI people payload, or None when the API call fails."""
    try:
        response = requests.get(SWAPI_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        logger.error("Error fetching data from %s: %s", SWAPI_URL, exc)
        return None


def lambda_handler(event, context):
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET environment variable is not set")

    logger.info("Fetching characters from SWAPI")
    data = fetch_swapi_data()

    if data is None:
        return {
            "statusCode": 502,
            "body": json.dumps({"message": "Upstream API request failed"}),
        }

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/bronze_{RAW_PREFIX}_{timestamp}.json"

    boto3.client("s3").put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json",
    )
    logger.info("Landed %s records at s3://%s/%s", len(data), S3_BUCKET, s3_key)

    # Option A in docs/PIPELINE.es.md: chain the transform directly from here.
    # Leave GLUE_JOB_NAME unset to orchestrate with Step Functions instead.
    if GLUE_JOB_NAME:
        boto3.client("glue").start_job_run(JobName=GLUE_JOB_NAME)
        logger.info("Triggered Glue job %s", GLUE_JOB_NAME)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Ingested {len(data)} records",
            "s3_key": s3_key,
        }),
    }

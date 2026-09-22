"""Silver-layer transform: read the most recent bronze JSON from ``raw/``,
clean and enrich it, and write a single CSV to ``processed/``.

Every transformation is expressed with native Spark column functions. Python
UDFs would force a row-by-row round trip between the JVM and a Python worker
and hide the logic from Catalyst, so the whole plan stays in the engine.

Job parameters
--------------
--JOB_NAME    supplied by Glue
--S3_BUCKET   bucket holding the raw/ and processed/ prefixes
"""

import os
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"
BLANK_VALUES = ["", "n/a", "unknown", "null"]

REQUIRED_FIELDS = ["name", "height", "mass", "hair_color", "skin_color",
                   "eye_color", "birth_year", "gender"]

# Star Wars dates are counted Before the Battle of Yavin; year 0 is pinned to
# 2000 so the output carries a plain integer year instead of "19BBY".
BBY_EPOCH = 2000
BBY_PATTERN = r"^([0-9.]+)\s*[Bb][Bb][Yy]$"

KG_TO_LB = 2.20462

# Jabba the Hutt weighs 1358 kg and skews every aggregate downstream.
MAX_MASS_KG = 1000

# A row missing this many of its fields is not worth keeping.
MAX_BLANK_FIELDS = 3


def clean(column):
    """Trim and lowercase a column so it can be compared to BLANK_VALUES."""
    return F.lower(F.trim(column.cast("string")))


def is_blank(column):
    """True when the value is null or one of the placeholder strings."""
    return column.isNull() | clean(column).isin(BLANK_VALUES)


def numeric(column):
    """Parse a numeric string, tolerating thousands separators.

    Anything unparseable — "unknown", "n/a", an empty string — casts to null.
    """
    return F.regexp_replace(column.cast("string"), ",", "").cast("double")


args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
S3_BUCKET = args["S3_BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")
resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_PREFIX)
objects = [o for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
if not objects:
    raise RuntimeError(f"No JSON files found under s3://{S3_BUCKET}/{RAW_PREFIX}")

latest_key = max(objects, key=lambda o: o["LastModified"])["Key"]
input_path = f"s3://{S3_BUCKET}/{latest_key}"
print(f"Processing: {input_path}")

df = spark.read.option("multiLine", True).json(input_path)
df = df.select(*[c for c in REQUIRED_FIELDS if c in df.columns])

# "19BBY" -> 1981. regexp_extract yields "" when the pattern does not match;
# the when() leaves those rows null so the arithmetic below propagates it.
matched_bby = F.regexp_extract(F.col("birth_year"), BBY_PATTERN, 1)
bby_years = F.when(matched_bby != "", matched_bby)
df = df.withColumn(
    "normalized_birth_year",
    (F.lit(BBY_EPOCH) - bby_years.cast("double")).cast("int").cast("string"),
)

df = df.withColumn("mass_lb", F.round(numeric(F.col("mass")) * KG_TO_LB, 2))

df = df.withColumn(
    "gender_id",
    F.when(clean(F.col("gender")) == "male", "M")
     .when(clean(F.col("gender")) == "female", "F")
     .otherwise("N"),
)

# A null mass fails the comparison and drops the row, which is intentional:
# a character with no recorded weight cannot be validated against the cap.
df = df.filter(numeric(F.col("mass")) <= MAX_MASS_KG)

check_cols = REQUIRED_FIELDS + ["normalized_birth_year", "mass_lb", "gender_id"]
blank_count = sum(
    is_blank(F.col(c)).cast("int") for c in check_cols if c in df.columns
)
df = df.filter(blank_count < MAX_BLANK_FIELDS)

# Carry the bronze timestamp through so a silver file can always be traced
# back to the exact payload it came from.
timestamp = os.path.basename(latest_key).replace(".json", "").split("_", 2)[-1]
tmp_prefix = f"tmp/silver_crew_data_{timestamp}/"
final_key = f"{PROCESSED_PREFIX}silver_crew_data_{timestamp}.csv"

df.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"s3://{S3_BUCKET}/{tmp_prefix}"
)

# Spark writes a directory of part files; publish the single part under a
# predictable name and drop the staging directory.
parts = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=tmp_prefix).get("Contents", [])
part_key = next(o["Key"] for o in parts if o["Key"].endswith(".csv"))

s3.copy_object(
    Bucket=S3_BUCKET,
    CopySource={"Bucket": S3_BUCKET, "Key": part_key},
    Key=final_key,
)

for obj in parts:
    s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])

print(f"Output written to: s3://{S3_BUCKET}/{final_key}")

job.commit()

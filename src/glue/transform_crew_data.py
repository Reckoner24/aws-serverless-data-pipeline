"""Silver-layer transform: read the most recent bronze JSON from ``raw/``,
clean and enrich it, and write a single CSV to ``processed/``.

Job parameters
--------------
--JOB_NAME    supplied by Glue
--S3_BUCKET   bucket holding the raw/ and processed/ prefixes
"""

import os
import re
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType

RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"
BLANK_VALUES = {"", "n/a", "unknown", "null"}

REQUIRED_FIELDS = ["name", "height", "mass", "hair_color", "skin_color",
                   "eye_color", "birth_year", "gender"]

# Star Wars dates are counted Before the Battle of Yavin; year 0 is pinned to
# 2000 so the output carries a plain integer year instead of "19BBY".
BBY_EPOCH = 2000

# Jabba the Hutt weighs 1358 kg and skews every aggregate downstream.
MAX_MASS_KG = 1000

# A row missing this many of its fields is not worth keeping.
MAX_BLANK_FIELDS = 3


def parse_birth_year(birth_year):
    if not birth_year or str(birth_year).strip().lower() in BLANK_VALUES:
        return None
    match = re.match(r"^([\d.]+)\s*BBY$", str(birth_year).strip(), re.IGNORECASE)
    if not match:
        return None
    try:
        return str(int(BBY_EPOCH - float(match.group(1))))
    except ValueError:
        return None


def to_mass_lb(mass_str):
    if not mass_str or str(mass_str).strip().lower() in BLANK_VALUES:
        return None
    try:
        return round(float(str(mass_str).replace(",", "")) * 2.20462, 2)
    except ValueError:
        return None


def to_gender_id(gender):
    value = str(gender).strip().lower() if gender else ""
    return {"male": "M", "female": "F"}.get(value, "N")


def mass_numeric(mass_str):
    if not mass_str:
        return None
    try:
        return float(str(mass_str).replace(",", ""))
    except ValueError:
        return None


def is_blank(value):
    return value is None or str(value).strip().lower() in BLANK_VALUES


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

parse_birth_year_udf = F.udf(parse_birth_year, StringType())
df = df.withColumn("normalized_birth_year", parse_birth_year_udf(F.col("birth_year")))

mass_lb_udf = F.udf(to_mass_lb, DoubleType())
df = df.withColumn("mass_lb", mass_lb_udf(F.col("mass")))

gender_id_udf = F.udf(to_gender_id, StringType())
df = df.withColumn("gender_id", gender_id_udf(F.col("gender")))

mass_num_udf = F.udf(mass_numeric, DoubleType())
df = df.withColumn("_mass_num", mass_num_udf(F.col("mass")))
df = df.filter(F.col("_mass_num") <= MAX_MASS_KG).drop("_mass_num")

is_blank_udf = F.udf(is_blank, BooleanType())
check_cols = REQUIRED_FIELDS + ["normalized_birth_year", "mass_lb", "gender_id"]
blank_exprs = [is_blank_udf(F.col(c)).cast("int") for c in check_cols if c in df.columns]
df = df.withColumn("_blank_count", sum(blank_exprs))
df = df.filter(F.col("_blank_count") < MAX_BLANK_FIELDS).drop("_blank_count")

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

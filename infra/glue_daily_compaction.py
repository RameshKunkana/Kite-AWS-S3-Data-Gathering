import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


IST = timezone(timedelta(hours=5, minutes=30))


def _today_ist() -> str:
    return datetime.now(tz=IST).strftime("%Y-%m-%d")


def _arg_or_default(args: dict, key: str, default: str) -> str:
    value = args.get(key)
    return value if value else default


def _daily_prefix(root: str, date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return f"{root.strip('/')}/year={d:%Y}/month={d:%m}/day={d:%d}/"


def _discover_instrument_folders(
    s3_client,
    bucket: str,
    raw_prefix: str,
    instrument_folder: str | None,
) -> list[str]:
    grouped: dict[str, list[str]] = defaultdict(list)
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=raw_prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".parquet"):
                continue
            relative = key[len(raw_prefix):]
            folder, _, _ = relative.partition("/")
            if not folder:
                continue
            if instrument_folder and folder != instrument_folder:
                continue
            grouped[folder].append(key)
    return sorted(grouped)


def _delete_prefix(s3_client, bucket: str, prefix: str) -> None:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def _find_part_key(s3_client, bucket: str, prefix: str) -> str:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if "/part-" in key and key.endswith(".parquet"):
                return key
    raise RuntimeError(f"No parquet part file found under s3://{bucket}/{prefix}")


def main() -> None:
    known_args = ["JOB_NAME", "bucket", "raw_root_prefix", "processed_root_prefix", "region", "date", "instrument_folder"]
    provided = set(sys.argv[1:])
    resolved = ["JOB_NAME"] + [a for a in known_args[1:] if f"--{a}" in provided]
    args = getResolvedOptions(sys.argv, resolved)

    bucket = _arg_or_default(args, "bucket", "drify-market-data")
    raw_root_prefix = _arg_or_default(args, "raw_root_prefix", "ticks")
    processed_root_prefix = _arg_or_default(args, "processed_root_prefix", "processed")
    region = _arg_or_default(args, "region", boto3.session.Session().region_name or "ap-south-1")
    date_str = _arg_or_default(args, "date", _today_ist())
    instrument_folder = args.get("instrument_folder")

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    s3_client = boto3.client("s3", region_name=region)
    raw_prefix = _daily_prefix(raw_root_prefix, date_str)
    processed_prefix = _daily_prefix(processed_root_prefix, date_str)

    folders = _discover_instrument_folders(s3_client, bucket, raw_prefix, instrument_folder)
    if not folders:
        print(f"No parquet files found under s3://{bucket}/{raw_prefix}")
        job.commit()
        return

    print(f"Compacting {len(folders)} instrument(s) for {date_str}")
    run_id = uuid.uuid4().hex

    for folder in folders:
        source_uri = f"s3://{bucket}/{raw_prefix}{folder}/"
        temp_prefix = f"{processed_prefix}{folder}/__tmp_{run_id}/"
        temp_uri = f"s3://{bucket}/{temp_prefix}"
        final_key = f"{processed_prefix}{folder}/{folder}.parquet"

        df = spark.read.parquet(source_uri)
        if df.rdd.isEmpty():
            print(f"Skipping {folder}: no rows in source")
            continue
        if "event_time" in df.columns:
            df = df.orderBy("event_time")

        _delete_prefix(s3_client, bucket, temp_prefix)
        df.coalesce(1).write.mode("overwrite").parquet(temp_uri)

        part_key = _find_part_key(s3_client, bucket, temp_prefix)
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": part_key},
            Key=final_key,
        )
        _delete_prefix(s3_client, bucket, temp_prefix)
        print(f"Wrote s3://{bucket}/{final_key}")

    job.commit()


if __name__ == "__main__":
    main()

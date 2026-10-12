"""AWS Glue job: raw CSV zone -> curated Parquet zone (one table/partition at a time).

Job arguments:
    --BUCKET        lake bucket name
    --INGEST_DATE   "auto" = process every raw ingest_date partition that has no
                    curated partition yet; or a date (YYYY-MM-DD) to (re)process
                    that one partition. Re-running a date overwrites it (idempotent).
    --extra-py-files s3://<bucket>/scripts/olist_transforms.py

For each table and ingest_date:
    1. parse the raw CSV (multi-line, quoted) and cast to typed columns
    2. AWS Glue Data Quality ruleset on the whole batch (gate + published results)
    3. rows that break row-level rules -> quarantine/  (kept for review)
    4. drop primary-key duplicates, write valid rows -> curated/  (Parquet)
If any batch fails its Data Quality gate, that table/date is NOT written to
curated and the job fails at the end, so EventBridge sends an alert.
"""
import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext

import olist_transforms as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "INGEST_DATE"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
log = glue_context.get_logger()
s3 = boto3.client("s3")

STATUSES = '["approved","canceled","created","delivered","invoiced","processing","shipped","unavailable"]'
RULESETS = {
    "orders": f"""Rules = [
        RowCount > 0,
        Completeness "order_id" >= 0.99,
        Uniqueness "order_id" >= 0.99,
        Completeness "purchased_at" >= 0.99,
        ColumnValues "order_status" in {STATUSES} with threshold >= 0.99
    ]""",
    "order_items": """Rules = [
        RowCount > 0,
        Completeness "order_id" >= 0.99,
        ColumnValues "price" >= 0 with threshold >= 0.99
    ]""",
    "order_payments": """Rules = [
        RowCount > 0,
        Completeness "order_id" >= 0.99,
        ColumnValues "payment_value" >= 0 with threshold >= 0.99
    ]""",
    "order_reviews": """Rules = [
        RowCount > 0,
        Completeness "review_id" >= 0.99,
        ColumnValues "review_score" >= 1 with threshold >= 0.99,
        ColumnValues "review_score" <= 5 with threshold >= 0.99
    ]""",
    "customers": """Rules = [
        RowCount > 0,
        IsComplete "customer_id",
        Uniqueness "customer_id" >= 0.99
    ]""",
    "products": """Rules = [
        RowCount > 0,
        IsComplete "product_id",
        Uniqueness "product_id" >= 0.99
    ]""",
    "sellers": """Rules = [
        RowCount > 0,
        IsComplete "seller_id",
        Uniqueness "seller_id" >= 0.99
    ]""",
    "category_translation": """Rules = [
        RowCount > 0,
        IsComplete "product_category_name"
    ]""",
}


def partitions(prefix):
    """ingest_date values found directly under s3://BUCKET/<prefix>."""
    dates = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"].rstrip("/").split("/")[-1]
            if name.startswith("ingest_date="):
                dates.add(name.split("=", 1)[1])
    return dates


def dates_to_process(table):
    raw = partitions(f"raw/olist/{table}/")
    if args["INGEST_DATE"] != "auto":
        return sorted(raw & {args["INGEST_DATE"]})
    return sorted(raw - partitions(f"curated/olist/{table}/"))


def run_glue_dq(df, table, ingest_date):
    dyf = DynamicFrame.fromDF(df, glue_context, f"dq_{table}")
    results = EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=RULESETS[table],
        publishing_options={
            "dataQualityEvaluationContext": f"olist_{table}_{ingest_date.replace('-', '')}",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
    )
    outcomes = SelectFromCollection.apply(dfc=results, key="ruleOutcomes").toDF().collect()
    return [{"rule": r["Rule"], "outcome": r["Outcome"], "reason": r["FailureReason"]} for r in outcomes]


summary, failed_batches = [], []
for table, cleaner in T.CLEANERS.items():
    for ingest_date in dates_to_process(table):
        base = f"olist/{table}/ingest_date={ingest_date}/"
        raw = T.read_csv(spark, f"s3://{BUCKET}/raw/{base}")
        valid, bad = cleaner(raw)
        valid, bad = valid.cache(), bad.cache()

        dq = run_glue_dq(T.union_all([valid, bad.drop("dq_errors")]), table, ingest_date)
        dq_failed = [r for r in dq if r["outcome"] != "Passed"]

        n_bad = bad.count()
        if n_bad:
            bad.write.mode("overwrite").parquet(f"s3://{BUCKET}/quarantine/{base}")

        record = {"table": table, "ingest_date": ingest_date, "raw_rows": raw.count(),
                  "quarantined_rows": n_bad, "dq_rules": len(dq), "dq_failed": dq_failed}
        if dq_failed:
            failed_batches.append(f"{table}/{ingest_date}")
            record["status"] = "REJECTED"
        else:
            valid, dupes = T.dedupe(valid, table)
            valid.write.mode("overwrite").parquet(f"s3://{BUCKET}/curated/{base}")
            record.update(status="LOADED", curated_rows=valid.count(), duplicates_removed=dupes)
        log.info(json.dumps(record))
        print(json.dumps(record))
        summary.append(record)

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
s3.put_object(Bucket=BUCKET, Key=f"logs/raw_to_curated/run={run_id}.json",
              Body=json.dumps(summary, indent=2, default=str).encode("utf-8"))

if failed_batches:
    raise RuntimeError(f"Data Quality gate failed for: {', '.join(failed_batches)} "
                       f"(see quarantine/ and logs/raw_to_curated/run={run_id}.json)")

job.commit()
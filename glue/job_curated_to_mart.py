"""AWS Glue job: curated Parquet zone -> mart zone (full rebuild).

Builds the same order-grain fact table as the PostgreSQL project, writes it
partitioned by purchase_month, and records the KPI snapshot for reconciliation.

Job arguments:
    --BUCKET          lake bucket name
    --extra-py-files  s3://<bucket>/scripts/olist_transforms.py
"""
import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

import olist_transforms as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def curated(table):
    return spark.read.parquet(f"s3://{BUCKET}/curated/olist/{table}/")


fct = T.build_fct_order_delivery(curated("orders"), curated("customers"), curated("order_items"),
                                 curated("order_payments"), curated("order_reviews"))
(fct.repartition("purchase_month")          # one file per month instead of many small files
    .write.mode("overwrite").partitionBy("purchase_month")
    .parquet(f"s3://{BUCKET}/mart/olist/fct_order_delivery/"))

foi = T.build_fct_order_item(curated("order_items"), curated("products"), curated("category_translation"),
                             curated("sellers"), curated("orders"))
foi.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/mart/olist/fct_order_item/")

kpis = T.compute_kpis(spark.read.parquet(f"s3://{BUCKET}/mart/olist/fct_order_delivery/"))
comparison = {k: {"lake": kpis[k], "postgres": v, "match": abs(kpis[k] - v) <= 0.01}
              for k, v in T.POSTGRES_BASELINE.items()}
print(json.dumps(comparison, indent=2))

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
boto3.client("s3").put_object(
    Bucket=BUCKET, Key=f"logs/kpis/run={run_id}.json",
    Body=json.dumps({"kpis": kpis, "vs_postgres": comparison}, indent=2).encode("utf-8"))

job.commit()
# Olist E-Commerce Data Lake on AWS

Rebuilds a PostgreSQL order-fulfillment data mart as an Amazon S3 data lake with
**AWS Glue 5.0 (PySpark, Crawlers, Data Catalog, Workflows, Data Quality)** and
**Amazon Athena**, then proves the migration by reconciling every KPI against the
PostgreSQL baseline.

> Data: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
> (CC BY-NC-SA 4.0). The CSV files are not included in this repository.

## Results

| Check | Result |
|---|---|
| Rows landed in the raw zone (9 source files) | 1,550,922 |
| Rows in the curated zone (8 tables; geolocation stays raw) | 550,759 |
| Orders in the mart | 99,441 in 25 monthly partitions |
| KPIs reconciled with PostgreSQL | **17 of 17** |
| Athena migration and data-quality checks | **12 of 12 pass** |
| Incremental load (batch 2) | only the 4 order tables with a new `ingest_date` were processed |
| Re-run of batch 1 (idempotency) | same row counts, checks still 12 of 12 |
| Failure drill | 6 of 12 rows quarantined, batch rejected, job failed, alert sent |
| Athena data scanned for a monthly query | raw CSV 18,998,138 bytes → mart Parquet 724 bytes (99.996% less) |
| Glue job runtime | raw → curated 46–135 s, curated → mart 93–100 s (G.1X × 2 workers) |

## Architecture

```
 Local                                         AWS (us-east-2)
 Olist CSVs ── prepare_batches.py ──► batch1, batch2 (+ manifest: row counts, MD5)
                    │
                    └─ upload_raw.py (append-only) ──► s3://<bucket>/raw/olist/<table>/ingest_date=<date>/
                                                               │
                        raw crawler (custom CSV classifier) ───┤──► Data Catalog: olist_raw
                                                               ▼
                       Glue job  olist_raw_to_curated  (PySpark)
                       · finds ingest_date partitions present in raw but not in curated
                       · parses multi-line quoted CSV, casts types, blank → NULL
                       · AWS Glue Data Quality: 26 rules gate each batch
                       · 24 row-level rules: invalid rows → quarantine/
                       · primary-key dedupe → curated/ (Parquet, overwrite per ingest_date)
                                                               │
                        curated crawler ───────────────────────┤──► Data Catalog: olist_curated
                                                               ▼
                       Glue job  olist_curated_to_mart
                       · order-grain fact table, partitioned by purchase month
                       · writes a KPI snapshot and its comparison with PostgreSQL
                                                               ▼
                              s3://<bucket>/mart/olist/...  ◄── Athena (partition projection)
                                                                · views, 12 QA checks, KPI queries

 Orchestration: Glue Workflow (start trigger → job 1 + raw crawler → job 2 + curated crawler)
 Alerting:      EventBridge (Glue Job State Change = FAILED / TIMEOUT) → SNS email
 Security:      Glue role is read-only on the raw zone; S3 Block Public Access and SSE-S3
```

| Zone | S3 prefix | Format | Written by |
|---|---|---|---|
| Raw (append-only) | `raw/olist/<table>/ingest_date=<date>/` | CSV as delivered | `scripts/upload_raw.py` |
| Curated | `curated/olist/<table>/ingest_date=<date>/` | Parquet, typed | `olist_raw_to_curated` |
| Quarantine | `quarantine/olist/<table>/ingest_date=<date>/` | Parquet + `dq_errors` | `olist_raw_to_curated` |
| Mart | `mart/olist/fct_order_delivery/purchase_month=<yyyy-MM>/` | Parquet, one file per month | `olist_curated_to_mart` |

## How it works

**Incremental and idempotent loads.** Each upload lands under its own `ingest_date`
partition. With `--INGEST_DATE auto` the job lists the partitions in raw and curated
and processes only the difference. Passing a date reprocesses that partition and
overwrites it, so a re-run never duplicates rows and any day can be backfilled.

**Two layers of data quality.**

| Layer | Scope | Rules | When it fails |
|---|---|---|---|
| Row-level rules in PySpark ([glue/olist_transforms.py](glue/olist_transforms.py)) | each row | 24 | the row goes to `quarantine/` with the names of the rules it broke; the load continues |
| AWS Glue Data Quality, DQDL ([glue/job_raw_to_curated.py](glue/job_raw_to_curated.py)) | the whole batch, 99% thresholds | 26 | the batch is not written to curated and the job fails, which triggers the alert |

A few bad rows should not stop the pipeline; a broadly broken batch should.

**Testable transformations.** All transformation logic is plain PySpark in
`glue/olist_transforms.py` with no AWS imports, so it runs in unit tests on a laptop.
The two Glue scripts are thin wrappers for job arguments, S3 paths, Glue Data
Quality, and logging.

## Evidence from the runs

`olist_raw_to_curated`, in order:

| Run | What happened | Tables processed | Outcome | Seconds |
|---|---|---:|---|---:|
| 1 | Batch 1 (orders before 2018 + all dimensions) | 8 | loaded 325,463 rows | 135 |
| 2 | Re-run with nothing new | 0 | no-op | 48 |
| 3 | Batch 2 (2018 orders) through the workflow | 4 | loaded 225,296 rows | 103 |
| 4 | Batch 1 reprocessed with `--INGEST_DATE` | 8 | same 325,463 rows | 120 |
| 5 | Failure drill: 12 review rows, 6 invalid | 1 | **rejected, job failed** | 77 |
| 6 | After moving the bad file to `rejected/` | 0 | no-op, pipeline healthy | 46 |

Failure drill detail: `Completeness "review_id" >= 0.99` measured 0.83 and
`ColumnValues "review_score" <= 5` found the value 9. The 6 invalid rows were written to
`quarantine/` and no `ingest_date=2026-10-03` partition was written to curated.

### KPI reconciliation (computed by `olist_curated_to_mart`)

| KPI | PostgreSQL | Data lake |
|---|---:|---:|
| Orders | 99,441 | 99,441 |
| Orders with a timestamp-sequence issue | 1,388 | 1,388 |
| Eligible delivered orders | 95,097 | 95,097 |
| On-time orders / late orders | 88,587 / 6,510 | 88,587 / 6,510 |
| On-time rate | 93.15% | 93.15% |
| Average delay of late orders (days) | 10.62 | 10.62 |
| Delivery time P50 / P90 (days) | 10.27 / 23.16 | 10.27 / 23.16 |
| Approval / handoff / carrier average (hours) | 9.7 / 68.4 / 224.7 | 9.7 / 68.4 / 224.7 |
| Carrier time P90 (hours) | 454.7 | 454.7 |
| Cancellation rate | 0.63% | 0.63% |
| Freight per order | 22.90 | 22.90 |
| Average review score | 4.09 | 4.09 |

### Athena scan benchmark: raw CSV vs mart Parquet

Each query ran three times in workgroup `olist_wg`. Data scanned was identical on every
run; engine time is the median. Both zones return the same answers.

| Query | Zone and storage | Question | Data scanned | Engine time | Result |
|---|---|---|---:|---:|---:|
| A | raw: CSV | Delivered orders purchased in 2018-05 | 18,998,138 bytes | 954 ms | 6,749 |
| B | mart: Parquet, one monthly partition | Delivered orders purchased in 2018-05 | 724 bytes | 476 ms | 6,749 |
| C | mart: Parquet, all 25 partitions | Delivered orders, all months | 15,523 bytes | 563 ms | 96,478 |
| D | raw: CSV | Delivered orders, all months | 18,998,138 bytes | 729 ms | 96,478 |

| Comparison | Effect | Data scanned |
|---|---|---|
| A → B | Columnar storage and partition pruning together | 99.996% less |
| D → C | Columnar storage alone: only the `order_status` column is read | 99.92% less |
| C → B | Partition pruning alone: 1 of 25 monthly partitions is read | 95.3% less |

Athena bills a minimum of 10 MB per query, so at this data size the saving does not
change the cost; the ratio is what carries over to larger tables. These are `count(*)`
queries on one column, and a query that selects more columns scans more. The queries
are in [athena/06_scan_benchmark.sql](athena/06_scan_benchmark.sql).

## Data issues found and fixed

- **Multi-line CSV fields.** Review comments contain line breaks and doubled quotes.
  For the batch-1 review file a naive read returns 47,613 rows, `multiLine` alone
  returns 45,386 with shifted columns, and `multiLine` plus quote escaping returns
  45,370, which matches the manifest.
- **Leading zeros.** Schema inference turns ZIP code prefixes into integers and drops
  the leading zero on 23,995 customer rows. Every column is read as a string and cast
  explicitly; ZIP prefixes are left-padded to 5 digits.
- **Wrong header, right row count.** An early version of the batch script wrote the
  orders header onto three other files. Row counts still matched the manifest, but the
  crawler classified those tables as `UNKNOWN`. Row-count reconciliation does not catch
  schema errors.
- **Approximate percentiles.** Athena's `approx_percentile` gave P50 11.09 and P90 23.13
  days on batch 1 where the exact values are 11.05 and 22.98. The Glue job uses Spark's
  exact `percentile`, and [athena/05_exact_percentile.sql](athena/05_exact_percentile.sql)
  does the same in SQL with window functions.
- **A month with no orders.** November 2016 has no orders, so the mart has 25 partitions
  across 26 calendar months. Partition projection returns an empty result for it.
- **UTF-8 BOM** in the category translation file is stripped from column names.

## Repository layout

```
glue/       olist_transforms.py (shared PySpark logic), job_raw_to_curated.py, job_curated_to_mart.py
scripts/    prepare_batches.py, upload_raw.py, make_bad_batch.py (failure-drill data)
athena/     mart DDL with partition projection, views, 12 QA checks, KPI and percentile queries
iam/        Glue trust and S3 policies, EventBridge pattern and target, SNS topic policy, S3 lifecycle
tests/      unit tests for the transformations
```

## Run the tests locally

Requires Python 3, PySpark 3.5 or later, pytest, and Java 17.

```bash
python -m pytest -q tests        # 7 passed
```

To prepare the data, download the Olist CSVs from Kaggle, copy `env.sh.example` to
`env.sh`, set `OLIST_SRC`, then:

```bash
source env.sh
python scripts/prepare_batches.py --src "$OLIST_SRC" --out dataset
python scripts/upload_raw.py --batch-dir dataset/batch1 --bucket "$BUCKET" --ingest-date "$BATCH1_DATE" --dry-run
```

## AWS resources

| Resource | Name |
|---|---|
| Glue databases | `olist_raw`, `olist_curated`, `olist_mart` |
| Glue jobs (Glue 5.0, G.1X × 2, 15-minute timeout, no retries) | `olist_raw_to_curated`, `olist_curated_to_mart` |
| Crawlers and classifier | `olist_raw_crawler`, `olist_curated_crawler`, `olist_csv` |
| Workflow and triggers | `olist_pipeline`: start trigger with a daily cron schedule (kept inactive to avoid cost; runs were started on demand), then a conditional trigger on job 1 success |
| Athena workgroup | `olist_wg`, with a 1 GB per-query scan limit |
| Alerting | EventBridge rule `olist-glue-job-failed` → SNS topic `olist_glue_alerts` |
| IAM role | `GlueOlistRole`: `AWSGlueServiceRole` plus the inline policy in `iam/` |

Replace `<BUCKET>` in `athena/01_create_mart_tables.sql` and the `__BUCKET__` and
`__TOPIC_ARN__` placeholders in `iam/*.template.json` with your own values.

## Runbook: a batch is rejected by the Data Quality gate

1. An alert email arrives: `Glue job olist_raw_to_curated ended in state FAILED`.
2. Read `logs/raw_to_curated/run=<id>.json` for the failed rules and their measured values.
3. Inspect the rejected rows in `quarantine/olist/<table>/ingest_date=<date>/`.
4. Move the raw partition to `rejected/` and upload a corrected file under a new `ingest_date`.
5. Re-run the job and confirm `athena/03_qa_tests.sql` returns 12 passes.

## Known limitations and next steps

- The mart is rebuilt with a full overwrite, so a reader can see partial data during a
  rebuild. Apache Iceberg tables would give atomic writes and `MERGE`.
- Duplicates across different `ingest_date` batches are not reconciled. None exist in
  this dataset.

-- Compare the raw tables created by the crawler with the upload manifests.
-- order_reviews is expected to MISMATCH: the review text contains line breaks
-- inside quotes, which Athena's CSV SerDe does not support. The curated zone,
-- parsed by Spark with multiLine, is the one that matches the manifest.
SELECT 'orders' AS tbl, ingest_date, count(*) AS rows FROM olist_raw.orders GROUP BY ingest_date
UNION ALL
SELECT 'order_reviews', ingest_date, count(*) FROM olist_raw.order_reviews GROUP BY ingest_date
UNION ALL
SELECT 'curated.order_reviews', ingest_date, count(*) FROM olist_curated.order_reviews GROUP BY ingest_date
ORDER BY tbl, ingest_date;
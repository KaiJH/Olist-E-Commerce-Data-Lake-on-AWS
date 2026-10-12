-- Same question on two zones. After each query, record "Data scanned" from the
-- query details panel (or: aws athena get-query-execution --query-execution-id <id>).

-- A. raw zone: CSV, every column is a string, no useful partition
SELECT count(*) AS delivered_may_2018
FROM olist_raw.orders
WHERE substr(order_purchase_timestamp, 1, 7) = '2018-05'
  AND order_status = 'delivered';

-- B. mart zone: Parquet (columnar) + partition projection on purchase_month
SELECT count(*) AS delivered_may_2018
FROM olist_mart.fct_order_delivery
WHERE purchase_month = '2018-05'
  AND order_status = 'delivered';

-- C. mart zone, all partitions: shows the effect of columnar storage alone
SELECT count(*) AS delivered_all_months
FROM olist_mart.fct_order_delivery
WHERE order_status = 'delivered';

-- D. raw zone, all months: the raw-zone counterpart of C
SELECT count(*) AS delivered_all_months
FROM olist_raw.orders
WHERE order_status = 'delivered';

-- 10 data-quality tests ported from the PostgreSQL project (sql/qa_tests.sql)
-- plus 2 migration checks against the upload manifests. Every row should be PASS.
WITH tests AS (
  SELECT 'T01 fct_order_delivery PK unique' AS test, 0e0 AS expected,
         CAST((SELECT count(*) FROM (SELECT order_id FROM olist_mart.fct_order_delivery
                                     GROUP BY order_id HAVING count(*) > 1) d) AS double) AS actual
  UNION ALL
  SELECT 'T02 fact rows = curated orders', 0e0,
         CAST((SELECT count(*) FROM olist_mart.fct_order_delivery)
            - (SELECT count(*) FROM olist_curated.orders) AS double)
  UNION ALL
  SELECT 'T03 no orphan customer', 0e0,
         CAST((SELECT count(*) FROM olist_mart.fct_order_delivery f
               LEFT JOIN olist_curated.customers c ON f.customer_id = c.customer_id
               WHERE c.customer_id IS NULL) AS double)
  UNION ALL
  SELECT 'T04 no negative total_delivery_hours in clean', 0e0,
         CAST((SELECT count(*) FROM olist_mart.v_delivered_clean WHERE total_delivery_hours < 0) AS double)
  UNION ALL
  SELECT 'T05 is_late fully classified in clean', 0e0,
         CAST((SELECT count(*) FROM olist_mart.v_delivered_clean WHERE is_late IS NULL) AS double)
  UNION ALL
  SELECT 'T06 freight reconciliation', 0e0,
         round(abs(CAST((SELECT sum(freight_amount) FROM olist_mart.fct_order_delivery) AS double)
                 - CAST((SELECT sum(freight_value) FROM olist_curated.order_items) AS double)), 2)
  UNION ALL
  SELECT 'T07 item_count reconciliation', 0e0,
         CAST((SELECT sum(item_count) FROM olist_mart.fct_order_delivery)
            - (SELECT count(*) FROM olist_curated.order_items) AS double)
  UNION ALL
  SELECT 'T08 clean population has no quality issue', 0e0,
         CAST((SELECT count(*) FROM olist_mart.v_delivered_clean WHERE has_quality_issue) AS double)
  UNION ALL
  SELECT 'T09 fct_order_item PK unique', 0e0,
         CAST((SELECT count(*) FROM (SELECT order_id, order_item_id FROM olist_mart.fct_order_item
                                     GROUP BY order_id, order_item_id HAVING count(*) > 1) d) AS double)
  UNION ALL
  SELECT 'T10 on_time + late = eligible', 0e0,
         CAST((SELECT count_if(is_late) + count_if(is_late = false) - count(*)
               FROM olist_mart.v_delivered_clean) AS double)
  UNION ALL
  SELECT 'M01 curated orders = manifest (99,441)', 99441e0,
         CAST((SELECT count(*) FROM olist_curated.orders) AS double)
  UNION ALL
  SELECT 'M02 curated reviews = manifest (99,224)', 99224e0,
         CAST((SELECT count(*) FROM olist_curated.order_reviews) AS double)
)
SELECT test, expected, actual,
       CASE WHEN abs(actual - expected) <= 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
FROM tests
ORDER BY test;
-- Athena runs one statement at a time: run each CREATE VIEW separately.

-- Analysis population: delivered, no quality issue, has an estimated date
-- (same definition as mart.v_delivered_clean in PostgreSQL)
CREATE OR REPLACE VIEW olist_mart.v_delivered_clean AS
SELECT *
FROM olist_mart.fct_order_delivery
WHERE is_delivered
  AND coalesce(has_quality_issue, false) = false
  AND estimated_delivery_at IS NOT NULL;

-- Orders kept for review but excluded from KPIs
CREATE OR REPLACE VIEW olist_mart.v_quality_issues AS
SELECT order_id, order_status, purchased_at, approved_at, delivered_carrier_at,
       delivered_customer_at, estimated_delivery_at, total_delivery_hours, delivery_variance_days
FROM olist_mart.fct_order_delivery
WHERE has_quality_issue;

-- Monthly KPI layer for reporting
CREATE OR REPLACE VIEW olist_mart.v_kpi_monthly AS
SELECT purchase_month,
       count(*)                                                   AS delivered_orders,
       count_if(is_late = false)                                  AS on_time_orders,
       round(100.0 * count_if(is_late = false) / count(*), 2)     AS on_time_rate_pct,
       round(approx_percentile(total_delivery_hours, 0.9) / 24.0, 2) AS p90_delivery_days_approx
FROM olist_mart.v_delivered_clean
GROUP BY purchase_month;
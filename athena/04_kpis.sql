-- KPI snapshot to reconcile with PostgreSQL (docs/kpi_dictionary.md).
-- approx_percentile is Athena's built-in; see 05_exact_percentile.sql for the exact value.
SELECT
  count(*)                                                             AS eligible_delivered_orders,
  count_if(is_late = false)                                            AS on_time_orders,
  count_if(is_late)                                                    AS late_orders,
  round(100.0 * count_if(is_late = false) / count(*), 2)               AS on_time_rate_pct,
  round(100.0 * count_if(is_late) / count(*), 2)                       AS late_rate_pct,
  round(avg(CASE WHEN is_late THEN delivery_variance_days END), 2)     AS avg_delay_days,
  round(approx_percentile(total_delivery_hours, 0.5) / 24.0, 2)        AS p50_days_approx,
  round(approx_percentile(total_delivery_hours, 0.9) / 24.0, 2)        AS p90_days_approx,
  round(avg(approval_hours), 1)                                        AS approval_avg_h,
  round(avg(handoff_hours), 1)                                         AS handoff_avg_h,
  round(avg(carrier_to_customer_hours), 1)                             AS carrier_avg_h
FROM olist_mart.v_delivered_clean;

SELECT
  count(*)                                                                        AS orders,
  count_if(has_quality_issue)                                                     AS quality_issue_orders,
  round(100.0 * count_if(order_status = 'canceled') / count(*), 2)                AS cancel_rate_pct,
  round(CAST(sum(freight_amount) FILTER (WHERE freight_amount > 0) AS double)
        / count_if(freight_amount > 0), 2)                                        AS freight_per_order,
  round(avg(CAST(review_score_avg AS double)), 2)                                 AS avg_review_score
FROM olist_mart.fct_order_delivery;
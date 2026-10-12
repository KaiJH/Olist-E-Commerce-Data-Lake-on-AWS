-- Exact percentile (same method as PostgreSQL percentile_cont) with window functions.
-- position = 1 + q * (n - 1); interpolate between the two nearest ranked values.
WITH ranked AS (
  SELECT total_delivery_hours AS v,
         row_number() OVER (ORDER BY total_delivery_hours) AS rn,
         count(*) OVER ()                                  AS n
  FROM olist_mart.v_delivered_clean
),
targets AS (
  SELECT q, 1 + q * (max(n) - 1) AS pos
  FROM ranked CROSS JOIN (VALUES 0.5, 0.9) AS t(q)
  GROUP BY q
)
SELECT t.q                                                              AS percentile,
       round((lo.v + (t.pos - floor(t.pos)) * (hi.v - lo.v)) / 24.0, 2) AS delivery_days
FROM targets t
JOIN ranked lo ON lo.rn = CAST(floor(t.pos) AS bigint)
JOIN ranked hi ON hi.rn = CAST(ceil(t.pos)  AS bigint)
ORDER BY t.q;
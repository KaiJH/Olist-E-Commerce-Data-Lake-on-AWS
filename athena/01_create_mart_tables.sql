-- Run once in Athena (workgroup olist_wg). Replace <BUCKET> with your bucket name.
-- Partition projection: Athena computes the partitions from the rule below,
-- so no crawler or MSCK REPAIR is needed after each mart rebuild.

CREATE EXTERNAL TABLE IF NOT EXISTS olist_mart.fct_order_delivery (
  order_id                   string,
  customer_id                string,
  customer_state             string,
  customer_city              string,
  order_status               string,
  purchased_at               timestamp,
  approved_at                timestamp,
  delivered_carrier_at       timestamp,
  delivered_customer_at      timestamp,
  estimated_delivery_at      timestamp,
  item_count                 int,
  seller_count               int,
  product_count              int,
  product_amount             decimal(14,2),
  freight_amount             decimal(14,2),
  payment_amount             decimal(14,2),
  review_score_avg           decimal(4,2),
  purchase_date              date,
  approval_hours             double,
  handoff_hours              double,
  carrier_to_customer_hours  double,
  total_delivery_hours       double,
  delivery_variance_days     int,
  is_delivered               boolean,
  is_late                    boolean,
  severity_band              string,
  has_quality_issue          boolean
)
PARTITIONED BY (purchase_month string)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/mart/olist/fct_order_delivery/'
TBLPROPERTIES (
  'projection.enabled'                    = 'true',
  'projection.purchase_month.type'        = 'date',
  'projection.purchase_month.format'      = 'yyyy-MM',
  'projection.purchase_month.range'       = '2016-09,2018-12',
  'projection.purchase_month.interval'    = '1',
  'projection.purchase_month.interval.unit' = 'MONTHS',
  'storage.location.template'             = 's3://<BUCKET>/mart/olist/fct_order_delivery/purchase_month=${purchase_month}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS olist_mart.fct_order_item (
  order_id       string,
  order_item_id  int,
  product_id     string,
  seller_id      string,
  category_en    string,
  seller_state   string,
  price          decimal(12,2),
  freight_value  decimal(12,2),
  purchase_date  date
)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/mart/olist/fct_order_item/';
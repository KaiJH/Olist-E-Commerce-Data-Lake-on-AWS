from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CSV_OPTIONS = {
    "header": "true",
    "multiLine": "true",
    "quote": '"',
    "escape": '"',
    "encoding": "UTF-8"
}

VALID_ORDER_STATUS = [
    "approved", "canceled", "created", "delivered",
    "invoiced", "processing", "shipped", "unavailable"
]

PRIMARY_KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"]
}

def read_csv(spark, path):
    return spark.read.options(**CSV_OPTIONS).csv(path)

def strip_bom(df):
    return df.toDF(*[c.lstrip("\ufeff") for c in df.columns])

def _blank_to_null(c):
    s = F.trim(F.col(c).cast("string"))
    return F.when(s == "", None).otherwise(s)

def _ts(c):
    return F.to_timestamp(_blank_to_null(c), "yyyy-MM-dd HH:mm:ss")

def _unparsed_ts(c, parsed):
    return _blank_to_null(c).isNotNull() & parsed.isNull()

def _errors(*rules):
    return F.filter(
        F.array(*[F.when(cond, F.lit(msg)) for cond, msg in rules]),
        lambda x: x.isNotNull()
    )
    
def split_valid(df, rules):
    tagged = df.withColumn("dq_errors", _errors(*rules))
    valid = tagged.filter(F.size("dq_errors") == 0).drop("dq_errors")
    bad = tagged.filter(F.size("dq_errors") > 0)
    return valid, bad

# ---------------------------------------------------------------- raw -> curated
def clean_orders(raw):
    ts_cols = {
        "purchased_at": "order_purchase_timestamp",
        "approved_at": "order_approved_at",
        "delivered_carrier_at": "order_delivered_carrier_date",
        "delivered_customer_at": "order_delivered_customer_date",
        "estimated_delivery_at": "order_estimated_delivery_date",
    }
    df = raw.select(
        _blank_to_null("order_id").alias("order_id"),
        _blank_to_null("customer_id").alias("customer_id"),
        F.lower(_blank_to_null("order_status")).alias("order_status"),
        *[_ts(src).alias(dst) for dst, src in ts_cols.items()],
        *[_unparsed_ts(src, _ts(src)).alias(f"_bad_{dst}") for dst, src in ts_cols.items()],
    )
    rules = [
        (F.col("order_id").isNull(), "order_id_missing"),
        (F.col("customer_id").isNull(), "customer_id_missing"),
        (~F.col("order_status").isin(VALID_ORDER_STATUS) | F.col("order_status").isNull(), "invalid_order_status"),
        (F.col("purchased_at").isNull(), "purchase_timestamp_missing"),
        *[(F.col(f"_bad_{c}"), f"unparseable_{c}") for c in ts_cols],
    ]
    valid, bad = split_valid(df, rules)
    drop = [f"_bad_{c}" for c in ts_cols]
    valid = valid.drop(*drop).withColumn("purchase_month", F.date_format("purchased_at", "yyyy-MM"))
    return valid, bad.drop(*drop)


def clean_order_items(raw):
    df = raw.select(
        _blank_to_null("order_id").alias("order_id"),
        _blank_to_null("order_item_id").cast("int").alias("order_item_id"),
        _blank_to_null("product_id").alias("product_id"),
        _blank_to_null("seller_id").alias("seller_id"),
        _ts("shipping_limit_date").alias("shipping_limit_at"),
        _blank_to_null("price").cast("decimal(12,2)").alias("price"),
        _blank_to_null("freight_value").cast("decimal(12,2)").alias("freight_value"),
    )
    return split_valid(df, [
        (F.col("order_id").isNull(), "order_id_missing"),
        (F.col("order_item_id").isNull() | (F.col("order_item_id") < 1), "invalid_order_item_id"),
        (F.col("product_id").isNull() | F.col("seller_id").isNull(), "product_or_seller_missing"),
        (F.col("price").isNull() | (F.col("price") < 0), "invalid_price"),
        (F.col("freight_value") < 0, "negative_freight"),
    ])


def clean_order_payments(raw):
    df = raw.select(
        _blank_to_null("order_id").alias("order_id"),
        _blank_to_null("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(_blank_to_null("payment_type")).alias("payment_type"),
        _blank_to_null("payment_installments").cast("int").alias("payment_installments"),
        _blank_to_null("payment_value").cast("decimal(12,2)").alias("payment_value"),
    )
    return split_valid(df, [
        (F.col("order_id").isNull(), "order_id_missing"),
        (F.col("payment_sequential").isNull(), "payment_sequential_missing"),
        (F.col("payment_value").isNull() | (F.col("payment_value") < 0), "invalid_payment_value"),
    ])


def clean_order_reviews(raw):
    df = raw.select(
        _blank_to_null("review_id").alias("review_id"),
        _blank_to_null("order_id").alias("order_id"),
        _blank_to_null("review_score").cast("int").alias("review_score"),
        _blank_to_null("review_comment_title").alias("review_comment_title"),
        _blank_to_null("review_comment_message").alias("review_comment_message"),
        _ts("review_creation_date").alias("review_created_at"),
        _ts("review_answer_timestamp").alias("review_answered_at"),
    )
    return split_valid(df, [
        (F.col("review_id").isNull() | F.col("order_id").isNull(), "key_missing"),
        (~F.col("review_score").between(1, 5) | F.col("review_score").isNull(), "review_score_out_of_range"),
    ])


def clean_customers(raw):
    df = raw.select(
        _blank_to_null("customer_id").alias("customer_id"),
        _blank_to_null("customer_unique_id").alias("customer_unique_id"),
        F.lpad(_blank_to_null("customer_zip_code_prefix"), 5, "0").alias("customer_zip_code_prefix"),
        F.initcap(_blank_to_null("customer_city")).alias("customer_city"),
        F.upper(_blank_to_null("customer_state")).alias("customer_state"),
    )
    return split_valid(df, [
        (F.col("customer_id").isNull(), "customer_id_missing"),
        (F.length("customer_state") != 2, "invalid_state"),
    ])


def clean_sellers(raw):
    df = raw.select(
        _blank_to_null("seller_id").alias("seller_id"),
        F.lpad(_blank_to_null("seller_zip_code_prefix"), 5, "0").alias("seller_zip_code_prefix"),
        F.initcap(_blank_to_null("seller_city")).alias("seller_city"),
        F.upper(_blank_to_null("seller_state")).alias("seller_state"),
    )
    return split_valid(df, [(F.col("seller_id").isNull(), "seller_id_missing")])


def clean_products(raw):
    ints = ["product_name_lenght", "product_description_lenght", "product_photos_qty",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    df = raw.select(
        _blank_to_null("product_id").alias("product_id"),
        _blank_to_null("product_category_name").alias("product_category_name"),
        # keep the source's misspelled "lenght" columns but fix the names in curated
        *[_blank_to_null(c).cast("int").alias(c.replace("lenght", "length")) for c in ints],
    )
    return split_valid(df, [(F.col("product_id").isNull(), "product_id_missing")])


def clean_category_translation(raw):
    df = strip_bom(raw).select(
        _blank_to_null("product_category_name").alias("product_category_name"),
        _blank_to_null("product_category_name_english").alias("product_category_name_english"),
    )
    return split_valid(df, [(F.col("product_category_name").isNull(), "category_missing")])


CLEANERS = {
    "orders": clean_orders,
    "order_items": clean_order_items,
    "order_payments": clean_order_payments,
    "order_reviews": clean_order_reviews,
    "customers": clean_customers,
    "products": clean_products,
    "sellers": clean_sellers,
    "category_translation": clean_category_translation,
}


def dedupe(df, table):
    """Drop exact primary-key duplicates inside one batch; returns (df, removed)."""
    before = df.count()
    out = df.dropDuplicates(PRIMARY_KEYS[table])
    return out, before - out.count()


# ---------------------------------------------------------------- curated -> mart
def _hours(end, start):
    return (F.unix_timestamp(end) - F.unix_timestamp(start)) / 3600.0


def build_fct_order_delivery(orders, customers, order_items, order_payments, order_reviews):
    items = order_items.groupBy("order_id").agg(
        F.count(F.lit(1)).alias("item_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.countDistinct("product_id").alias("product_count"),
        F.sum("price").alias("product_amount"),
        F.sum("freight_value").alias("freight_amount"),
    )
    pays = order_payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_amount"),
        F.count(F.lit(1)).alias("payment_count"),
        F.max("payment_installments").alias("max_installments"),
    )
    reviews = order_reviews.groupBy("order_id").agg(
        F.round(F.avg("review_score"), 2).cast("decimal(4,2)").alias("review_score_avg"),
        F.count(F.lit(1)).alias("review_count"),
    )
    cust = customers.select("customer_id", "customer_state", "customer_city")

    f = (orders.drop("ingest_date")
         .join(cust, "customer_id", "left")
         .join(items, "order_id", "left")
         .join(pays, "order_id", "left")
         .join(reviews, "order_id", "left"))

    f = f.select(
        "order_id", "customer_id", "customer_state", "customer_city", "order_status",
        "purchased_at", "approved_at", "delivered_carrier_at", "delivered_customer_at", "estimated_delivery_at",
        F.coalesce("item_count", F.lit(0)).cast("int").alias("item_count"),
        F.coalesce("seller_count", F.lit(0)).cast("int").alias("seller_count"),
        F.coalesce("product_count", F.lit(0)).cast("int").alias("product_count"),
        F.coalesce("product_amount", F.lit(0)).cast("decimal(14,2)").alias("product_amount"),
        F.coalesce("freight_amount", F.lit(0)).cast("decimal(14,2)").alias("freight_amount"),
        F.col("payment_amount").cast("decimal(14,2)").alias("payment_amount"),
        "review_score_avg",
        F.to_date("purchased_at").alias("purchase_date"),
        "purchase_month",
    )

    f = (f
         .withColumn("approval_hours", _hours("approved_at", "purchased_at"))
         .withColumn("handoff_hours", _hours("delivered_carrier_at", "approved_at"))
         .withColumn("carrier_to_customer_hours", _hours("delivered_customer_at", "delivered_carrier_at"))
         .withColumn("total_delivery_hours", _hours("delivered_customer_at", "purchased_at"))
         .withColumn("delivery_variance_days",
                     F.datediff(F.to_date("delivered_customer_at"), F.to_date("estimated_delivery_at")))
         .withColumn("is_delivered",
                     (F.col("order_status") == "delivered") & F.col("delivered_customer_at").isNotNull()))

    f = f.withColumn(
        "is_late",
        F.when(F.col("is_delivered") & F.col("estimated_delivery_at").isNotNull(),
               F.to_date("delivered_customer_at") > F.to_date("estimated_delivery_at")),
    )
    f = f.withColumn(
        "severity_band",
        F.when(~F.coalesce(F.col("is_late"), F.lit(False)), None)
         .when(F.col("delivery_variance_days").between(1, 2), "1-2 days")
         .when(F.col("delivery_variance_days").between(3, 7), "3-7 days")
         .when(F.col("delivery_variance_days") >= 8, "8+ days"),
    )
    # Same three-valued logic as the PostgreSQL UPDATE: the result can be NULL.
    f = f.withColumn(
        "has_quality_issue",
        (F.col("approved_at").isNotNull() & (F.col("approved_at") < F.col("purchased_at")))
        | (F.col("delivered_carrier_at").isNotNull() & F.col("approved_at").isNotNull()
           & (F.col("delivered_carrier_at") < F.col("approved_at")))
        | (F.col("delivered_customer_at").isNotNull() & F.col("delivered_carrier_at").isNotNull()
           & (F.col("delivered_customer_at") < F.col("delivered_carrier_at")))
        | (F.col("total_delivery_hours") < 0)
        | ((F.col("order_status") != "delivered") & F.col("delivered_customer_at").isNotNull()),
    )
    return f


def build_fct_order_item(order_items, products, category_translation, sellers, orders):
    prod = (products.drop("ingest_date")
            .join(category_translation.drop("ingest_date"), "product_category_name", "left")
            .select("product_id",
                    F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown"))
                    .alias("category_en")))
    return (order_items.drop("ingest_date")
            .join(prod, "product_id", "left")
            .join(sellers.select("seller_id", "seller_state"), "seller_id", "left")
            .join(orders.select("order_id", F.to_date("purchased_at").alias("purchase_date")), "order_id", "left")
            .select("order_id", "order_item_id", "product_id", "seller_id", "category_en",
                    "seller_state", "price", "freight_value", "purchase_date"))


def delivered_clean(fct):
    """Equivalent of mart.v_delivered_clean."""
    return fct.filter(
        F.col("is_delivered")
        & ~F.coalesce(F.col("has_quality_issue"), F.lit(False))
        & F.col("estimated_delivery_at").isNotNull()
    )


def compute_kpis(fct):
    """The 10 KPIs from sql/05_kpi_benchmarks.sql, returned as a dict."""
    clean = delivered_clean(fct)
    c = clean.agg(
        F.count(F.lit(1)).alias("eligible"),
        F.sum(F.when(F.col("is_late") == False, 1).otherwise(0)).alias("on_time"),  # noqa: E712
        F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late"),
        F.expr("percentile(total_delivery_hours, 0.5)").alias("p50_h"),
        F.expr("percentile(total_delivery_hours, 0.9)").alias("p90_h"),
        F.avg("approval_hours").alias("approval_avg_h"),
        F.avg("handoff_hours").alias("handoff_avg_h"),
        F.avg("carrier_to_customer_hours").alias("carrier_avg_h"),
        F.expr("percentile(carrier_to_customer_hours, 0.9)").alias("carrier_p90_h"),
    ).first()
    avg_delay = clean.filter(F.col("is_late")).agg(F.avg("delivery_variance_days")).first()[0]
    all_orders = fct.agg(
        F.count(F.lit(1)).alias("orders"),
        F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled"),
        F.sum(F.when(F.col("has_quality_issue"), 1).otherwise(0)).alias("quality_issues"),
    ).first()
    freight = fct.filter(F.col("freight_amount") > 0).agg(
        (F.sum("freight_amount") / F.count(F.lit(1))).alias("v")).first()["v"]
    review = fct.filter(F.col("review_score_avg").isNotNull()).agg(F.avg("review_score_avg")).first()[0]
    return {
        "orders": all_orders["orders"],
        "quality_issue_orders": all_orders["quality_issues"],
        "eligible_delivered_orders": c["eligible"],
        "on_time_orders": c["on_time"],
        "late_orders": c["late"],
        "on_time_rate_pct": round(100.0 * c["on_time"] / c["eligible"], 2),
        "late_rate_pct": round(100.0 * c["late"] / c["eligible"], 2),
        "avg_delay_days": round(float(avg_delay), 2),
        "p50_days": round(c["p50_h"] / 24.0, 2),
        "p90_days": round(c["p90_h"] / 24.0, 2),
        "approval_avg_h": round(c["approval_avg_h"], 1),
        "handoff_avg_h": round(c["handoff_avg_h"], 1),
        "carrier_avg_h": round(c["carrier_avg_h"], 1),
        "carrier_p90_h": round(c["carrier_p90_h"], 1),
        "cancel_rate_pct": round(100.0 * all_orders["canceled"] / all_orders["orders"], 2),
        "freight_per_order": round(float(freight), 2),
        "avg_review_score": round(float(review), 2),
    }


# PostgreSQL results from docs/kpi_dictionary.md (the migration baseline)
POSTGRES_BASELINE = {
    "orders": 99441,
    "quality_issue_orders": 1388,
    "eligible_delivered_orders": 95097,
    "on_time_orders": 88587,
    "late_orders": 6510,
    "on_time_rate_pct": 93.15,
    "late_rate_pct": 6.85,
    "avg_delay_days": 10.62,
    "p50_days": 10.27,
    "p90_days": 23.16,
    "approval_avg_h": 9.7,
    "handoff_avg_h": 68.4,
    "carrier_avg_h": 224.7,
    "carrier_p90_h": 454.7,
    "cancel_rate_pct": 0.63,
    "freight_per_order": 22.90,
    "avg_review_score": 4.09,
}


def union_all(dfs):
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
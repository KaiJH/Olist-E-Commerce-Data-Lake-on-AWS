"""Unit tests for glue/olist_transforms.py (plain PySpark, no AWS).

Run:  JAVA_HOME=<JDK 17> python -m pytest -q tests
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glue"))
import olist_transforms as T  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


ORDER_COLS = ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
              "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]


def test_multiline_review_is_one_row(spark, tmp_path):
    p = tmp_path / "reviews.csv"
    p.write_text('"review_id","order_id","review_comment_message"\n'
                  '"r1","o1","line one\nline two with ""quotes"""\n', encoding="utf-8")
    df = T.read_csv(spark, str(p))
    assert df.count() == 1
    assert df.first()["review_comment_message"] == 'line one\nline two with "quotes"'


def test_bom_is_stripped(spark, tmp_path):
    p = tmp_path / "t.csv"
    p.write_bytes("﻿product_category_name,product_category_name_english\nbeleza_saude,health_beauty\n"
                  .encode("utf-8"))
    valid, bad = T.clean_category_translation(T.read_csv(spark, str(p)))
    assert valid.columns == ["product_category_name", "product_category_name_english"]
    assert valid.count() == 1 and bad.count() == 0


def test_orders_blank_timestamp_becomes_null_and_bad_rows_quarantined(spark):
    raw = spark.createDataFrame([
        ("o1", "c1", "Delivered ", "2017-10-02 10:56:33", "", "", "", "2017-10-18 00:00:00"),
        ("o2", "c2", "teleported", "2017-10-02 10:56:33", "", "", "", ""),
        ("o3", "c3", "shipped", "not-a-date", "", "", "", ""),
    ], ORDER_COLS)
    valid, bad = T.clean_orders(raw)
    row = valid.first()
    assert valid.count() == 1
    assert row["order_status"] == "delivered" and row["approved_at"] is None
    assert row["purchase_month"] == "2017-10"
    errors = {r["order_id"]: r["dq_errors"] for r in bad.collect()}
    assert errors["o2"] == ["invalid_order_status"]
    assert "purchase_timestamp_missing" in errors["o3"]


def test_review_score_out_of_range_is_quarantined(spark):
    cols = ", ".join(f"{c} string" for c in ["review_id", "order_id", "review_score", "review_comment_title",
                                               "review_comment_message", "review_creation_date",
                                               "review_answer_timestamp"])
    raw = spark.createDataFrame([("r1", "o1", "5", None, None, "", ""),
                                 ("r2", "o2", "9", None, None, "", "")], cols)
    valid, bad = T.clean_order_reviews(raw)
    assert valid.count() == 1
    assert bad.first()["dq_errors"] == ["review_score_out_of_range"]


def test_zip_prefix_keeps_leading_zero(spark):
    raw = spark.createDataFrame([("c1", "u1", "1037", "sao paulo", "sp")],
                                ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                                 "customer_city", "customer_state"])
    row = T.clean_customers(raw)[0].first()
    assert row["customer_zip_code_prefix"] == "01037"
    assert row["customer_city"] == "Sao Paulo" and row["customer_state"] == "SP"


def test_dedupe_on_primary_key(spark):
    df = spark.createDataFrame([("o1", 1), ("o1", 1), ("o1", 2)], ["order_id", "order_item_id"])
    out, removed = T.dedupe(df, "order_items")
    assert out.count() == 2 and removed == 1


def test_fact_is_order_grain_and_flags_late_and_quality(spark):
    orders, _ = T.clean_orders(spark.createDataFrame([
        # on time
        ("o1", "c1", "delivered", "2017-01-01 10:00:00", "2017-01-01 12:00:00", "2017-01-02 10:00:00",
         "2017-01-05 10:00:00", "2017-01-10 00:00:00"),
        # late by 3 days
        ("o2", "c1", "delivered", "2017-01-01 10:00:00", "2017-01-01 12:00:00", "2017-01-02 10:00:00",
         "2017-01-13 10:00:00", "2017-01-10 00:00:00"),
        # quality issue: delivered to customer before carrier pickup
        ("o3", "c1", "delivered", "2017-01-01 10:00:00", "2017-01-01 12:00:00", "2017-01-05 10:00:00",
         "2017-01-04 10:00:00", "2017-01-10 00:00:00"),
    ], ORDER_COLS))
    customers, _ = T.clean_customers(spark.createDataFrame(
        [("c1", "u1", "01037", "x", "SP")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]))
    items = spark.createDataFrame(
        [("o1", 1, "p1", "s1", 10.0, 2.0), ("o1", 2, "p2", "s1", 5.0, 1.0), ("o2", 1, "p1", "s2", 7.0, 3.0)],
        "order_id string, order_item_id int, product_id string, seller_id string, price double, freight_value double")
    pays = spark.createDataFrame([("o1", 1, 1, 18.0), ("o2", 1, 1, 10.0)],
                                 "order_id string, payment_sequential int, payment_installments int, "
                                 "payment_value double")
    reviews = spark.createDataFrame([("r1", "o1", 5)], "review_id string, order_id string, review_score int")

    fct = T.build_fct_order_delivery(orders, customers, items, pays, reviews)
    rows = {r["order_id"]: r for r in fct.collect()}
    assert fct.count() == 3                                  # one row per order, no fan-out
    assert rows["o1"]["item_count"] == 2 and float(rows["o1"]["freight_amount"]) == 3.0
    assert rows["o1"]["is_late"] is False
    assert rows["o2"]["is_late"] is True and rows["o2"]["severity_band"] == "3-7 days"
    assert rows["o3"]["has_quality_issue"] is True
    assert T.delivered_clean(fct).count() == 2
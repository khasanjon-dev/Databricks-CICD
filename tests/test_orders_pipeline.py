from datetime import date

import pytest
from pyspark.sql import SparkSession

from src.orders_pipeline import clean_orders, calculate_daily_revenue


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("orders-pipeline-tests").getOrCreate()
    yield spark
    spark.stop()


def test_clean_orders(spark):
    data = [
        (1, 101, "2026-11-01", 100.0),
        (2, 102, "2026-11-01", 200.0),
        (None, 103, "2026-11-01", 300.0),
        (3, 104, "2026-11-01", -50.0),
    ]

    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_date", "amount"]
    )
    result = clean_orders(df)

    assert result.count() == 2


def test_calculate_daily_revenue(spark):
    data = [
        (1, 101, date(2026, 11, 1), 100.0),
        (2, 102, date(2026, 11, 1), 200.0),
        (3, 103, date(2026, 11, 2), 150.0),
    ]

    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_date", "amount"],
    )

    result = calculate_daily_revenue(df)

    revenues = {
        row["order_date"]: row["daily_revenue"]
        for row in result.collect()
    }

    assert revenues[date(2026, 11, 1)] == 300.0
    assert revenues[date(2026, 11, 2)] == 150.0

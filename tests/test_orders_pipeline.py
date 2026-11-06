from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType

from transformations import (
    clean_orders,
    calculate_daily_revenue,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("orders-pipeline-tests")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def orders_df(spark):
    data = [
        (1, 101, "2026-11-01", "100.0"),
        (2, 102, "2026-11-01", "200.0"),
        (3, 103, "2026-11-02", "150.0"),
        (4, 104, "2026-11-02", "-50.0"),
        (5, None, "2026-11-03", "80.0"),
        (6, 106, "invalid-date", "40.0"),
        (7, 107, "2026-11-03", "invalid"),
    ]

    return spark.createDataFrame(
        data,
        [
            "order_id",
            "customer_id",
            "order_date",
            "amount",
        ],
    )


def test_clean_orders_removes_invalid_records(orders_df):
    result = clean_orders(orders_df)

    order_ids = sorted(
        row.order_id for row in result.collect()
    )

    assert order_ids == [1, 2, 3]


def test_clean_orders_converts_types(orders_df):
    result = clean_orders(orders_df)

    assert isinstance(
        result.schema["order_date"].dataType,
        DateType,
    )

    assert isinstance(
        result.schema["amount"].dataType,
        DoubleType,
    )


def test_calculate_daily_revenue(orders_df):
    cleaned_orders = clean_orders(orders_df)

    result = calculate_daily_revenue(cleaned_orders)

    revenue_by_date = {
        row.order_date: row.daily_revenue
        for row in result.collect()
    }

    assert revenue_by_date == {
        date(2026, 11, 1): 300.0,
        date(2026, 11, 2): 150.0,
    }

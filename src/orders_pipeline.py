from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, sum


def clean_orders(orders_df: DataFrame) -> DataFrame:
    """
    Remove invalid orders and select required columns.
    """
    return (
        orders_df
        .filter(col("order_id").isNotNull())
        .filter(col("amount") > 0)
        .withColumn("order_date", to_date(col("order_date")))
        .select("order_id", "customer_id", "order_date", "amount")
    )


def calculate_daily_revenue(orders_df: DataFrame) -> DataFrame:
    """Calculate total revenue per day."""
    return (
        orders_df
        .groupBy("order_date")
        .agg(sum("amount").alias("daily_revenue"))
    )

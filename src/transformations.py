from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_orders(orders_df: DataFrame) -> DataFrame:
    """
    Clean orders:
    - Convert order_date to DateType
    - Convert amount to DoubleType
    - Remove invalid records
    """

    cleaned_df = (
        orders_df
        .withColumn(
            "order_date",
            F.to_date("order_date"),
        )
        .withColumn(
            "amount",
            F.col("amount").cast("double"),
        )
    )

    return cleaned_df.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_date").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
    )


def calculate_daily_revenue(
        orders_df: DataFrame,
) -> DataFrame:
    """
    Calculate revenue per day.
    """

    return (
        orders_df
        .groupBy("order_date")
        .agg(
            F.sum("amount").alias("daily_revenue")
        )
        .orderBy("order_date")
    )

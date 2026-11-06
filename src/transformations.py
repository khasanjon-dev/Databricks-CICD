from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "order_date",
            F.expr("try_cast(order_date AS DATE)")
        )
        .withColumn(
            "amount",
            F.expr("try_cast(amount AS DOUBLE)")
        )
        .filter(
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("order_date").isNotNull()
            & F.col("amount").isNotNull()
            & (F.col("amount") > 0)
        )
    )


def calculate_daily_revenue(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("order_date")
        .agg(F.sum("amount").alias("daily_revenue"))
        .orderBy("order_date")
    )

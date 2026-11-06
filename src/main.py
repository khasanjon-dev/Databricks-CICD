from pyspark.sql import SparkSession

from transformations import (
    clean_orders,
    calculate_daily_revenue,
)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    orders_data = [
        (1, 101, "2026-11-01", "100.0"),
        (2, 102, "2026-11-01", "200.0"),
        (3, 103, "2026-11-02", "150.0"),
        (4, 104, "2026-11-02", "-50.0"),
        (5, 105, "2026-11-03", "invalid"),
    ]

    orders_df = spark.createDataFrame(
        orders_data,
        [
            "order_id",
            "customer_id",
            "order_date",
            "amount",
        ],
    )

    cleaned_orders = clean_orders(orders_df)

    daily_revenue = calculate_daily_revenue(
        cleaned_orders
    )

    daily_revenue.show(truncate=False)


if __name__ == "__main__":
    main()

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(catalog: str = "dev"):
    spark = SparkSession.builder.getOrCreate()

    silver_table = f"{catalog}.silver.workouts"
    gold_table = f"{catalog}.gold.user_workout_summary"

    df = spark.read.table(silver_table)

    gold_df = (
        df.groupBy("user_id")
        .agg(
            F.count("workout_id").alias("total_workouts"),
            F.sum("duration_minutes").alias("total_duration_minutes"),
            F.sum("calories").alias("total_calories"),
            F.round(F.avg("calories"), 2).alias("avg_calories_per_workout")
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )

    print(f"Gold aggregation completed: {gold_table}")


if __name__ == "__main__":
    catalog = sys.argv[1] if len(sys.argv) > 1 else "dev"
    main(catalog)

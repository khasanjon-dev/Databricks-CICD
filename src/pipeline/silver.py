import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main(catalog="dev"):
    spark = SparkSession.builder.getOrCreate()

    bronze_table = f"{catalog}.bronze.workouts"
    silver_table = f"{catalog}.silver.workouts"

    df = spark.read.table(bronze_table)

    # Remove duplicate workout IDs
    df = df.dropDuplicates(["workout_id"])

    # Filter invalid records
    df = df.filter(
        (col("duration_minutes") > 0)
        & (col("calories") > 0)
        & col("workout_type").isin(
            "running", "cycling", "swimming"
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )

    print(f"Silver transformation completed: {silver_table}")


if __name__ == "__main__":
    catalog = sys.argv[1] if len(sys.argv) > 1 else "dev"
    main(catalog)

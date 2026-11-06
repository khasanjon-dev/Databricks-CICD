from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date


def get_workout_records():
    """Return mock source workout records."""
    return [
        (1, 101, "running", 30, 250, "2026-11-01"),
        (2, 102, "cycling", 45, 400, "2026-11-02"),
        (3, 101, "swimming", 25, 200, "2026-11-03"),
    ]


def main(catalog="dev"):
    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(
        get_workout_records(),
        [
            "workout_id",
            "user_id",
            "workout_type",
            "duration_minutes",
            "calories",
            "workout_date"
        ]
    )

    df = df.withColumn("workout_date", to_date("workout_date"))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.bronze.workouts")
    )

    print(f"Bronze ingestion completed: {catalog}.bronze.workouts")


if __name__ == "__main__":
    catalog = "dev"
    main(catalog)

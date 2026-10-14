import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


PROJECT = "labs-507309"
STREAM_TABLE = f"{PROJECT}.raw_ds.rides_stream"
HISTORY_TABLE = f"{PROJECT}.raw_ds.nyc_taxi_2024_01_clean"
DRIVER_FILE = "gs://arshi-spark-lab-2026/reference/dim_driver.csv"
OUTPUT_PATH = "gs://arshi-spark-lab-2026/curated/rides/"


def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else OUTPUT_PATH
    zone_file = sys.argv[2] if len(sys.argv) > 2 else None

    spark = (
        SparkSession.builder
        .appName("ride-hailing-curated-processing")
        .getOrCreate()
    )

    # 1. Read streaming rides from BigQuery.
    stream = (
        spark.read.format("bigquery")
        .option("table", STREAM_TABLE)
        .load()
    )

    # Use ingest_ts if the source schema has it.
    if "ingest_ts" not in stream.columns:
        stream = stream.withColumn(
            "ingest_ts", F.lit(None).cast("timestamp")
        )

    stream = stream.select(
        F.col("ride_id").cast("string"),
        F.col("driver_id").cast("string"),
        F.col("rider_id").cast("string"),
        F.col("pickup_zone").cast("string"),
        F.col("dropoff_zone").cast("string"),
        F.col("pickup_ts").cast("timestamp"),
        F.col("dropoff_ts").cast("timestamp"),
        F.col("fare").cast("double"),
        F.lit(None).cast("double").alias("trip_distance"),
        F.col("surge_multiplier").cast("double"),
        F.col("ingest_ts").cast("timestamp"),
    ).withColumn("source", F.lit("stream"))

    # 2. Read and normalize historical NYC Taxi rides.
    history = (
        spark.read.format("bigquery")
        .option("table", HISTORY_TABLE)
        .load()
    )

    # Generate a repeatable historical ride ID.
    history = history.withColumn(
        "ride_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("pickup_datetime").cast("string"),
                F.col("dropoff_datetime").cast("string"),
                F.col("pickup_location_id").cast("string"),
                F.col("dropoff_location_id").cast("string"),
                F.col("passenger_count").cast("string"),
                F.col("trip_distance").cast("string"),
                F.col("fare_amount").cast("string"),
            ),
            256,
        ),
    )

    history = history.select(
        F.col("ride_id"),
        F.lit(None).cast("string").alias("driver_id"),
        F.lit(None).cast("string").alias("rider_id"),
        F.col("pickup_location_id").cast("string").alias("pickup_zone"),
        F.col("dropoff_location_id").cast("string").alias("dropoff_zone"),
        F.col("pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("fare_amount").cast("double").alias("fare"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.lit(None).cast("double").alias("surge_multiplier"),
        F.lit(None).cast("timestamp").alias("ingest_ts"),
    ).withColumn("source", F.lit("history"))

    # 3. Combine both sources.
    rides = stream.unionByName(history)

    # 4. Clean invalid records.
    rides = rides.withColumn(
        "trip_minutes",
        (
            F.col("dropoff_ts").cast("long")
            - F.col("pickup_ts").cast("long")
        ) / 60.0,
    )

    rides = rides.filter(
        F.col("ride_id").isNotNull()
        & F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & F.col("fare").isNotNull()
        & (F.col("fare") >= 0)
        & (F.col("trip_minutes") >= 0)
        & (F.col("trip_minutes") <= 240)
    )

    # 5. Deduplicate.
    # Until ingest_ts is populated upstream, event timestamps are a
    # fallback; they do not guarantee latest-ingestion ordering.
    dedup_window = (
        Window.partitionBy("ride_id")
        .orderBy(
            F.coalesce(
                F.col("ingest_ts"), F.col("dropoff_ts")
            ).desc(),
            F.col("pickup_ts").desc(),
            F.col("source").asc(),
        )
    )

    rides = (
        rides.withColumn(
            "_row_number", F.row_number().over(dedup_window)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # 6. Enrich with the driver dimension.
    drivers = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(DRIVER_FILE)
        .select(
            F.col("driver_id").cast("string"),
            "driver_name",
            "vehicle_type",
            F.col("rating").cast("double").alias("driver_rating"),
        )
        .dropDuplicates(["driver_id"])
    )

    rides = rides.join(drivers, on="driver_id", how="left")

    # 7. Optionally enrich pickup/drop-off zones.
    # Expected CSV columns: zone_id, zone_name.
    if zone_file:
        zones = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(zone_file)
            .select(
                F.col("zone_id").cast("string"),
                F.col("zone_name").cast("string"),
            )
            .dropDuplicates(["zone_id"])
        )

        pickup_zones = zones.select(
            F.col("zone_id").alias("_pickup_zone_id"),
            F.col("zone_name").alias("pickup_zone_name"),
        )

        dropoff_zones = zones.select(
            F.col("zone_id").alias("_dropoff_zone_id"),
            F.col("zone_name").alias("dropoff_zone_name"),
        )

        rides = (
            rides.join(
                pickup_zones,
                rides.pickup_zone == pickup_zones._pickup_zone_id,
                "left",
            )
            .drop("_pickup_zone_id")
            .join(
                dropoff_zones,
                rides.dropoff_zone == dropoff_zones._dropoff_zone_id,
                "left",
            )
            .drop("_dropoff_zone_id")
        )

    # 8. Derive business metrics.
    rides = (
        rides.withColumn(
            "revenue_per_km",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare") / F.col("trip_distance"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "surge_flag",
            F.when(
                F.col("surge_multiplier").isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(F.col("surge_multiplier") > 1.0),
        )
        .withColumn("weekday", F.date_format("pickup_ts", "EEEE"))
        .withColumn("hour_bucket", F.hour("pickup_ts"))
        .withColumn("dt", F.to_date("pickup_ts"))
    )

    # 9. Write curated Parquet, partitioned by pickup date.
    (
        rides.write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )

    print(f"Curated Parquet written to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()

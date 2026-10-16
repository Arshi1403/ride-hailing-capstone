"""Dataproc Serverless job: raw (stream + history) -> curated Parquet, plus the driver snapshot for SCD2."""
import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

STREAM_SCHEMA = (
    "ride_id STRING, driver_id STRING, rider_id STRING, pickup_zone STRING, "
    "dropoff_zone STRING, pickup_ts STRING, dropoff_ts STRING, fare DOUBLE, "
    "surge_multiplier DOUBLE"
)
MAX_TRIP_MINUTES = 240
KM_PER_MILE = 1.609344


def read_stream(spark: SparkSession, table: str) -> DataFrame:
    """Parse the Pub/Sub BigQuery-subscription landing table; publish_time is the ingest time."""
    raw = spark.read.format("bigquery").option("table", table).load()
    return (
        raw.select(
            F.from_json("data", STREAM_SCHEMA).alias("e"),
            F.col("publish_time").alias("ingest_ts"),
        )
        .select("e.*", "ingest_ts")
        .withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts", F.to_timestamp("dropoff_ts"))
        .withColumn("trip_distance", F.lit(None).cast("double"))
        .withColumn("source", F.lit("stream"))
    )


def read_history(spark: SparkSession, table: str) -> DataFrame:
    raw = spark.read.format("bigquery").option("table", table).load()
    ride_id = F.sha2(
        F.concat_ws(
            "|",
            *[
                F.col(c).cast("string")
                for c in (
                    "pickup_datetime", "dropoff_datetime", "pickup_location_id",
                    "dropoff_location_id", "trip_distance", "fare_amount",
                )
            ],
        ),
        256,
    )
    return raw.select(
        ride_id.alias("ride_id"),
        F.lit(None).cast("string").alias("driver_id"),
        F.lit(None).cast("string").alias("rider_id"),
        F.col("pickup_location_id").cast("string").alias("pickup_zone"),
        F.col("dropoff_location_id").cast("string").alias("dropoff_zone"),
        F.col("pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("fare_amount").cast("double").alias("fare"),
        F.lit(None).cast("double").alias("surge_multiplier"),
        F.lit(None).cast("timestamp").alias("ingest_ts"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.lit("history").alias("source"),
    )


def clean(df: DataFrame) -> DataFrame:
    """Drop null timestamps, negative fares and trips longer than 4 hours."""
    df = df.withColumn(
        "trip_minutes",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
    )
    return df.filter(
        F.col("ride_id").isNotNull()
        & F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("fare") >= 0)
        & (F.col("trip_minutes") >= 0)
        & (F.col("trip_minutes") <= MAX_TRIP_MINUTES)
    )


def dedupe(df: DataFrame) -> DataFrame:
    """Keep the row with the latest ingest_ts per ride_id."""
    w = Window.partitionBy("ride_id").orderBy(F.col("ingest_ts").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich(df: DataFrame, drivers: DataFrame, zones: DataFrame) -> DataFrame:
    pickup = zones.select(
        F.col("zone_id").alias("pickup_zone"), F.col("zone_name").alias("pickup_zone_name")
    )
    dropoff = zones.select(
        F.col("zone_id").alias("dropoff_zone"), F.col("zone_name").alias("dropoff_zone_name")
    )
    return (
        df.join(drivers.select("driver_id", "vehicle_type"), "driver_id", "left")
        .join(pickup, "pickup_zone", "left")
        .join(dropoff, "dropoff_zone", "left")
    )


def derive(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "revenue_per_km",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare") / (F.col("trip_distance") * KM_PER_MILE),
            ),
        )
        .withColumn("surge_flag", F.coalesce(F.col("surge_multiplier"), F.lit(1.0)) > 1.0)
        .withColumn("weekday", F.date_format("pickup_ts", "EEEE"))
        .withColumn("hour_bucket", F.hour("pickup_ts"))
        .withColumn("dt", F.to_date("pickup_ts"))
    )


def prepare_driver_snapshot(df: DataFrame, dt: str) -> DataFrame:
    """Standardise the daily snapshot; attr_hash covers the SCD2-tracked attributes."""
    return (
        df.select(
            F.col("driver_id").cast("string"),
            "driver_name",
            "address",
            "vehicle_type",
            F.col("rating").cast("double"),
        )
        .dropDuplicates(["driver_id"])
        .withColumn("attr_hash", F.sha2(F.concat_ws("|", "address", "vehicle_type"), 256))
        .withColumn("dt", F.to_date(F.lit(dt)))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dt", required=True, help="run date YYYY-MM-DD (driver snapshot to load)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("ride-hailing-curated")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    bucket = f"gs://{args.bucket}"

    drivers_raw = (
        spark.read.option("header", True)
        .csv(f"{bucket}/raw/driver_snapshot/dt={args.dt}/drivers.csv")
    )
    snapshot = prepare_driver_snapshot(drivers_raw, args.dt).cache()
    zones = spark.read.option("header", True).csv(f"{bucket}/reference/dim_zone/*.csv")

    rides = read_stream(spark, f"{args.project}.raw_ds.rides_stream").unionByName(
        read_history(spark, f"{args.project}.raw_ds.rides_history")
    )
    curated = derive(enrich(dedupe(clean(rides)), snapshot, zones))

    # Dynamic overwrite: only the dt partitions present in this run are replaced (idempotent).
    curated.write.mode("overwrite").partitionBy("dt").parquet(f"{bucket}/curated/rides")
    snapshot.write.mode("overwrite").partitionBy("dt").parquet(f"{bucket}/curated/dim_driver_snapshot")
    spark.stop()


if __name__ == "__main__":
    main()

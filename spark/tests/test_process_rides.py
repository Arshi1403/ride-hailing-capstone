import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import process_rides as pr  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def ts(h, m=0):
    return datetime(2026, 1, 1, h, m)


def test_clean_drops_bad_rows(spark):
    rows = [
        ("ok", ts(1), ts(2), 10.0, None, None, None),
        ("null_ts", None, ts(2), 10.0, None, None, None),
        ("neg_fare", ts(1), ts(2), -5.0, None, None, None),
        ("too_long", ts(1), datetime(2026, 1, 1, 6, 0, 1), 10.0, None, None, None),
    ]
    df = spark.createDataFrame(rows, "ride_id string, pickup_ts timestamp, dropoff_ts timestamp, "
                                     "fare double, ingest_ts timestamp, trip_distance double, "
                                     "surge_multiplier double")
    assert [r.ride_id for r in pr.clean(df).collect()] == ["ok"]


def test_dedupe_keeps_latest_ingest(spark):
    df = spark.createDataFrame(
        [("a", 1.0, ts(1)), ("a", 2.0, ts(2)), ("b", 3.0, None)],
        "ride_id string, fare double, ingest_ts timestamp",
    )
    out = {r.ride_id: r.fare for r in pr.dedupe(df).collect()}
    assert out == {"a": 2.0, "b": 3.0}


def test_derive_metrics(spark):
    df = spark.createDataFrame(
        [(ts(14), 16.09344, 10.0, 1.5), (ts(9), 5.0, None, None)],
        "pickup_ts timestamp, fare double, trip_distance double, surge_multiplier double",
    )
    rows = {r.hour_bucket: r for r in pr.derive(df).collect()}
    assert rows[14].revenue_per_km == pytest.approx(1.0)
    assert rows[14].surge_flag is True
    assert rows[9].revenue_per_km is None and rows[9].surge_flag is False
    assert rows[14].weekday == "Thursday"


def test_driver_snapshot_hash_changes_with_vehicle(spark):
    df = spark.createDataFrame(
        [("D1", "n", "1 Main St", "Sedan", "4.5"), ("D2", "n", "1 Main St", "SUV", "4.5")],
        "driver_id string, driver_name string, address string, vehicle_type string, rating string",
    )
    h = [r.attr_hash for r in pr.prepare_driver_snapshot(df, "2026-01-01").collect()]
    assert len(set(h)) == 2

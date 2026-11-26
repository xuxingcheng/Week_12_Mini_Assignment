"""A small, runnable PySpark pipeline for NYC for-hire trips, Q1 2024."""

import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import time

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parent


def timed_collect(spark, frame, label, plan_path=None):
    """Run the same action each time so the cache comparison is fair."""
    spark.sparkContext.setJobGroup(label, label)
    start = time.perf_counter()
    # A fresh query prevents reuse of an already executed AQE query stage.
    query = frame.select("*")
    rows = query.collect()  # Only the small aggregate reaches the driver.
    seconds = time.perf_counter() - start
    print(f"{label}: {seconds:.3f} seconds ({len(rows)} result rows)", flush=True)
    if plan_path is not None:
        save_plan(query, plan_path)
    return rows, seconds


def save_plan(frame, path):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        frame.explain(mode="formatted")
    path.write_text(buffer.getvalue())
    print(buffer.getvalue(), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ui-seconds", type=int, default=0,
                        help="Keep Spark UI alive after success for screenshots.")
    args = parser.parse_args()
    artifacts = ROOT / "artifacts"
    events = artifacts / "spark-events"
    events.mkdir(parents=True, exist_ok=True)
    paths = [ROOT / "data" / f"fhvhv_tripdata_2024-{m:02d}.parquet" for m in range(1, 4)]
    for path in paths + [ROOT / "data" / "taxi_zone_lookup.csv"]:
        if not path.exists():
            raise FileNotFoundError(f"Missing {path.name}; run python download_data.py first.")
    input_bytes = sum(p.stat().st_size for p in paths)
    if input_bytes < 1_000_000_000:
        raise ValueError("Download all three months: input must be at least 1 GB.")

    spark = (SparkSession.builder
             .appName("Week12-NYC-Trips")
             .master("local[4]")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.session.timeZone", "America/New_York")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", events.as_uri())
             .config("spark.local.dir", str(artifacts / "spark-local"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    started = time.perf_counter()
    try:
        print(f"Spark {spark.version}; master={sc.master}; UI={sc.uiWebUrl}", flush=True)
        print(f"Input: {input_bytes:,} bytes ({input_bytes / 1e9:.3f} GB)", flush=True)
        raw = spark.read.parquet(*[str(p) for p in paths])
        zones = (spark.read.option("header", True)
                 .schema("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")
                 .csv(str(ROOT / "data" / "taxi_zone_lookup.csv"))
                 .select("LocationID", "Borough")
                 .filter(F.col("Borough").isin("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island")))

        # TRANSFORMATIONS: build a plan; no trip-processing jobs run yet.
        sc.setJobGroup("lazy-demo", "Lazy transformations followed by count action")
        jobs_before = len(sc.statusTracker().getJobIdsForGroup("lazy-demo"))
        clean = (raw.select("pickup_datetime", "PULocationID", "trip_miles",
                            "trip_time", "base_passenger_fare")
                 .filter((F.col("pickup_datetime") >= "2024-01-01") &
                         (F.col("pickup_datetime") < "2024-04-01"))
                 .filter(F.col("trip_miles") > 0)
                 .filter(F.col("base_passenger_fare") > 0)
                 .filter(F.col("trip_time").between(60, 10800))
                 .withColumn("pickup_month", F.date_format("pickup_datetime", "yyyy-MM"))
                 .withColumn("pickup_hour", F.hour("pickup_datetime"))
                 .withColumn("duration_minutes", F.col("trip_time") / 60.0))
        # Broadcasting the tiny lookup avoids shuffling the large trip table.
        trips = clean.join(F.broadcast(zones), clean.PULocationID == zones.LocationID, "inner")
        monthly = trips.groupBy("pickup_month", "Borough").agg(
            F.count("*").alias("trips"),
            F.sum("base_passenger_fare").alias("total_base_fare"),
            F.avg("trip_miles").alias("avg_miles"),
            F.avg("duration_minutes").alias("avg_minutes"),
        )
        jobs_after_transformations = len(sc.statusTracker().getJobIdsForGroup("lazy-demo"))
        print(f"Lazy demo: jobs before={jobs_before}, after transformations={jobs_after_transformations}", flush=True)
        save_plan(monthly, artifacts / "physical_plan.txt")

        # ACTION: count triggers the scan, filtering and join.
        valid_rows = trips.count()
        jobs_after_action = len(sc.statusTracker().getJobIdsForGroup("lazy-demo"))
        print(f"After count action: {valid_rows:,} valid trips; jobs={jobs_after_action}", flush=True)
        sc.setJobGroup("input-count", "Count source rows using Parquet metadata")
        raw_rows = raw.count()

        # Compare identical collect actions with and without caching.
        baseline_rows, uncached_first = timed_collect(spark, monthly, "uncached-first")
        repeat_rows, uncached_repeat = timed_collect(spark, monthly, "uncached-repeat", artifacts / "adaptive_plan.txt")
        monthly.cache()  # Also lazy: the next action fills the cache.
        cached_rows, cache_fill = timed_collect(spark, monthly, "cache-materialization")
        _, cached_first = timed_collect(spark, monthly, "cached-first")
        _, cached_repeat = timed_collect(spark, monthly, "cached-repeat", artifacts / "cached_plan.txt")
        # Check exact group counts; aggregate floating-point sums can differ slightly.
        counts = lambda rows: {(r.pickup_month, r.Borough): r.trips for r in rows}
        assert counts(baseline_rows) == counts(repeat_rows) == counts(cached_rows)
        assert sum(r.trips for r in cached_rows) == valid_rows

        trips.createOrReplaceTempView("trips")
        monthly.createOrReplaceTempView("monthly_borough")
        # SQL 1: aggregate the large filtered dataset by pickup hour.
        hourly = spark.sql("""
            SELECT pickup_hour, COUNT(*) AS trips,
                   ROUND(AVG(base_passenger_fare), 2) AS avg_base_fare
            FROM trips
            GROUP BY pickup_hour
            ORDER BY trips DESC, pickup_hour
        """)
        hourly_rows, hourly_seconds = timed_collect(spark, hourly, "sql-hourly-demand")
        # SQL 2: reuse cached monthly aggregates; calculate a weighted average.
        borough = spark.sql("""
            SELECT Borough, SUM(trips) AS trips,
                   ROUND(SUM(total_base_fare) / SUM(trips), 2) AS avg_base_fare
            FROM monthly_borough
            GROUP BY Borough
            ORDER BY trips DESC, Borough
        """)
        borough_rows, _ = timed_collect(spark, borough, "sql-borough-summary")
        assert sum(r.trips for r in hourly_rows) == valid_rows
        assert sum(r.trips for r in borough_rows) == valid_rows
        print("SQL 1: busiest pickup hours", flush=True)
        for row in hourly_rows[:5]:
            print(row, flush=True)
        print("SQL 2: borough summary", flush=True)
        for row in borough_rows:
            print(row, flush=True)

        sc.setJobGroup("write-results", "Write and read back Parquet results")
        output = ROOT / "output"
        # A single partition is appropriate ONLY for these tiny final summaries.
        # Partition directories allow month-based pruning when results are read.
        monthly.coalesce(1).write.mode("overwrite").partitionBy("pickup_month").parquet(str(output / "monthly_borough"))
        borough.coalesce(1).write.mode("overwrite").parquet(str(output / "borough_summary"))
        hourly.coalesce(1).write.mode("overwrite").parquet(str(output / "hourly_demand"))
        check = spark.read.parquet(str(output / "monthly_borough"))
        assert check.count() == len(cached_rows)
        assert check.agg(F.sum("trips")).first()[0] == valid_rows
        save_plan(check.filter(F.col("pickup_month") == "2024-01"), artifacts / "partition_pruning_plan.txt")

        metrics = {
            "spark_version": spark.version, "master": sc.master,
            "input_bytes": input_bytes, "raw_rows": raw_rows, "valid_rows": valid_rows,
            "excluded_rows": raw_rows - valid_rows, "monthly_output_rows": len(cached_rows),
            "lazy_demo": {"jobs_before": jobs_before,
                          "jobs_after_transformations": jobs_after_transformations,
                          "jobs_after_action": jobs_after_action},
            "timings_seconds": {"uncached_first": uncached_first, "uncached_repeat": uncached_repeat,
                                "cache_materialization": cache_fill, "cached_first": cached_first,
                                "cached_repeat": cached_repeat, "hourly_query": hourly_seconds},
            "cache_repeat_speedup": uncached_repeat / cached_repeat,
            "monthly_results": [r.asDict() for r in sorted(cached_rows, key=lambda r: (r.pickup_month, r.Borough))],
            "borough_results": [r.asDict() for r in borough_rows],
            "hourly_results": [r.asDict() for r in hourly_rows],
            "total_seconds": time.perf_counter() - started,
        }
        (artifacts / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
        monthly.unpersist()
        print(f"SUCCESS: wrote and verified all three Parquet outputs in {metrics['total_seconds']:.2f}s.", flush=True)
        print(f"Cache repeat speedup: {metrics['cache_repeat_speedup']:.1f}x", flush=True)
        if args.ui_seconds > 0:
            print(f"Spark UI remains at {sc.uiWebUrl} for {args.ui_seconds}s for screenshots.", flush=True)
            time.sleep(args.ui_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

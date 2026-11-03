"""Validate published weekly totals with Spark, using only normalized local files.

Run on c0 with its normal c0-only Spark wrapper; these input files are not on a
shared filesystem. Example:
  6893-spark-submit spark_weekly_check.py --data-dir ~/6893/data/study/latest

The report records actual execution placement. This is a correctness comparison
with the DuckDB feature export, not a multi-host or speedup benchmark. Neither
pandas nor PyArrow is required by the Python driver.
"""

import argparse
from datetime import date, datetime, timezone
from functools import reduce
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import time


METRICS = ("dl_downloads", "observed_gh_event_count", "release_count")
PARTITIONS = 8


def now():
    return datetime.now(timezone.utc).isoformat()


def _json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError("Unsupported report value: " + type(value).__name__)


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(report, indent=2, sort_keys=True, default=_json_value, allow_nan=False) + "\n"
    with tempfile.NamedTemporaryFile("w", dir=path.parent, prefix=path.name + ".", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def input_record(path):
    record = {"path": str(path), "exists": path.exists(), "sha256": None}
    if path.is_file():
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        record.update(bytes=path.stat().st_size, sha256=digest.hexdigest())
    return record


def partition_probe(index, rows):
    import os
    import socket
    count = sum(1 for _ in rows)
    yield index, socket.gethostname(), os.environ.get("SPARK_LOCAL_IP"), count


def executor_resources(spark):
    output = {"executors": [], "available": False}
    try:
        iterator = spark.sparkContext._jsc.sc().statusStore().executorList(True).iterator()
        while iterator.hasNext():
            item = iterator.next()
            if item.id() == "driver":
                continue
            output["executors"].append({
                "executor_id": item.id(), "host_port": item.hostPort(),
                "cores": int(item.totalCores()), "completed_tasks": int(item.completedTasks()),
                "total_shuffle_read_bytes": int(item.totalShuffleRead()),
                "total_shuffle_write_bytes": int(item.totalShuffleWrite()),
            })
        output["available"] = True
        output["active_executor_cores"] = sum(item["cores"] for item in output["executors"])
    except Exception as error:
        output["error"] = type(error).__name__ + ": " + str(error)[:300]
    return output


def _require_columns(frame, columns, name):
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(name + " is missing columns: " + ", ".join(missing))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=os.environ.get("RADAR_STUDY_DIR", "~/6893/data/study/latest"))
    parser.add_argument("--daily", help="Normalized Parquet with an ISO date string")
    parser.add_argument("--reference", help="DuckDB weekly reference CSV")
    parser.add_argument("--output", help="Validation JSON output path")
    args = parser.parse_args(argv)
    root = Path(args.data_dir).expanduser().resolve()
    daily_path = Path(args.daily).expanduser().resolve() if args.daily else root / "daily_normalized.parquet"
    reference_path = Path(args.reference).expanduser().resolve() if args.reference else root / "weekly_reference.csv"
    output_path = Path(args.output).expanduser().resolve() if args.output else root / "spark_validation.json"
    started = time.perf_counter()
    report = {
        "ok": False, "status": "error", "started_at": now(),
        "scope": "Weekly correctness validation on the normalized study dataset",
        "placement_policy": "c0-only wrapper; local input files are not a shared filesystem",
        "multi_host_benchmark": False, "performance_claim": None,
        "timing_scope": "Entire validation, including scans, partition probes, shuffles and comparisons",
        "requested_partitions": PARTITIONS, "metrics": list(METRICS),
        "weekly_rule": "Monday UTC; exactly seven rows, seven distinct days and seven finite nonnull values per metric; otherwise NULL",
        "application_id": None, "master": None, "executor_hosts": [],
        "inputs": {}, "mismatch_rows": None, "mismatch_examples": [],
    }
    spark, daily, weekly, compared = None, None, None, None
    code = 1
    try:
        report["inputs"] = {"daily": input_record(daily_path), "reference": input_record(reference_path)}
        if not daily_path.exists() or not reference_path.exists():
            raise FileNotFoundError("Both daily_normalized.parquet and weekly_reference.csv must exist")
        from pyspark import StorageLevel
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.appName("AI Technology Radar weekly validation").getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.shuffle.partitions", str(PARTITIONS))
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.ansi.enabled", "false")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        spark.sparkContext.setLogLevel("WARN")
        context = spark.sparkContext
        report.update(application_id=context.applicationId, master=context.master, spark_version=spark.version)
        report["cpu_resources"] = {
            "driver_host": socket.gethostname(), "driver_os_logical_cpus": os.cpu_count(),
            "driver_jvm_available_processors": int(context._jvm.java.lang.Runtime.getRuntime().availableProcessors()),
            "spark_default_parallelism": context.defaultParallelism,
            "configured_executor_cores": context.getConf().get("spark.executor.cores", "not_configured"),
            "configured_total_executor_cores": context.getConf().get("spark.cores.max", "not_configured"),
        }
        raw = spark.read.parquet(daily_path.as_uri())
        _require_columns(raw, ["tool_id", "date", *METRICS], "Daily Parquet")
        report["input_schema"] = raw.schema.simpleString()
        raw = raw.select("tool_id", "date", *METRICS)
        date_text = F.col("date").cast("string")
        parsed_date = F.when(date_text.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(date_text, "yyyy-MM-dd"))
        projections = [F.col("tool_id").cast("string").alias("tool_id"), parsed_date.alias("date")]
        for metric in METRICS:
            value = F.col(metric).cast("double")
            valid = value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf"))
            projections.append(F.when(valid, value).otherwise(F.lit(None).cast("double")).alias(metric))
        daily = raw.select(*projections).repartition(PARTITIONS).persist(StorageLevel.MEMORY_AND_DISK)
        probe = daily.rdd.mapPartitionsWithIndex(partition_probe).collect()
        report["partition_execution"] = [
            {"partition": item[0], "host": item[1], "spark_local_ip": item[2], "rows": item[3]}
            for item in sorted(probe)
        ]
        daily_count = sum(item[3] for item in probe)
        report["daily_rows"] = daily_count
        report["input_partitions"] = daily.rdd.getNumPartitions()
        report["executor_hosts"] = sorted({item[1] for item in probe})
        if daily_count == 0:
            raise ValueError("The normalized daily dataset is empty; no validation can be claimed")
        if len(probe) != PARTITIONS or {item[0] for item in probe} != set(range(PARTITIONS)):
            raise ValueError("The eight requested partitions were not all executed")
        invalid_daily = daily.filter(F.col("tool_id").isNull() | (F.trim(F.col("tool_id")) == "") | F.col("date").isNull()).count()
        duplicates = daily.groupBy("tool_id", "date").count().filter(F.col("count") > 1)
        duplicate_summary = duplicates.agg(F.sum(F.col("count") - 1).alias("excess_rows")).first()
        report["invalid_daily_keys"] = invalid_daily
        report["duplicate_daily_excess_rows"] = int(duplicate_summary["excess_rows"] or 0)

        reference_raw = spark.read.option("header", "true").option("inferSchema", "false").csv(reference_path.as_uri())
        _require_columns(reference_raw, ["tool_id", "week_start", *METRICS], "Weekly reference CSV")
        reference_values = []
        for metric in METRICS:
            value = F.col(metric).cast("double")
            valid = value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf"))
            reference_values.append(F.when(valid, value).otherwise(F.lit(None).cast("double")).alias(metric))
        reference = reference_raw.select(
            F.col("tool_id").cast("string").alias("tool_id"),
            F.to_date(F.col("week_start"), "yyyy-MM-dd").alias("week_start"),
            *reference_values,
        )
        report["reference_rows"] = reference.count()
        if not report["reference_rows"]:
            raise ValueError("The weekly reference is empty; no comparison can be claimed")
        invalid_reference = reference.filter(F.col("tool_id").isNull() | (F.trim(F.col("tool_id")) == "")
                                             | F.col("week_start").isNull() | (F.dayofweek("week_start") != 2)).count()
        report["invalid_reference_keys"] = invalid_reference
        reference_duplicate_keys = reference.groupBy("tool_id", "week_start").count().filter(F.col("count") > 1).count()
        report["duplicate_reference_keys"] = reference_duplicate_keys
        if invalid_reference or reference_duplicate_keys:
            raise ValueError("The reference contains invalid Monday keys or duplicate tool-week keys")

        aggregates = [F.count(F.lit(1)).alias("daily_rows"), F.countDistinct("date").alias("distinct_days")]
        for metric in METRICS:
            aggregates += [F.count(metric).alias(metric + "_days"), F.sum(metric).alias(metric + "_sum")]
        weekly_raw = daily.withColumn("week_start", F.to_date(F.date_trunc("week", F.col("date")))) \
            .groupBy("tool_id", "week_start").agg(*aggregates)
        projections = ["tool_id", "week_start", "daily_rows", "distinct_days"]
        for metric in METRICS:
            valid = (F.col("daily_rows") == 7) & (F.col("distinct_days") == 7) & (F.col(metric + "_days") == 7)
            projections += [F.when(valid, F.col(metric + "_sum")).otherwise(F.lit(None).cast("double")).alias(metric),
                            F.col(metric + "_days")]
        weekly_all = weekly_raw.select(*projections)
        week_scope = reference.select("week_start").distinct()
        excluded = weekly_all.join(week_scope, "week_start", "left_anti")
        report["excluded_out_of_reference_week_rows"] = excluded.count()
        weekly = weekly_all.join(week_scope, "week_start", "left_semi").persist(StorageLevel.MEMORY_AND_DISK)
        report["spark_weekly_rows"] = weekly.count()
        report["weekly_aggregation_partitions"] = weekly_raw.rdd.getNumPartitions()
        report["weekly_aggregation_plan"] = weekly_raw._jdf.queryExecution().executedPlan().toString()[:12000]
        report["coverage_histogram"] = [row.asDict() for row in weekly.groupBy("daily_rows", "distinct_days", *[metric + "_days" for metric in METRICS]).count().collect()]
        bounds = reference.agg(F.min("week_start").alias("first"), F.max("week_start").alias("last")).first()
        report["reference_week_range"] = {"first": bounds["first"], "last": bounds["last"]}

        actual = weekly.select("tool_id", "week_start", *[F.col(metric).alias(metric + "_spark") for metric in METRICS]).withColumn("spark_row_present", F.lit(True))
        expected = reference.select("tool_id", "week_start", *[F.col(metric).alias(metric + "_reference") for metric in METRICS]).withColumn("reference_row_present", F.lit(True))
        compared = actual.join(expected, ["tool_id", "week_start"], "full_outer").persist(StorageLevel.MEMORY_AND_DISK)
        missing_row = F.col("spark_row_present").isNull() | F.col("reference_row_present").isNull()
        failures = [~F.col(metric + "_spark").eqNullSafe(F.col(metric + "_reference")) for metric in METRICS]
        mismatches = compared.filter(missing_row | reduce(lambda a, b: a | b, failures))
        report["mismatch_rows"] = mismatches.count()
        report["mismatch_examples"] = [row.asDict() for row in mismatches.orderBy("tool_id", "week_start").limit(20).collect()]
        metric_checks = {}
        for metric in METRICS:
            actual_value, expected_value = F.col(metric + "_spark"), F.col(metric + "_reference")
            stats = compared.agg(
                F.count(actual_value).alias("spark_nonnull_weeks"), F.count(expected_value).alias("reference_nonnull_weeks"),
                F.sum(F.when(~actual_value.eqNullSafe(expected_value), 1).otherwise(0)).alias("value_or_null_mismatches"),
                F.sum(F.when(actual_value.isNull() != expected_value.isNull(), 1).otherwise(0)).alias("null_coverage_mismatches"),
                F.sum(actual_value).alias("spark_total"), F.sum(expected_value).alias("reference_total"),
            ).first().asDict()
            metric_checks[metric] = stats
        report["metric_checks"] = metric_checks
        observed_totals = sum(item["spark_nonnull_weeks"] for item in metric_checks.values())
        if not observed_totals:
            raise ValueError("Every compared total is NULL; no observed metric totals were validated")
        report["ok"] = report["mismatch_rows"] == 0 and not invalid_daily and not report["duplicate_daily_excess_rows"]
        report["status"] = "matched" if report["ok"] else "mismatch"
        code = 0 if report["ok"] else 2
    except Exception as error:
        report["ok"] = False
        report["status"] = "error"
        report["error"] = {"type": type(error).__name__, "message": str(error)[:3000]}
    finally:
        if spark is not None:
            report["executor_resources"] = executor_resources(spark)
            for frame in (compared, weekly, daily):
                if frame is not None:
                    try:
                        frame.unpersist()
                    except Exception:
                        pass
            try:
                spark.stop()
            except Exception as error:
                report["spark_stop_error"] = type(error).__name__
        report["finished_at"] = now()
        report["runtime_seconds"] = round(time.perf_counter() - started, 6)
        write_report(output_path, report)
    print(json.dumps({key: report.get(key) for key in ["ok", "status", "application_id", "master", "daily_rows", "spark_weekly_rows", "mismatch_rows", "runtime_seconds", "executor_hosts"]}, default=_json_value))
    return code


if __name__ == "__main__":
    raise SystemExit(main())

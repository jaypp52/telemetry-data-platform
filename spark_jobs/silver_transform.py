"""bronze (arrival date D and D+1) -> silver (event date D), one run per date.

Steps for ``--date D``:

1. Read bronze partitions ``event_date in (D, D+1)``. Bronze is partitioned by arrival
   time, so D+1 holds events that happened on D but arrived after midnight (late events).
2. Parse the JSON envelope. Rows with an impossible clock (``event_ts`` before 2020 or more
   than 10 minutes after the broker received them) belong to their *arrival* date and are
   rejected; every other row belongs to the date of its ``event_ts``. Keep rows of date D.
3. Deduplicate on ``event_id``, keeping the earliest ``ingest_ts`` (ties: Kafka order).
4. Parse each event type with the schema registry; rows breaking the JSON Schema
   (missing field, wrong type, out of range, bad enum) go to ``silver/_rejects``.
5. Write ``silver/<event_type>/event_date=D`` with dynamic partition overwrite. Rows are
   written in a total order, so re-running a date reproduces the same data pages byte for
   byte (only parquet-mr's unordered list of encodings in the footer can change between
   JVM runs; ``scripts/stats.py`` normalises it when it checksums files).
6. Write ``run_reports/silver_<D>.json`` (rows in/out, duplicates, late, rejects by reason).

    spark-submit spark_jobs/silver_transform.py --date 2026-01-03 [--date ...]
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import Column, DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

from spark_jobs.common import (  # noqa: E402
    ENVELOPE_FIELDS,
    EVENT_TYPES,
    Paths,
    get_spark,
    paths,
    payload_fields,
    spark_schema,
    validation_rules,
    write_json,
)

MIN_VALID_TS = "2020-01-01 00:00:00"
MAX_CLOCK_SKEW = "INTERVAL 10 MINUTES"
LATE_AFTER = "INTERVAL 5 MINUTES"  # arrival more than this after event_ts counts as a late event
ENVELOPE_SCHEMA = StructType([StructField(f, StringType()) for f in ENVELOPE_FIELDS])
LINEAGE = ["kafka_ts", "ingest_ts", "kafka_partition", "kafka_offset", "arrival_date", "is_late"]
CLOCK_REASONS = ("invalid_timestamp", "clock_before_2020", "clock_in_future")


def read_bronze_window(spark: SparkSession, p: Paths, d: date) -> DataFrame | None:
    if not (p.bronze / "_spark_metadata").exists():
        return None
    # Read the table root (not the partition folders) so Spark uses the streaming sink's
    # commit log and never sees files from an uncommitted micro-batch.
    bronze = spark.read.parquet(str(p.bronze))
    return bronze.where(F.col("event_date").isin(d, d + timedelta(days=1)))


def attribute(bronze: DataFrame) -> DataFrame:
    """Parse the envelope, flag clock problems and decide which date each row belongs to."""
    env = F.from_json("value", ENVELOPE_SCHEMA)
    df = (
        bronze.withColumnRenamed("event_date", "arrival_date")
        .withColumn("env", env)
        .select("*", *[F.col(f"env.{f}").alias(f) for f in ENVELOPE_FIELDS])
        .drop("env")
        .withColumnRenamed("event_ts", "event_ts_raw")
        .withColumn("event_ts", F.col("event_ts_raw").cast("timestamp"))
    )
    clock_reason = (
        F.when(F.col("event_ts").isNull(), "invalid_timestamp")
        .when(F.col("event_ts") < F.lit(MIN_VALID_TS).cast("timestamp"), "clock_before_2020")
        .when(F.col("event_ts") > F.col("kafka_ts") + F.expr(MAX_CLOCK_SKEW), "clock_in_future")
    )
    return (
        df.withColumn("clock_reason", clock_reason)
        .withColumn("event_date", F.when(F.col("clock_reason").isNull(), F.to_date("event_ts"))
                    .otherwise(F.col("arrival_date")))
        .withColumn("is_late", F.col("kafka_ts") > F.col("event_ts") + F.expr(LATE_AFTER))
    )


def dedupe(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("event_id").orderBy("ingest_ts", "kafka_ts", "kafka_partition", "kafka_offset")
    return df.withColumn("dup_rank", F.row_number().over(w))


def schema_violation(event_type: str, parsed: str = "p") -> Column:
    """First JSON-Schema rule the parsed payload breaks, as 'field:rule', else null."""
    checks = []
    for field, rule, arg in validation_rules(event_type):
        c = F.col(f"{parsed}.{field}")
        if rule == "required":
            bad = c.isNull()
        elif rule == "minimum":
            bad = c < F.lit(arg)
        elif rule == "maximum":
            bad = c > F.lit(arg)
        elif rule == "enum":
            bad = ~c.isin(*arg)
        else:  # pattern
            bad = ~c.rlike(arg)
        checks.append(F.when(bad, F.lit(f"{field}:{rule}")))
    return F.coalesce(*checks)


def parse_type(df: DataFrame, event_type: str) -> DataFrame:
    """Parse one event type's payload; adds ``reject_detail`` (null when valid)."""
    return (
        df.where(F.col("event_type") == event_type)
        .withColumn("p", F.from_json("value", spark_schema(event_type)))
        .withColumn("reject_detail", schema_violation(event_type))
    )


def write_partition(df: DataFrame, root, d: date, sort_cols: list[str]) -> int:
    """Replace exactly partition D under ``root``. Returns the row count written."""
    n = df.count()
    part_dir = root / f"event_date={d.isoformat()}"
    if n == 0:
        shutil.rmtree(part_dir, ignore_errors=True)  # nothing left for D: drop the stale partition
        return 0
    (
        df.coalesce(1)  # one file per date and type: small, and a total row order
        .sortWithinPartitions(*sort_cols)
        .write.mode("overwrite")  # with partitionOverwriteMode=dynamic only D is replaced
        .partitionBy("event_date")
        .parquet(str(root))
    )
    return n


def transform_date(spark: SparkSession, d: date, p: Paths | None = None) -> dict:
    p = p or paths()
    t0 = time.monotonic()
    window = [d.isoformat(), (d + timedelta(days=1)).isoformat()]
    report: dict = {"date": d.isoformat(), "bronze_partitions_read": window}
    bronze = read_bronze_window(spark, p, d)
    if bronze is None:
        report.update(rows_in=0, note="no bronze data yet")
        write_json(p.run_reports / f"silver_{d.isoformat()}.json", report)
        return report

    attributed = attribute(bronze)
    too_late = attributed.where(
        (F.col("arrival_date") == F.lit(d + timedelta(days=1))) & F.col("clock_reason").isNull()
        & (F.col("event_date") < F.lit(d))
    ).count()
    ranked = dedupe(attributed.where(F.col("event_date") == F.lit(d))).persist(StorageLevel.MEMORY_AND_DISK)
    unique = ranked.where(F.col("dup_rank") == 1)

    rejects = [
        unique.where(F.col("clock_reason").isNotNull())
        .withColumn("reject_reason", F.col("clock_reason")).withColumn("reject_detail", F.lit(None).cast("string")),
        unique.where(F.col("clock_reason").isNull() & ~F.col("event_type").isin(*EVENT_TYPES))
        .withColumn("reject_reason", F.lit("unknown_event_type")).withColumn("reject_detail", F.col("event_type")),
    ]
    clean = unique.where(F.col("clock_reason").isNull())
    rows_out, late, late_next_day = {}, 0, 0
    for et in EVENT_TYPES:
        parsed = parse_type(clean, et).persist(StorageLevel.MEMORY_AND_DISK)
        rejects.append(parsed.where(F.col("reject_detail").isNotNull())
                       .withColumn("reject_reason", F.lit("schema_violation")).drop("p"))
        good = parsed.where(F.col("reject_detail").isNull())
        out = good.select(
            "event_id", "device_id", "event_type", "event_ts", "firmware_version",
            *[F.col(f"p.{f}").alias(f) for f in payload_fields(et)],
            *LINEAGE, "event_date",
        )
        rows_out[et] = write_partition(out, p.silver / et, d, ["event_ts", "event_id"])
        stats = good.agg(F.sum(F.col("is_late").cast("int")).alias("late"),
                         F.sum((F.col("arrival_date") > F.lit(d)).cast("int")).alias("next_day")).first()
        late += stats["late"] or 0
        late_next_day += stats["next_day"] or 0
        parsed.unpersist()

    reject_cols = ["event_id", "device_id", "event_type", "event_ts_raw", "firmware_version", "reject_reason",
                   "reject_detail", "value", *LINEAGE, "event_date"]
    all_rejects = rejects[0].select(*reject_cols)
    for r in rejects[1:]:
        all_rejects = all_rejects.unionByName(r.select(*reject_cols))
    all_rejects = all_rejects.persist(StorageLevel.MEMORY_AND_DISK)
    by_reason = {r["reject_reason"]: r["count"] for r in all_rejects.groupBy("reject_reason").count().collect()}
    write_partition(all_rejects, p.rejects, d, ["event_id", "reject_reason"])

    counts = ranked.agg(F.count("*").alias("rows_in"), F.sum((F.col("dup_rank") > 1).cast("int")).alias("dups")).first()
    report.update(
        rows_in=counts["rows_in"],
        duplicates_removed=counts["dups"] or 0,
        rows_out=sum(rows_out.values()),
        rows_out_by_type=rows_out,
        late_events=late,
        late_events_recovered_from_next_day=late_next_day,
        too_late_dropped=too_late,
        rejects=sum(by_reason.values()),
        rejects_by_reason=dict(sorted(by_reason.items())),
        runtime_s=round(time.monotonic() - t0, 2),
    )
    all_rejects.unpersist()
    ranked.unpersist()
    write_json(p.run_reports / f"silver_{d.isoformat()}.json", report)
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", action="append", required=True, type=date.fromisoformat,
                    help="event date to (re)build; repeat to build several")
    a = ap.parse_args(argv)
    spark = get_spark("silver_transform")
    for d in sorted(set(a.date)):
        r = transform_date(spark, d)
        print({k: r.get(k) for k in ("date", "rows_in", "duplicates_removed", "rows_out", "rejects", "late_events",
                                     "late_events_recovered_from_next_day", "runtime_s")})
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

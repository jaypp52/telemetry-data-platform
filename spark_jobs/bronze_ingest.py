"""Kafka -> bronze Parquet (Spark Structured Streaming, ``trigger(availableNow=True)``).

Each run reads everything that arrived since the last committed offsets, writes it, and
stops. Offsets live in the checkpoint and the Parquet file sink commits files through its
``_spark_metadata`` log, so re-running ingest never duplicates rows.

Bronze keeps the raw JSON string plus Kafka metadata. It is partitioned by *arrival* time
(the Kafka record timestamp): bronze is append-only and an old partition is never
rewritten when a late event shows up. Silver re-buckets rows by event time.
Messages that are not parseable JSON envelopes go to ``bronze/_quarantine``.

    spark-submit spark_jobs/bronze_ingest.py [--source kafka|files]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import UTC, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType  # noqa: E402

from spark_jobs.common import Paths, get_spark, paths, write_json  # noqa: E402

TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry.raw")
BRONZE_COLUMNS = ["kafka_key", "value", "topic", "kafka_partition", "kafka_offset", "kafka_ts", "ingest_ts"]
LANDING_SCHEMA = StructType([
    StructField("key", StringType()), StructField("value", StringType()), StructField("topic", StringType()),
    StructField("partition", IntegerType()), StructField("offset", LongType()), StructField("timestamp", LongType()),
])


def kafka_source(spark: SparkSession, bootstrap: str, topic: str = TOPIC) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", os.environ.get("BRONZE_MAX_OFFSETS_PER_TRIGGER", "400000"))
        .load()
    )
    return raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )


def file_source(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Kafka-shaped JSONL files (``simulator --sink jsonl``): same stream semantics, no broker."""
    raw = spark.readStream.schema(LANDING_SCHEMA).option("maxFilesPerTrigger", 4).json(landing_dir)
    return raw.select(
        F.col("key").alias("kafka_key"),
        F.col("value"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.timestamp_millis(F.col("timestamp")).alias("kafka_ts"),
    )


def to_bronze(df: DataFrame) -> DataFrame:
    """Add ingest time, arrival partitions and a malformed flag. Pure function: unit-tested."""
    envelope_ok = (
        F.get_json_object("value", "$.event_id").isNotNull()
        & F.get_json_object("value", "$.event_type").isNotNull()
        & F.get_json_object("value", "$.device_id").isNotNull()
    )
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("event_date", F.to_date("kafka_ts"))
        .withColumn("event_hour", F.hour("kafka_ts"))
        .withColumn("is_malformed", ~F.coalesce(envelope_ok, F.lit(False)))
    )


def start_queries(stream: DataFrame, p: Paths, checkpoint_prefix: str = "bronze"):
    bronze = to_bronze(stream)
    good = (
        bronze.where(~F.col("is_malformed")).select(*BRONZE_COLUMNS, "event_date", "event_hour")
        .writeStream.format("parquet")
        .option("path", str(p.bronze))
        .option("checkpointLocation", str(p.checkpoints / f"{checkpoint_prefix}_telemetry"))
        .partitionBy("event_date", "event_hour")
        .trigger(availableNow=True)
        .queryName("bronze_telemetry")
        .start()
    )
    bad = (
        bronze.where(F.col("is_malformed")).select(*BRONZE_COLUMNS, "event_date")
        .withColumn("quarantine_reason", F.lit("unparseable_envelope"))
        .writeStream.format("parquet")
        .option("path", str(p.quarantine))
        .option("checkpointLocation", str(p.checkpoints / f"{checkpoint_prefix}_quarantine"))
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .queryName("bronze_quarantine")
        .start()
    )
    return good, bad


def run(spark: SparkSession, source: str = "kafka", bootstrap: str | None = None, landing_dir: str | None = None,
        p: Paths | None = None, topic: str = TOPIC) -> dict:
    p = p or paths()
    t0 = time.monotonic()
    if source == "kafka":
        stream = kafka_source(spark, bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), topic)
        prefix = "bronze"
    else:
        landing = landing_dir or str(p.landing / topic)
        os.makedirs(landing, exist_ok=True)
        stream = file_source(spark, landing)
        prefix = "bronze_files"
    good, bad = start_queries(stream, p, prefix)
    good.awaitTermination()
    bad.awaitTermination()

    def rows(q) -> int:
        return int(sum(prog["numInputRows"] for prog in q.recentProgress))

    report = {
        "source": source,
        "messages_read": rows(good),  # both queries read the same input; bad rows go to quarantine
        "micro_batches": len(good.recentProgress),
        "runtime_s": round(time.monotonic() - t0, 2),
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    write_json(p.run_reports / "bronze" / f"bronze_{stamp}.json", report)
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["kafka", "files"], default=os.environ.get("BRONZE_SOURCE", "kafka"))
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--landing-dir", default=None)
    a = ap.parse_args(argv)
    spark = get_spark("bronze_ingest", kafka=a.source == "kafka")
    report = run(spark, a.source, a.bootstrap, a.landing_dir)
    print(report)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

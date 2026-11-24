"""Shared Spark plumbing: session builder, lake paths and the schema registry.

The schema registry is built from ``simulator/schemas/*.json`` so the producer contract
and the Spark parsing schemas can never drift apart.
"""

from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
SCHEMA_DIR = PROJECT_DIR / "simulator" / "schemas"
EVENT_TYPES = ("heartbeat", "lte_sample", "gps_fix", "trip_start", "trip_end", "device_event")
ENVELOPE_FIELDS = ("event_id", "device_id", "event_type", "event_ts", "firmware_version")

SPARK_VERSION = "3.5.8"
KAFKA_PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
])


@dataclass(frozen=True)
class Paths:
    data: Path

    @property
    def lake(self) -> Path:
        return self.data / "lake"

    @property
    def bronze(self) -> Path:
        return self.lake / "bronze" / "telemetry"

    @property
    def quarantine(self) -> Path:
        return self.lake / "bronze" / "_quarantine"

    @property
    def silver(self) -> Path:
        return self.lake / "silver"

    @property
    def rejects(self) -> Path:
        return self.lake / "silver" / "_rejects"

    @property
    def checkpoints(self) -> Path:
        return self.data / "checkpoints"

    @property
    def landing(self) -> Path:
        return self.data / "landing"

    @property
    def run_reports(self) -> Path:
        return self.data / "run_reports"


def paths() -> Paths:
    return Paths(Path(os.environ.get("DATA_DIR", "/opt/data")))


def _kafka_jars_present() -> bool:
    import pyspark

    return bool(glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "spark-sql-kafka-0-10_*.jar")))


def get_spark(app: str, kafka: bool = False):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")  # no _SUCCESS files
    )
    if kafka and not _kafka_jars_present():  # the Docker image bakes the jars in; CI downloads them
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGES)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


@cache
def json_schema(event_type: str) -> dict:
    return json.loads((SCHEMA_DIR / f"{event_type}.json").read_text())


def payload_fields(event_type: str) -> list[str]:
    """Fields specific to an event type, in schema order (envelope fields excluded)."""
    return [f for f in json_schema(event_type)["properties"] if f not in ENVELOPE_FIELDS]


def spark_schema(event_type: str):
    """StructType used by ``from_json`` for one event type (event_ts kept as string, cast later)."""
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

    mapping = {"number": DoubleType(), "integer": IntegerType(), "boolean": BooleanType(), "string": StringType()}
    fields = []
    for name, spec in json_schema(event_type)["properties"].items():
        fields.append(StructField(name, mapping.get(spec.get("type", "string"), StringType()), True))
    return StructType(fields)


def validation_rules(event_type: str) -> list[tuple[str, str, object]]:
    """(field, rule, arg) tuples from the JSON Schema: required, minimum, maximum, enum, pattern."""
    schema = json_schema(event_type)
    rules: list[tuple[str, str, object]] = [(f, "required", None) for f in schema["required"]]
    for name, spec in schema["properties"].items():
        for rule in ("minimum", "maximum", "enum", "pattern"):
            if rule in spec:
                rules.append((name, rule, spec[rule]))
    return rules


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
    tmp.replace(path)

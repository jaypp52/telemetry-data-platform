"""Kafka -> bronze against a real broker. Runs when KAFKA_BOOTSTRAP_SERVERS is set (CI does)."""

import json
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP_SERVERS not set")]


def produce(topic: str, start: int, n: int) -> None:
    from simulator.events import Message
    from simulator.produce import KafkaSink
    from tests.sparkhelpers import heartbeat, uid

    sink = KafkaSink(BOOTSTRAP, topic)
    now = datetime.now(UTC) - timedelta(minutes=5)
    for i in range(start, start + n):
        ts = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        sink.send(Message(now, i, "dev-000001", json.dumps(heartbeat(uid(i), ts))), use_arrival_ts=True)
    sink.send(Message(now, -1, "dev-000001", '{"event_id": "trunc'), use_arrival_ts=True)
    sink.close()


def test_kafka_to_bronze_is_exactly_once_across_reruns(spark, lake):
    from confluent_kafka.admin import AdminClient, NewTopic

    from spark_jobs.bronze_ingest import run

    topic = f"test.telemetry.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(30)

    produce(topic, 0, 200)
    run(spark, source="kafka", bootstrap=BOOTSTRAP, p=lake, topic=topic)
    run(spark, source="kafka", bootstrap=BOOTSTRAP, p=lake, topic=topic)  # nothing new
    bronze = spark.read.parquet(str(lake.bronze))
    assert bronze.count() == 200
    assert bronze.select("kafka_partition", "kafka_offset").distinct().count() == 200
    assert spark.read.parquet(str(lake.quarantine)).count() == 1

    produce(topic, 1000, 25)
    run(spark, source="kafka", bootstrap=BOOTSTRAP, p=lake, topic=topic)
    assert spark.read.parquet(str(lake.bronze)).count() == 225
    admin.delete_topics([topic])

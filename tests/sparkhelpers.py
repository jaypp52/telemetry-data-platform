"""Helpers to build Kafka-shaped landing files for Spark tests."""

import json
from datetime import UTC, datetime
from pathlib import Path


def ts_ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def record(value, arrival: str, offset: int, partition: int = 0, key: str = "dev-000001") -> dict:
    if not isinstance(value, str):
        value = json.dumps(value)
    return {"key": key, "value": value, "topic": "telemetry.raw", "partition": partition, "offset": offset,
            "timestamp": ts_ms(arrival)}


def write_landing(directory: Path, records: list[dict], name: str = "part-00000.jsonl") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text("".join(json.dumps(r) + "\n" for r in records))


def heartbeat(event_id: str, event_ts: str, **overrides) -> dict:
    event = {"event_id": event_id, "device_id": "dev-000001", "event_type": "heartbeat", "event_ts": event_ts,
             "firmware_version": "1.0.0", "cpu_pct": 30.0, "mem_free_mb": 400, "temp_c": 50.0,
             "storage_free_pct": 70.0}
    event.update(overrides)
    return event


def uid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"

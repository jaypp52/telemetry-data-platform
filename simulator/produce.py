"""Simulator CLI: generate device telemetry and publish it to Kafka.

Examples
--------
Historical backfill data, as fast as possible::

    python -m simulator.produce --mode historical --devices 500 --start 2026-01-01 --days 7

A continuous live stream for the hourly DAG (event time = now, same fleet timeline)::

    python -m simulator.produce --mode live --devices 500 --rate 50/s

``--sink jsonl`` writes Kafka-shaped records to ``$DATA_DIR/landing`` instead, so tests
and CI can run the whole pipeline without a broker.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import zlib
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

from simulator.devices import build_fleet, rollout_for, write_reference_files
from simulator.events import FaultConfig, FleetSimulator, Message

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/data"))
TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry.raw")
PARTITIONS = 6


class KafkaSink:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer  # imported lazily so jsonl mode needs no broker

        self.topic = topic
        self.errors = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # broker drops producer retries: no duplicates from us
            "acks": "all",
            "linger.ms": 50,
            "batch.num.messages": 10_000,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500_000,
        })

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.errors += 1

    def send(self, msg: Message, use_arrival_ts: bool) -> None:
        kwargs = {"timestamp": int(msg.arrival.timestamp() * 1000)} if use_arrival_ts else {}
        while True:
            try:
                self.producer.produce(self.topic, key=msg.key, value=msg.value, on_delivery=self._on_delivery, **kwargs)
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(60)
        if remaining or self.errors:
            raise RuntimeError(f"Kafka delivery failed: {self.errors} errors, {remaining} unflushed")


class JsonlSink:
    """Kafka-shaped records on disk (key, value, topic, partition, offset, timestamp)."""

    def __init__(self, landing_dir: Path, topic: str, batch_size: int = 200_000):
        self.dir = landing_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.topic, self.batch_size = topic, batch_size
        self.offsets_path = self.dir / "_offsets.json"
        self.offsets = json.loads(self.offsets_path.read_text()) if self.offsets_path.exists() else {}
        self.buf: list[str] = []
        self.file_no = len(list(self.dir.glob("part-*.jsonl")))

    def send(self, msg: Message, use_arrival_ts: bool) -> None:
        partition = zlib.crc32(msg.key.encode()) % PARTITIONS
        offset = self.offsets.get(str(partition), 0)
        self.offsets[str(partition)] = offset + 1
        ts = msg.arrival if use_arrival_ts else datetime.now(UTC)
        self.buf.append(json.dumps({"key": msg.key, "value": msg.value, "topic": self.topic, "partition": partition,
                                    "offset": offset, "timestamp": int(ts.timestamp() * 1000)}))
        if len(self.buf) >= self.batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self.buf:
            return
        final = self.dir / f"part-{self.file_no:05d}.jsonl"
        tmp = self.dir / f".{final.name}.tmp"  # Spark's file source ignores dot-files
        tmp.write_text("\n".join(self.buf) + "\n")
        tmp.rename(final)
        self.file_no += 1
        self.buf = []
        self.offsets_path.write_text(json.dumps(self.offsets))

    def close(self) -> None:
        self._flush()


def parse_rate(text: str) -> float:
    return float(text.lower().removesuffix("/s"))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["historical", "live"], default="historical")
    p.add_argument("--devices", type=int, default=500)
    p.add_argument("--start", default=os.environ.get("SIM_START_DATE", "2026-01-01"),
                   help="first simulated UTC day; anchors the fleet timeline in both modes")
    p.add_argument("--days", type=float, default=7, help="days in the simulated window (rollout spans 25%%-85%%)")
    p.add_argument("--rate", type=parse_rate, default=50.0, help="live: max events/sec, e.g. 50/s")
    p.add_argument("--duration-min", type=float, default=None, help="live: stop after N minutes (default: run forever)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sink", choices=["kafka", "jsonl"], default="kafka")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--data-dir", type=Path, default=DATA_DIR)
    g = p.add_argument_group("fault injection (each can be tuned or switched off)")
    g.add_argument("--no-faults", action="store_true", help="disable every fault below")
    g.add_argument("--crash-model", default="TX-200")
    g.add_argument("--crash-multiplier", type=float, default=2.0)
    g.add_argument("--bad-lte-fraction", type=float, default=0.03)
    g.add_argument("--no-dead-zone", action="store_true")
    g.add_argument("--duplicate-rate", type=float, default=0.005)
    g.add_argument("--late-rate", type=float, default=0.01)
    g.add_argument("--late-max-hours", type=float, default=6.0)
    g.add_argument("--clock-error-rate", type=float, default=0.002)
    g.add_argument("--malformed-rate", type=float, default=0.0001)
    return p


def faults_from_args(a: argparse.Namespace) -> FaultConfig:
    if a.no_faults:
        return FaultConfig.disabled()
    return FaultConfig(crash_model=a.crash_model, crash_multiplier=a.crash_multiplier,
                       bad_lte_fraction=a.bad_lte_fraction, dead_zone=not a.no_dead_zone,
                       duplicate_rate=a.duplicate_rate, late_rate=a.late_rate, late_max_hours=a.late_max_hours,
                       clock_error_rate=a.clock_error_rate, malformed_rate=a.malformed_rate)


def run_historical(sim: FleetSimulator, sink, start: datetime, days: float) -> int:
    sent, end = 0, start + timedelta(days=days)
    window = start
    while window < end:
        nxt = min(end, window + timedelta(hours=1))
        sim.advance(window, nxt)
        for msg in sim.drain(until=nxt):  # late events stay buffered until they "arrive"
            sink.send(msg, use_arrival_ts=True)
            sent += 1
        window = nxt
    for msg in sim.drain():
        sink.send(msg, use_arrival_ts=True)
        sent += 1
    return sent


def run_live(sim: FleetSimulator, sink, rate: float, duration_min: float | None) -> int:
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    began = time.monotonic()
    minute = datetime.now(UTC).replace(second=0, microsecond=0)
    ready: deque[Message] = deque()
    tokens, last, sent = rate, time.monotonic(), 0
    while not stop["flag"]:
        now = datetime.now(UTC)
        if now >= minute:
            sim.advance(minute, minute + timedelta(minutes=1))
            minute += timedelta(minutes=1)
        ready.extend(sim.drain(until=now))
        t = time.monotonic()
        tokens, last = min(rate, tokens + (t - last) * rate), t
        while ready and tokens >= 1:  # token bucket: never exceed --rate
            sink.send(ready.popleft(), use_arrival_ts=False)
            tokens -= 1
            sent += 1
        if duration_min is not None and time.monotonic() - began >= duration_min * 60:
            break
        time.sleep(0.05)
    return sent


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)
    faults = faults_from_args(a)
    # The fleet's timeline (install dates, firmware rollout) is anchored at --start in both
    # modes, so a live stream started after the historical window continues the same world:
    # devices that upgraded during the rollout keep reporting 1.1.0.
    start = datetime.fromisoformat(a.start).replace(tzinfo=UTC)
    fleet = build_fleet(a.devices, start, a.days, a.seed, faults.bad_lte_fraction)
    write_reference_files(fleet, rollout_for(start, a.days), a.data_dir / "lake" / "reference")

    # Each live session gets its own random stream; reusing the historical one would replay
    # the same event UUIDs (dbt's unique test on silver event_id caught exactly that).
    stream = "historical" if a.mode == "historical" else f"live-{datetime.now(UTC):%Y%m%dT%H%M%S}"
    sim = FleetSimulator(fleet, faults, a.seed, stream)
    if a.sink == "kafka":
        sink = KafkaSink(a.bootstrap, a.topic)
    else:
        sink = JsonlSink(a.data_dir / "landing" / a.topic, a.topic)
    t0 = time.monotonic()
    if a.mode == "historical":
        sent = run_historical(sim, sink, start, a.days)
    else:
        sent = run_live(sim, sink, a.rate, a.duration_min)
    sink.close()
    elapsed = time.monotonic() - t0

    report = {
        "mode": a.mode, "sink": a.sink, "devices": a.devices, "start": start.isoformat(), "days": a.days,
        "seed": a.seed, "faults": vars(faults), "events_generated": sum(sim.events_by_type.values()),
        "events_by_type": dict(sorted(sim.events_by_type.items())), "messages_sent": sent,
        "faults_injected": dict(sorted(sim.faults_injected.items())),
        "upgrading_devices": sum(d.upgrade_at is not None for d in fleet),
        "bad_lte_devices": sum(d.bad_lte for d in fleet),
        "runtime_s": round(elapsed, 2), "messages_per_sec": round(sent / elapsed, 1) if elapsed else None,
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    out_dir = a.data_dir / "run_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"simulator_{a.mode}.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({k: report[k] for k in ("mode", "messages_sent", "events_by_type", "faults_injected",
                                             "runtime_s", "messages_per_sec")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

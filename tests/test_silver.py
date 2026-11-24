import hashlib
import json
from datetime import date

import pytest

from spark_jobs.bronze_ingest import run as bronze_run
from spark_jobs.silver_transform import transform_date
from tests.sparkhelpers import heartbeat, record, uid, write_landing

D = date(2026, 1, 2)


def lte(event_id, event_ts, **overrides):
    event = {"event_id": event_id, "device_id": "dev-000002", "event_type": "lte_sample", "event_ts": event_ts,
             "firmware_version": "1.1.0", "rsrp_dbm": -90.0, "sinr_db": 10.0, "registered": True, "ip_assigned": True}
    event.update(overrides)
    return event


@pytest.fixture()
def bronze(spark, lake):
    recs = [
        record(heartbeat(uid(1), "2026-01-02T10:00:00.100Z"), "2026-01-02T10:00:01", 1),
        record(heartbeat(uid(1), "2026-01-02T10:00:00.100Z"), "2026-01-02T10:00:40", 2),  # duplicate
        record(heartbeat(uid(2), "2026-01-02T23:50:00.000Z"), "2026-01-03T02:00:00", 3),  # late, next-day bronze
        record(heartbeat(uid(3), "1970-01-01T03:00:00.000Z"), "2026-01-02T12:00:00", 4),  # clock in 1970
        record(heartbeat(uid(4), "2026-04-12T12:00:00.000Z"), "2026-01-02T12:00:00", 5),  # clock in the future
        record(heartbeat(uid(5), "2026-01-02T12:00:00.000Z", temp_c=500.0), "2026-01-02T12:00:01", 6),
        record(heartbeat(uid(6), "2026-01-02T12:00:00.000Z", cpu_pct="abc"), "2026-01-02T12:00:01", 7),
        record({k: v for k, v in lte(uid(7), "2026-01-02T12:00:00.000Z").items() if k != "sinr_db"},
               "2026-01-02T12:00:01", 8),
        record(lte(uid(8), "2026-01-02T12:00:00.000Z"), "2026-01-02T12:00:01", 9),
        record({**heartbeat(uid(9), "2026-01-02T12:00:00.000Z"), "event_type": "teleport"}, "2026-01-02T12:00:01", 10),
        record(heartbeat(uid(10), "2026-01-01T20:00:00.000Z"), "2026-01-03T01:00:00", 11),  # too late for D-1
        record(heartbeat(uid(11), "2026-01-03T09:00:00.000Z"), "2026-01-03T09:00:01", 12),  # belongs to D+1
        record(heartbeat(uid(12), "2026-01-02T00:00:00.000Z"), "2026-01-02T00:00:01", 13),
        record('{"event_id":"x","event_type":"heart', "2026-01-02T13:00:00", 14),  # malformed -> quarantine
    ]
    write_landing(lake.landing / "telemetry.raw", recs)
    bronze_run(spark, source="files", p=lake)
    return lake


def silver_ids(spark, lake, event_type, d=D):
    path = lake.silver / event_type / f"event_date={d.isoformat()}"
    if not path.exists():
        return set()
    return {r["event_id"] for r in spark.read.parquet(str(path)).collect()}


def test_dedupe_late_events_clock_and_schema_rejects(spark, bronze):
    report = transform_date(spark, D, p=bronze)
    assert silver_ids(spark, bronze, "heartbeat") == {uid(1), uid(2), uid(12)}
    assert silver_ids(spark, bronze, "lte_sample") == {uid(8)}
    assert report["duplicates_removed"] == 1
    assert report["late_events_recovered_from_next_day"] == 1
    assert report["late_events"] == 1
    assert report["too_late_dropped"] == 1
    assert report["rejects_by_reason"] == {"clock_before_2020": 1, "clock_in_future": 1,
                                           "schema_violation": 3, "unknown_event_type": 1}
    rejects = {r["event_id"]: r for r in spark.read.parquet(str(bronze.rejects)).collect()}
    assert rejects[uid(5)]["reject_detail"] == "temp_c:maximum"
    assert rejects[uid(6)]["reject_detail"] == "cpu_pct:required"  # wrong type parses to null
    assert rejects[uid(7)]["reject_detail"] == "sinr_db:required"
    assert report["rows_out"] == 4
    assert json.loads((bronze.run_reports / "silver_2026-01-02.json").read_text())["rows_in"] == 11


def test_rerun_is_byte_identical_and_only_touches_its_date(spark, bronze):
    def checksums():
        return {str(p.relative_to(bronze.silver)).split("/part-")[0]: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in bronze.silver.rglob("*.parquet")}

    transform_date(spark, D, p=bronze)
    transform_date(spark, date(2026, 1, 3), p=bronze)
    first = checksums()
    report = {k: v for k, v in json.loads((bronze.run_reports / "silver_2026-01-02.json").read_text()).items()
              if k != "runtime_s"}
    assert "heartbeat/event_date=2026-01-03" in first

    transform_date(spark, D, p=bronze)  # re-run D only
    assert checksums() == first
    again = {k: v for k, v in json.loads((bronze.run_reports / "silver_2026-01-02.json").read_text()).items()
             if k != "runtime_s"}
    assert again == report
    assert silver_ids(spark, bronze, "heartbeat", date(2026, 1, 3)) == {uid(11)}

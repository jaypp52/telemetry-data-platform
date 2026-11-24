import json
from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest
from jsonschema import Draft202012Validator

from simulator.devices import NEW_FIRMWARE, build_fleet, rollout_for, write_reference_files
from simulator.events import EVENT_TYPES, FaultConfig, FleetSimulator, in_dead_zone
from simulator.produce import JsonlSink, run_historical
from spark_jobs.common import json_schema

START = datetime(2026, 1, 1, tzinfo=UTC)
VALIDATORS = {et: Draft202012Validator(json_schema(et)) for et in EVENT_TYPES}


def simulate(devices=40, days=2, faults=None, seed=7):
    fleet = build_fleet(devices, START, days, seed)
    sim = FleetSimulator(fleet, faults if faults is not None else FaultConfig(), seed)
    window, end = START, START + timedelta(days=days)
    msgs = []
    while window < end:
        sim.advance(window, window + timedelta(hours=1))
        window += timedelta(hours=1)
        msgs += sim.drain(until=window)
    msgs += sim.drain()
    return fleet, sim, msgs


@pytest.fixture(scope="module")
def faulty():
    return simulate(devices=120, days=2)


def test_every_clean_event_matches_its_json_schema():
    _, sim, msgs = simulate(devices=30, days=1, faults=FaultConfig.disabled())
    assert len(msgs) == sum(sim.events_by_type.values()) > 1000
    for m in msgs:
        event = json.loads(m.value)
        errors = list(VALIDATORS[event["event_type"]].iter_errors(event))
        assert not errors, (event, errors[0].message)
    assert set(sim.events_by_type) == set(EVENT_TYPES)


def test_fault_rates_are_within_tolerance(faulty):
    _, sim, msgs = faulty
    events = sum(sim.events_by_type.values())
    f = sim.faults_injected
    assert f["duplicate"] / events == pytest.approx(0.005, abs=0.0015)
    assert f["late"] / events == pytest.approx(0.01, abs=0.002)
    assert f["clock_error"] / events == pytest.approx(0.002, abs=0.0008)
    assert 0 < f["malformed"] / events < 0.0005
    assert len(msgs) == events + f["duplicate"]


def test_faults_show_up_in_the_delivered_messages(faulty):
    _, sim, msgs = faulty
    malformed, ids, late, clock = 0, Counter(), 0, 0
    for m in msgs:
        try:
            event = json.loads(m.value)
        except json.JSONDecodeError:
            malformed += 1
            continue
        ids[event["event_id"]] += 1
        ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
        if ts.year < 2020 or ts > m.arrival + timedelta(minutes=10):
            clock += 1
        elif m.arrival - ts > timedelta(minutes=10):
            late += 1
    assert malformed >= sim.faults_injected["malformed"]  # a malformed original also breaks its duplicate
    assert sum(1 for n in ids.values() if n > 1) > 0.8 * sim.faults_injected["duplicate"]
    assert clock > 0.9 * sim.faults_injected["clock_error"]
    assert late > 0.9 * sim.faults_injected["late"]
    late_hours = [(m.arrival - datetime.fromisoformat(json.loads(m.value)["event_ts"].replace("Z", "+00:00")))
                  for m in msgs if m.value.endswith("}")]
    assert max(d for d in late_hours if d < timedelta(days=1)) <= timedelta(hours=6, seconds=150)


def test_messages_are_released_in_arrival_order(faulty):
    _, _, msgs = faulty
    assert all(a.arrival <= b.arrival for a, b in zip(msgs, msgs[1:], strict=False))


def test_simulation_is_deterministic():
    _, _, a = simulate(devices=10, days=1)
    _, _, b = simulate(devices=10, days=1)
    assert [(m.arrival, m.value) for m in a] == [(m.arrival, m.value) for m in b]


def test_delivery_faults_do_not_change_which_events_happen():
    delivery_off = FaultConfig(duplicate_rate=0, late_rate=0, clock_error_rate=0, malformed_rate=0)
    _, _, clean = simulate(devices=15, days=1, faults=delivery_off)
    _, _, noisy = simulate(devices=15, days=1)
    noisy_ids = {json.loads(m.value)["event_id"] for m in noisy if m.value.endswith("}")}
    clean_ids = {json.loads(m.value)["event_id"] for m in clean}
    assert noisy_ids <= clean_ids and len(clean_ids - noisy_ids) < 0.001 * len(clean_ids) + 3


def test_live_sessions_never_reuse_historical_event_ids():
    fleet = build_fleet(20, START, 2, 7)
    ids = []
    for stream in ("historical", "live-20261123T100000", "live-20261123T110000"):
        sim = FleetSimulator(fleet, FaultConfig.disabled(), 7, stream)
        sim.advance(START, START + timedelta(hours=12))
        ids.append({json.loads(m.value)["event_id"] for m in sim.drain()})
    assert all(ids) and not (ids[0] & ids[1]) and not (ids[0] & ids[2]) and not (ids[1] & ids[2])


def test_no_faults_flag_disables_everything():
    _, sim, msgs = simulate(devices=20, days=1, faults=FaultConfig.disabled())
    assert sum(sim.faults_injected.values()) == 0
    assert len({json.loads(m.value)["event_id"] for m in msgs}) == len(msgs)


def test_firmware_rollout_is_staggered_inside_the_window():
    fleet = build_fleet(500, START, 7, seed=42)
    rollout = rollout_for(START, 7)
    upgrades = [d.upgrade_at for d in fleet if d.upgrade_at]
    assert 0.78 < len(upgrades) / len(fleet) < 0.92
    assert all(rollout.start <= u <= rollout.end for u in upgrades)
    assert len({u.date() for u in upgrades}) >= 4  # spread over several days
    d = next(d for d in fleet if d.upgrade_at)
    assert d.firmware_at(d.upgrade_at - timedelta(seconds=1)) == "1.0.0"
    assert d.firmware_at(d.upgrade_at) == NEW_FIRMWARE


def test_bad_lte_fraction_and_crash_multiplier_flags():
    fleet = build_fleet(500, START, 7, seed=42, bad_lte_fraction=0.03)
    assert sum(d.bad_lte for d in fleet) == 15
    assert not any(d.bad_lte for d in build_fleet(500, START, 7, seed=42, bad_lte_fraction=0))

    def crashes_per_minute(faults):
        _, sim, msgs = simulate(devices=60, days=3, faults=faults, seed=3)
        by_model = Counter()
        minutes = Counter()
        model = {d.device_id: d.hardware_model for d in build_fleet(60, START, 3, 3)}
        for m in msgs:
            e = json.loads(m.value) if m.value.endswith("}") else None
            if not e or e["firmware_version"] != NEW_FIRMWARE:
                continue
            if e["event_type"] == "heartbeat":
                minutes[model[e["device_id"]]] += 1
            elif e.get("event_code") == "CAM_CRASH":
                by_model[model[e["device_id"]]] += 1
        return {k: by_model[k] / minutes[k] for k in minutes}

    rates = crashes_per_minute(FaultConfig(crash_multiplier=25))
    assert rates["TX-200"] > 5 * max(rates["TX-100"], rates["TX-300"])


def test_dead_zone_drops_lte_for_everyone_inside_it():
    _, _, msgs = simulate(devices=100, days=2, faults=FaultConfig(bad_lte_fraction=0))
    events = [json.loads(m.value) for m in msgs if m.value.endswith("}")]
    gps = {(e["device_id"], e["event_ts"][:16]): in_dead_zone(e["lat"], e["lon"])
           for e in events if e["event_type"] == "gps_fix"}
    inside = [e["registered"] for e in events
              if e["event_type"] == "lte_sample" and gps.get((e["device_id"], e["event_ts"][:16]))]
    outside = [e["registered"] for e in events
               if e["event_type"] == "lte_sample" and gps.get((e["device_id"], e["event_ts"][:16])) is False]
    assert len(inside) > 50
    assert sum(inside) / len(inside) < 0.2
    assert sum(outside) / len(outside) > 0.95


def test_jsonl_sink_and_reference_files(tmp_path):
    fleet = build_fleet(5, START, 1, 1)
    write_reference_files(fleet, rollout_for(START, 1), tmp_path / "ref")
    registry = (tmp_path / "ref" / "fleet_registry.csv").read_text().splitlines()
    assert len(registry) == 6 and registry[0].endswith("factory_firmware_version")
    assert "1.1.0" in (tmp_path / "ref" / "firmware_releases.csv").read_text()

    sink = JsonlSink(tmp_path / "landing", "t", batch_size=100)
    sent = run_historical(FleetSimulator(fleet, FaultConfig(), 1), sink, START, 0.5)
    sink.close()
    lines = [json.loads(line) for f in sorted((tmp_path / "landing").glob("part-*.jsonl"))
             for line in f.read_text().splitlines()]
    assert len(lines) == sent > 0
    assert {r["topic"] for r in lines} == {"t"}
    assert len({(r["partition"], r["offset"]) for r in lines}) == sent  # offsets unique per partition

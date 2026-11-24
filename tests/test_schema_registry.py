from simulator.events import EVENT_CODES
from simulator.events import EVENT_TYPES as SIM_EVENT_TYPES
from spark_jobs.common import EVENT_TYPES, json_schema, payload_fields, validation_rules


def test_registry_covers_every_simulated_event_type():
    assert set(EVENT_TYPES) == set(SIM_EVENT_TYPES)


def test_payload_fields_exclude_envelope():
    assert payload_fields("heartbeat") == ["cpu_pct", "mem_free_mb", "temp_c", "storage_free_pct"]
    assert "event_id" not in payload_fields("gps_fix")


def test_rules_come_from_the_json_schema():
    rules = validation_rules("heartbeat")
    assert ("temp_c", "minimum", -40) in rules and ("temp_c", "maximum", 125) in rules
    assert ("event_id", "required", None) in rules
    enum = dict(((f, r), a) for f, r, a in validation_rules("device_event"))[("event_code", "enum")]
    assert set(enum) == set(EVENT_CODES)


def test_every_schema_forbids_unknown_fields():
    assert all(json_schema(et)["additionalProperties"] is False for et in EVENT_TYPES)

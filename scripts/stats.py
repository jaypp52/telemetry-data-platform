"""Pipeline statistics: the only source of the numbers in the README.

    python scripts/stats.py [--date D]          # collect -> data/stats/stats.json
    python scripts/stats.py --readme            # ...and rewrite the README stats section
    python scripts/stats.py --snapshot NAME     # save a fingerprint of every output
    python scripts/stats.py --compare A B       # are two fingerprints identical? (exit 1 if not)

A fingerprint holds row counts and checksums for bronze, silver and rejects (SHA-256 of
each committed Parquet file's pages and footer, see ``parquet_digest``) and for every
warehouse table (an order-independent hash of all rows). Two backfills are idempotent when their
fingerprints match exactly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import sys
from datetime import UTC, datetime
from pathlib import Path

DATA = Path(os.environ.get("DATA_DIR", "/opt/data"))
ROOT = Path(__file__).resolve().parents[1]
STATS = DATA / "stats"
LAKE = DATA / "lake"
WAREHOUSE = DATA / "warehouse.duckdb"
README = ROOT / "README.md"
START, END = "<!-- stats:start -->", "<!-- stats:end -->"
EVENT_TYPES = ("heartbeat", "lte_sample", "gps_fix", "trip_start", "trip_end", "device_event")


def load(path: Path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def parquet_digest(path: Path) -> str:
    """SHA-256 of a Parquet file's data pages plus its footer, with one field normalised.

    parquet-mr (Spark's writer) lists each column chunk's encodings from a Java hash set,
    whose iteration order changes from one JVM to the next. Everything else is compared
    byte for byte: every data and dictionary page, the schema, statistics, offsets and
    the key-value metadata.
    """
    import pyarrow.parquet as pq

    data = path.read_bytes()
    footer_len = struct.unpack("<I", data[-8:-4])[0]
    h = hashlib.sha256(data[: len(data) - 8 - footer_len])  # magic + all pages
    meta = pq.ParquetFile(path).metadata
    footer = meta.to_dict()
    for rg in footer["row_groups"]:
        for col in rg["columns"]:
            col["encodings"] = sorted(col["encodings"])
    h.update(json.dumps(footer, sort_keys=True, default=str).encode())
    h.update(json.dumps(sorted((k.decode(), v.decode()) for k, v in (meta.metadata or {}).items())).encode())
    return h.hexdigest()


def committed_files(table_dir: Path) -> list[Path]:
    """Files of a Structured Streaming file sink, read from its _spark_metadata commit log."""
    log_dir = table_dir / "_spark_metadata"
    if not log_dir.exists():
        return sorted(table_dir.rglob("*.parquet"))
    def batch_id(p: Path) -> int:
        stem = p.name.split(".")[0]
        return int(stem) if stem.isdigit() else -1

    entries = sorted(log_dir.iterdir(), key=batch_id)
    # The newest .compact file contains everything before it; replay from there.
    compacts = [e for e in entries if e.name.endswith(".compact")]
    if compacts:
        entries = entries[entries.index(compacts[-1]):]
    files: dict[str, bool] = {}
    for entry in entries:
        if not entry.name.split(".")[0].isdigit():
            continue
        for line in entry.read_text().splitlines()[1:]:  # first line is the format version
            rec = json.loads(line)
            files[rec["path"]] = rec.get("action", "add") == "add"
    return sorted(Path(p.removeprefix("file://")) for p, keep in files.items() if keep)


def partition_of(path: Path) -> str:
    return "/".join(part for part in path.parts if "=" in part)


def lake_section(files: list[Path]) -> dict:
    return {"files": len(files), "partitions": len({partition_of(f) for f in files}),
            "bytes": sum(f.stat().st_size for f in files)}


def parquet_rows(con, files: list[Path]) -> int:
    if not files:
        return 0
    paths = ", ".join(f"'{f}'" for f in files)
    return con.execute(f"select count(*) from read_parquet([{paths}])").fetchone()[0]


def lake_files() -> dict[str, list[Path]]:
    out = {"bronze": committed_files(LAKE / "bronze" / "telemetry"),
           "quarantine": committed_files(LAKE / "bronze" / "_quarantine"),
           "rejects": sorted((LAKE / "silver" / "_rejects").rglob("*.parquet"))}
    for et in EVENT_TYPES:
        out[f"silver/{et}"] = sorted((LAKE / "silver" / et).rglob("*.parquet"))
    return out


def warehouse_tables(con) -> list[tuple[str, str]]:
    return con.execute(
        "select table_schema, table_name from information_schema.tables "
        "where table_schema in ('marts', 'snapshots') and table_type = 'BASE TABLE' order by 1, 2"
    ).fetchall()


def fingerprint() -> dict:
    import duckdb

    fp: dict = {"lake": {}, "warehouse": {}, "silver_reports": {}}
    con = duckdb.connect()
    for name, files in lake_files().items():
        by_partition: dict[str, list[str]] = {}
        for f in files:  # file names contain random UUIDs, so compare contents per partition
            by_partition.setdefault(partition_of(f), []).append(parquet_digest(f))
        fp["lake"][name] = {"rows": parquet_rows(con, files),
                            "files": {k: sorted(v) for k, v in sorted(by_partition.items())}}
    if WAREHOUSE.exists():
        wh = duckdb.connect(str(WAREHOUSE), read_only=True)
        for schema, table in warehouse_tables(wh):
            rows, digest = wh.execute(
                f"select count(*), md5(coalesce(string_agg(h, '' order by h), '')) "
                f"from (select md5(cast(t as varchar)) as h from {schema}.{table} as t)"
            ).fetchone()
            fp["warehouse"][f"{schema}.{table}"] = {"rows": rows, "md5": digest}
        wh.close()
    for report in sorted((DATA / "run_reports").glob("silver_*.json")):
        r = load(report, {})
        r.pop("runtime_s", None)
        fp["silver_reports"][report.stem] = r
    return fp


def compare(a: dict, b: dict) -> list[str]:
    diffs = []
    for section in ("lake", "warehouse", "silver_reports"):
        keys = sorted(set(a.get(section, {})) | set(b.get(section, {})))
        for k in keys:
            if a.get(section, {}).get(k) != b.get(section, {}).get(k):
                diffs.append(f"{section}:{k}")
    return diffs


def dbt_section() -> dict:
    manifest = load(STATS / "dbt_manifest.json") or load(ROOT / "dbt" / "target" / "manifest.json")
    results = load(STATS / "dbt_build_results.json")
    out: dict = {}
    if manifest:
        nodes = [n for n in manifest["nodes"].values() if n.get("package_name") == "telemetry"]
        count = lambda t: sum(n["resource_type"] == t for n in nodes)  # noqa: E731
        out.update(models=count("model"), tests=count("test"), snapshots=count("snapshot"), seeds=count("seed"),
                   sources=sum(s.get("package_name") == "telemetry" for s in manifest["sources"].values()))
    if results:
        statuses: dict[str, int] = {}
        for r in results["results"]:
            statuses[r["status"]] = statuses.get(r["status"], 0) + 1
        out["last_build"] = {"statuses": statuses, "elapsed_s": round(results.get("elapsed_time", 0), 1),
                             "generated_at": results["metadata"]["generated_at"]}
        out["all_passing"] = set(statuses) <= {"success", "pass"}
    return out


def collect(run_date: str | None = None) -> dict:
    import duckdb

    con = duckdb.connect()
    files = lake_files()
    silver_files = [f for k, v in files.items() if k.startswith("silver/") for f in v]
    reports = [load(p, {}) for p in sorted((DATA / "run_reports").glob("silver_*.json"))]
    reports = [r for r in reports if r.get("rows_in")]
    rejects_by_reason: dict[str, int] = {}
    for r in reports:
        for reason, n in r.get("rejects_by_reason", {}).items():
            rejects_by_reason[reason] = rejects_by_reason.get(reason, 0) + n
    total = lambda key: sum(r.get(key, 0) for r in reports)  # noqa: E731

    sim = load(DATA / "run_reports" / "simulator_historical.json") or {}
    live = load(DATA / "run_reports" / "simulator_live.json") or {}
    stats: dict = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_date": run_date,
        "fleet": {
            "devices": sim.get("devices"), "days": sim.get("days"), "start": sim.get("start"),
            "events_generated": sim.get("events_generated"), "messages_sent": sim.get("messages_sent"),
            "events_by_type": sim.get("events_by_type"), "faults_injected": sim.get("faults_injected"),
            "upgrading_devices": sim.get("upgrading_devices"), "bad_lte_devices": sim.get("bad_lte_devices"),
            "historical_messages_per_sec": sim.get("messages_per_sec"),
            "live_events_per_sec": live.get("messages_per_sec"), "live_runtime_s": live.get("runtime_s"),
        },
        "pipeline": {
            "bronze_rows": parquet_rows(con, files["bronze"]),
            "quarantined_rows": parquet_rows(con, files["quarantine"]),
            "silver_rows": parquet_rows(con, silver_files),
            "silver_dates": [r["date"] for r in reports],
            "duplicates_removed": total("duplicates_removed"),
            "late_events": total("late_events"),
            "late_events_recovered_from_next_day": total("late_events_recovered_from_next_day"),
            "too_late_dropped": total("too_late_dropped"),
            "rejects": total("rejects"),
            "rejects_by_reason": dict(sorted(rejects_by_reason.items())),
            "silver_runtime_s_per_date": round(total("runtime_s") / len(reports), 1) if reports else None,
        },
        "lake": {"bronze": lake_section(files["bronze"]), "silver": lake_section(silver_files),
                 "rejects": lake_section(files["rejects"])},
        "warehouse": {},
        "dbt": dbt_section(),
        "backfill": load(STATS / "backfill_runs.json", {"runs": []}),
        "idempotency": load(STATS / "idempotency.json"),
        "resources": load(STATS / "resources.json"),
    }
    if WAREHOUSE.exists():
        wh = duckdb.connect(str(WAREHOUSE), read_only=True)
        for schema, table in warehouse_tables(wh):
            stats["warehouse"][f"{schema}.{table}"] = wh.execute(f"select count(*) from {schema}.{table}").fetchone()[0]
        stats["warehouse_bytes"] = WAREHOUSE.stat().st_size
        firmware = wh.execute(
            "select firmware_version, hardware_model, round(100.0 * sum(crash_count) / sum(device_days), 2) "
            "from marts.fct_firmware_daily group by 1, 2 order by 2, 1"
        ).fetchall() if ("marts", "fct_firmware_daily") in warehouse_tables(wh) else []
        stats["crash_rate_per_100_device_days"] = [
            {"firmware_version": f, "hardware_model": m, "rate": r} for f, m, r in firmware]
        wh.close()
    return stats


def fmt(n) -> str:
    if n is None:
        return "not measured yet"
    if isinstance(n, bool):
        return "yes" if n else "no"
    if isinstance(n, float):
        return f"{n:,.1f}"
    if isinstance(n, int):
        return f"{n:,}"
    return str(n)


def readme_table(s: dict) -> str:
    f, p, d, b = s["fleet"], s["pipeline"], s["dbt"], s.get("backfill") or {}
    runs = b.get("runs", [])
    ok_runs = [r for r in runs if r.get("exit_code") == 0]
    first = round(ok_runs[0]["runtime_s"] / 60, 1) if ok_runs else None
    second = round(ok_runs[1]["runtime_s"] / 60, 1) if len(ok_runs) > 1 else None
    idem = s.get("idempotency") or {}
    res = s.get("resources") or {}
    build = d.get("last_build", {}).get("statuses", {})
    rows = [
        ("Simulated fleet",
         f"{fmt(f.get('devices'))} devices, {fmt(f.get('days'))} days from {str(f.get('start'))[:10]}"),
        ("Events generated / messages sent", f"{fmt(f.get('events_generated'))} / {fmt(f.get('messages_sent'))}"),
        ("Simulator throughput (historical)", f"{fmt(f.get('historical_messages_per_sec'))} msg/s"),
        ("Simulator rate (live mode)", f"{fmt(f.get('live_events_per_sec'))} events/s"),
        ("Faults injected", ", ".join(f"{k} {fmt(v)}" for k, v in (f.get("faults_injected") or {}).items()) or "-"),
        ("Bronze rows (quarantined malformed)", f"{fmt(p['bronze_rows'])} ({fmt(p['quarantined_rows'])})"),
        ("Silver rows", fmt(p["silver_rows"])),
        ("Duplicates removed", fmt(p["duplicates_removed"])),
        ("Late events (recovered from the next day's bronze)",
         f"{fmt(p['late_events'])} ({fmt(p['late_events_recovered_from_next_day'])})"),
        ("Rejects by reason", ", ".join(f"{k} {fmt(v)}" for k, v in p["rejects_by_reason"].items()) or "-"),
        ("dbt project", f"{fmt(d.get('models'))} models, {fmt(d.get('snapshots'))} snapshot, "
                        f"{fmt(d.get('seeds'))} seeds, {fmt(d.get('tests'))} tests"),
        ("Last `dbt build`", ", ".join(f"{k} {v}" for k, v in sorted(build.items())) or "not run yet"),
        ("Warehouse rows: fct_device_minute / fct_device_daily",
         f"{fmt(s['warehouse'].get('marts.fct_device_minute'))} / {fmt(s['warehouse'].get('marts.fct_device_daily'))}"),
        ("dim_device rows (SCD2 versions)", fmt(s["warehouse"].get("marts.dim_device"))),
        ("7-day backfill runtime: 1st / 2nd run", f"{fmt(first)} min / {fmt(second)} min"),
        ("Second backfill identical (row counts + checksums)",
         fmt(idem.get("identical")) if idem else "not measured yet"),
        ("Peak container memory during the backfill",
         f"{fmt(res.get('peak_total_mib'))} MiB" if res else "not measured yet"),
    ]
    lines = ["| Metric | Value |", "|---|---|", *[f"| {k} | {v} |" for k, v in rows]]
    if s.get("crash_rate_per_100_device_days"):
        lines += ["", "CAM_CRASH rate per 100 device-days (from `marts.fct_firmware_daily`):", "",
                  "| Hardware | Firmware | Crashes / 100 device-days |", "|---|---|---|"]
        lines += [f"| {r['hardware_model']} | {r['firmware_version']} | {r['rate']} |"
                  for r in s["crash_rate_per_100_device_days"]]
    lines += ["", f"_Generated by `make stats` at {s['generated_at']} from `data/stats/stats.json`._"]
    return "\n".join(lines)


def update_readme(table: str, readme: Path = README) -> bool:
    text = readme.read_text()
    if START not in text or END not in text:
        return False
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    readme.write_text(f"{head}{START}\n{table}\n{END}{tail}")
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="logical date of the pipeline run (for the record)")
    ap.add_argument("--readme", action="store_true", help="rewrite the README stats section")
    ap.add_argument("--snapshot", metavar="NAME", help="save a fingerprint as data/stats/fingerprints/NAME.json")
    ap.add_argument("--compare", nargs=2, metavar=("A", "B"), help="compare two saved fingerprints")
    a = ap.parse_args(argv)
    STATS.mkdir(parents=True, exist_ok=True)

    if a.snapshot:
        out = STATS / "fingerprints" / f"{a.snapshot}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(fingerprint(), indent=1, sort_keys=True))
        print(f"fingerprint written to {out}")
        return 0
    if a.compare:
        fa, fb = (load(STATS / "fingerprints" / f"{n}.json") for n in a.compare)
        if fa is None or fb is None:
            print("missing fingerprint", file=sys.stderr)
            return 2
        diffs = compare(fa, fb)
        result = {"compared": list(a.compare), "identical": not diffs, "differences": diffs,
                  "lake_files": sum(len(h) for v in fa["lake"].values() for h in v["files"].values()),
                  "warehouse_tables": len(fa["warehouse"]),
                  "checked_at": datetime.now(UTC).isoformat(timespec="seconds")}
        (STATS / "idempotency.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
        return 0 if not diffs else 1

    stats = collect(a.date)
    (STATS / "stats.json").write_text(json.dumps(stats, indent=2, default=str))
    print(json.dumps({"pipeline": stats["pipeline"], "dbt": stats["dbt"]}, indent=2, default=str))
    if a.readme:
        print("README updated" if update_readme(readme_table(stats)) else "README markers not found")
    return 0


if __name__ == "__main__":
    sys.exit(main())

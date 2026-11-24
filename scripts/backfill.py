"""Run a timed Airflow backfill of ``telemetry_backfill`` and fingerprint the result.

    python scripts/backfill.py --start 2026-01-01 --end 2026-01-07

Each call appends to ``data/stats/backfill_runs.json`` and saves fingerprint
``backfill_<n>``. From the second run on, the two latest fingerprints are compared and
the verdict is written to ``data/stats/idempotency.json``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stats  # noqa: E402

DAG_ID = "telemetry_backfill"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    a = ap.parse_args(argv)

    log_path = stats.STATS / "backfill_runs.json"
    stats.STATS.mkdir(parents=True, exist_ok=True)
    log = stats.load(log_path, {"runs": []})
    n = len(log["runs"]) + 1
    cmd = ["airflow", "dags", "backfill", "-s", a.start, "-e", a.end, DAG_ID, "--reset-dagruns", "-y"]
    print("+", " ".join(cmd), flush=True)
    t0 = time.monotonic()
    code = subprocess.call(cmd)
    runtime = round(time.monotonic() - t0, 1)
    log["runs"].append({"run": n, "start": a.start, "end": a.end, "runtime_s": runtime, "exit_code": code,
                        "finished_at": datetime.now(UTC).isoformat(timespec="seconds")})
    log_path.write_text(json.dumps(log, indent=2))
    print(f"backfill #{n} finished in {runtime / 60:.1f} min with exit code {code}")
    if code != 0:
        return code

    stats.main(["--snapshot", f"backfill_{n}"])
    ok_runs = [r["run"] for r in log["runs"] if r["exit_code"] == 0]
    if len(ok_runs) >= 2:
        return stats.main(["--compare", f"backfill_{ok_runs[-2]}", f"backfill_{ok_runs[-1]}"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

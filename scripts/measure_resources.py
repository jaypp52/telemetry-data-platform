"""Run a command and sample `docker stats` while it runs; keep the peak memory.

    python3 scripts/measure_resources.py -- docker compose exec ... python scripts/backfill.py ...

Writes data/stats/resources.json (peak MiB per container and peak total). Runs on the
host because it needs the Docker CLI; standard library only.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path

UNITS = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1 / 1024, "MB": 1, "GB": 1024}
OUT = Path(__file__).resolve().parents[1] / "data" / "stats" / "resources.json"


def mib(text: str) -> float:
    text = text.strip()
    for unit in sorted(UNITS, key=len, reverse=True):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * UNITS[unit]
    return 0.0


def sample() -> dict[str, float]:
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
                         capture_output=True, text=True, check=False).stdout
    usage = {}
    for line in out.splitlines():
        name, mem = line.split("\t")
        if "telemetry" in name or "kafka" in name or "airflow" in name:
            usage[name] = mib(mem.split("/")[0])
    return usage


def main() -> int:
    cmd = sys.argv[sys.argv.index("--") + 1:] if "--" in sys.argv else sys.argv[1:]
    peaks: dict[str, float] = {}
    peak_total = [0.0]
    done = threading.Event()

    def watch():
        while not done.is_set():
            usage = sample()
            for k, v in usage.items():
                peaks[k] = max(peaks.get(k, 0.0), v)
            peak_total[0] = max(peak_total[0], sum(usage.values()))
            done.wait(5)

    t = threading.Thread(target=watch, daemon=True)
    t.start()
    code = subprocess.call(cmd)
    done.set()
    t.join()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"command": " ".join(cmd), "peak_total_mib": round(peak_total[0]),
                               "peak_by_container_mib": {k: round(v) for k, v in sorted(peaks.items())},
                               "measured_at": datetime.now(UTC).isoformat(timespec="seconds")}, indent=2))
    return code


if __name__ == "__main__":
    sys.exit(main())

"""Fleet definition: which devices exist, what hardware they run, and when they upgrade.

Everything here is synthetic. The fleet is a generic set of connected in-vehicle
devices; names, models and regions are invented for this project.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

HARDWARE_MODELS = ("TX-100", "TX-200", "TX-300")
HARDWARE_WEIGHTS = (0.45, 0.35, 0.20)
# Free memory budget per model, in MB.
MODEL_MEMORY_MB = {"TX-100": 512, "TX-200": 1024, "TX-300": 2048}

# region_id -> (name, IANA timezone, home latitude, home longitude)
REGIONS = {
    "us-west": ("US West", "America/Los_Angeles", 37.77, -122.42),
    "us-east": ("US East", "America/New_York", 40.71, -74.00),
    "eu-central": ("EU Central", "Europe/Berlin", 50.11, 8.68),
    "in-west": ("India West", "Asia/Kolkata", 19.07, 72.88),
    "ap-southeast": ("AP Southeast", "Asia/Singapore", 1.35, 103.82),
}

OLD_FIRMWARE = "1.0.0"
NEW_FIRMWARE = "1.1.0"
OLD_FIRMWARE_RELEASED = date(2025, 6, 2)


@dataclass(frozen=True)
class Device:
    device_id: str
    hardware_model: str
    region_id: str
    install_date: date
    home_lat: float
    home_lon: float
    upgrade_at: datetime | None  # moment the device switches to NEW_FIRMWARE; None = never in this run
    bad_lte: bool  # one of the ~3% of devices with a poor modem / antenna

    @property
    def timezone(self) -> str:
        return REGIONS[self.region_id][1]

    def firmware_at(self, ts: datetime) -> str:
        return NEW_FIRMWARE if self.upgrade_at is not None and ts >= self.upgrade_at else OLD_FIRMWARE


@dataclass(frozen=True)
class Rollout:
    """Staggered OTA rollout of NEW_FIRMWARE across the simulated window."""

    start: datetime
    end: datetime
    coverage: float = 0.85  # share of the fleet that upgrades before the window ends


def rollout_for(start: datetime, days: float) -> Rollout:
    return Rollout(start=start + timedelta(days=days * 0.25), end=start + timedelta(days=days * 0.85))


def build_fleet(
    n_devices: int,
    start: datetime,
    days: float,
    seed: int = 42,
    bad_lte_fraction: float = 0.03,
) -> list[Device]:
    """Deterministic fleet: the same arguments always give the same devices."""
    rng = random.Random(f"fleet-{seed}")
    rollout = rollout_for(start, days)
    window_s = (rollout.end - rollout.start).total_seconds()
    region_ids = list(REGIONS)
    n_bad = round(n_devices * bad_lte_fraction)
    bad_ids = set(rng.sample(range(n_devices), n_bad)) if n_bad else set()

    fleet = []
    for i in range(n_devices):
        region_id = region_ids[i % len(region_ids)]
        _, _, lat, lon = REGIONS[region_id]
        upgrade_at = None
        if rng.random() < rollout.coverage:
            upgrade_at = rollout.start + timedelta(seconds=rng.uniform(0, window_s))
        fleet.append(
            Device(
                device_id=f"dev-{i + 1:06d}",
                hardware_model=rng.choices(HARDWARE_MODELS, HARDWARE_WEIGHTS)[0],
                region_id=region_id,
                install_date=date(2025, 1, 1) + timedelta(days=rng.randrange(0, 300)),
                home_lat=round(lat + rng.uniform(-0.25, 0.25), 6),
                home_lon=round(lon + rng.uniform(-0.25, 0.25), 6),
                upgrade_at=upgrade_at,
                bad_lte=i in bad_ids,
            )
        )
    return fleet


def write_reference_files(fleet: list[Device], rollout: Rollout, reference_dir: Path) -> None:
    """Master data that a provisioning / OTA system would export (not telemetry, so not on Kafka)."""
    reference_dir.mkdir(parents=True, exist_ok=True)
    with open(reference_dir / "fleet_registry.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["device_id", "hardware_model", "region_id", "install_date", "factory_firmware_version"])
        for d in fleet:
            w.writerow([d.device_id, d.hardware_model, d.region_id, d.install_date.isoformat(), OLD_FIRMWARE])

    releases_path = reference_dir / "firmware_releases.csv"
    releases = {OLD_FIRMWARE: OLD_FIRMWARE_RELEASED, NEW_FIRMWARE: rollout.start.date()}
    if releases_path.exists():  # keep the earliest known release date per version
        with open(releases_path) as f:
            for row in csv.DictReader(f):
                known = date.fromisoformat(row["release_date"])
                releases[row["firmware_version"]] = min(known, releases.get(row["firmware_version"], known))
    with open(releases_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["firmware_version", "release_date"])
        for version, released in sorted(releases.items()):
            w.writerow([version, released.isoformat()])


def utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)

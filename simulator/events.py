"""Event generators and fault injection.

Two random streams per device keep the simulation reproducible:

* the *data* stream decides trips, sensor readings and device events;
* the *fault* stream decides duplicates, late arrival, clock errors and malformed JSON.

Because they are separate, turning the data-quality faults off (``FaultConfig.disabled``)
changes how events are delivered but not which events happen.
"""

from __future__ import annotations

import heapq
import json
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from simulator.devices import MODEL_MEMORY_MB, NEW_FIRMWARE, Device

EVENT_TYPES = ("heartbeat", "lte_sample", "gps_fix", "trip_start", "trip_end", "device_event")

# lat_min, lat_max, lon_min, lon_max: an LTE coverage hole that drops every device inside it.
DEAD_ZONE = (37.62, 37.80, -122.30, -122.10)

EVENT_CODES = {  # code -> (severity, message)
    "CAM_CRASH": ("error", "camera pipeline exited unexpectedly"),
    "REBOOT": ("warning", "device rebooted"),
    "LOW_MEMORY": ("warning", "free memory below 25% of budget"),
    "OVERHEAT": ("critical", "board temperature above 85 C"),
    "FW_UPDATE": ("info", "firmware update applied"),
    "SD_CARD_ERROR": ("error", "storage write failed"),
}

BASE_CRASH_PER_MIN = 0.0004  # CAM_CRASH probability per drive minute before fault multipliers
SPONTANEOUS_REBOOT_PER_MIN = 0.00005
SD_ERROR_PER_MIN = 0.00002


@dataclass
class FaultConfig:
    """Every fault is a flag so it can be switched off or tuned from the CLI."""

    crash_model: str = "TX-200"  # hardware model whose crash rate rises on NEW_FIRMWARE
    crash_multiplier: float = 2.0
    bad_lte_fraction: float = 0.03
    dead_zone: bool = True
    duplicate_rate: float = 0.005
    late_rate: float = 0.01
    late_max_hours: float = 6.0
    clock_error_rate: float = 0.002
    malformed_rate: float = 0.0001

    @classmethod
    def disabled(cls) -> FaultConfig:
        return cls(crash_multiplier=1.0, bad_lte_fraction=0.0, dead_zone=False, duplicate_rate=0.0,
                   late_rate=0.0, clock_error_rate=0.0, malformed_rate=0.0)


@dataclass
class Trip:
    trip_id: str
    start: datetime  # UTC, aligned to a whole minute
    minutes: int

    @property
    def end(self) -> datetime:
        return self.start + timedelta(minutes=self.minutes)


@dataclass(order=True)
class Message:
    arrival: datetime
    seq: int
    key: str = field(compare=False)
    value: str = field(compare=False)


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def in_dead_zone(lat: float, lon: float) -> bool:
    lat_min, lat_max, lon_min, lon_max = DEAD_ZONE
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


class DeviceState:
    """Mutable per-device state that carries over from one minute to the next."""

    def __init__(self, device: Device, seed: int, stream: str = "historical"):
        self.device = device
        # `stream` separates simulation runs over the same fleet (historical vs each live
        # session): same devices, but never the same random draws, so never the same UUIDs.
        self.rng = random.Random(f"data-{seed}-{stream}-{device.device_id}")
        self.fault_rng = random.Random(f"fault-{seed}-{stream}-{device.device_id}")
        self.tz = ZoneInfo(device.timezone)
        self.lat, self.lon = device.home_lat, device.home_lon
        self.heading = self.rng.uniform(0, 360)
        self.speed = 0.0
        self.odometer = round(self.rng.uniform(5_000, 80_000), 1)
        self.storage = self.rng.uniform(40, 95)
        self.mem_budget = MODEL_MEMORY_MB[device.hardware_model]
        self.plans: dict[date, list[Trip]] = {}
        self.fw_update_reported = False
        self.trip_flags: dict[str, set[str]] = {}  # per-trip one-shot events (LOW_MEMORY, OVERHEAT)
        self.leak: dict[str, float] = {}  # per-trip memory leak, MB per minute
        self.hot_day: dict[date, bool] = {}

    def new_id(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def trips_for_local_day(self, day: date) -> list[Trip]:
        if day in self.plans:
            return self.plans[day]
        rng = self.rng
        n = rng.choices(range(6), (0.08, 0.12, 0.25, 0.25, 0.18, 0.12))[0]
        starts = sorted(self._trip_start_minute() for _ in range(n))
        trips, prev_end = [], -1
        for start_min in starts:
            minutes = int(min(150, max(5, rng.lognormvariate(math.log(28), 0.6))))
            start_min = max(start_min, prev_end + 10)
            if start_min + minutes > 23 * 60 + 50:
                continue
            local = datetime(day.year, day.month, day.day, tzinfo=self.tz) + timedelta(minutes=start_min)
            trips.append(Trip(self.new_id(), local.astimezone(UTC), minutes))
            prev_end = start_min + minutes
        self.plans[day] = trips
        self.hot_day[day] = rng.random() < 0.02
        return trips

    def _trip_start_minute(self) -> int:
        rng = self.rng
        peak = rng.choices((8, 13, 18, None), (0.35, 0.2, 0.35, 0.1))[0]
        if peak is None:
            return rng.randrange(6 * 60, 22 * 60)
        return int(max(5 * 60, min(22 * 60, rng.gauss(peak * 60, 50))))


class FleetSimulator:
    """Turns a fleet into events, window by window, in event-time order per device."""

    def __init__(self, fleet: list[Device], faults: FaultConfig | None = None, seed: int = 42,
                 stream: str = "historical"):
        self.faults = faults or FaultConfig()
        self.states = [DeviceState(d, seed, stream) for d in fleet]
        self.events_by_type: Counter[str] = Counter()
        self.faults_injected: Counter[str] = Counter()
        self._seq = 0
        self._pending: list[Message] = []

    # ---- public API ---------------------------------------------------------------------

    def advance(self, window_start: datetime, window_end: datetime) -> None:
        """Generate every event whose true timestamp is in [window_start, window_end)."""
        for st in self.states:
            for event, true_ts in self._device_events(st, window_start, window_end):
                self.events_by_type[event["event_type"]] += 1
                for msg in self._deliver(st, event, true_ts):
                    heapq.heappush(self._pending, msg)

    def drain(self, until: datetime | None = None) -> list[Message]:
        """Pop messages that have *arrived* at the broker before ``until`` (all if None)."""
        out = []
        while self._pending and (until is None or self._pending[0].arrival < until):
            out.append(heapq.heappop(self._pending))
        return out

    # ---- generation ---------------------------------------------------------------------

    def _device_events(self, st: DeviceState, ws: datetime, we: datetime):
        local_days = {ws.astimezone(st.tz).date(), (we - timedelta(microseconds=1)).astimezone(st.tz).date()}
        for day in sorted(local_days):
            for trip in st.trips_for_local_day(day):
                if trip.end < ws or trip.start >= we:
                    continue
                if ws <= trip.start < we:
                    yield self._envelope(st, "trip_start", trip.start, trip_id=trip.trip_id,
                                         odometer_km=round(st.odometer, 1)), trip.start
                first = max(0, math.ceil((ws - trip.start).total_seconds() / 60))
                last = min(trip.minutes, math.ceil((we - trip.start).total_seconds() / 60))
                for i in range(first, last):
                    yield from self._minute(st, trip, i, trip.start + timedelta(minutes=i))
                if ws <= trip.end < we:
                    yield self._envelope(st, "trip_end", trip.end, trip_id=trip.trip_id,
                                         odometer_km=round(st.odometer, 1)), trip.end

    def _minute(self, st: DeviceState, trip: Trip, i: int, minute: datetime):
        rng, dev, f = st.rng, st.device, self.faults
        # motion
        if i < 2 or i >= trip.minutes - 2:
            target = 20.0
        else:
            target = 55.0
        st.speed = min(130.0, max(0.0, st.speed + rng.gauss((target - st.speed) * 0.3, 8)))
        dist_km = st.speed / 60
        st.heading = (st.heading + rng.gauss(0, 15)) % 360
        if math.hypot(st.lat - dev.home_lat, st.lon - dev.home_lon) > 0.35:
            st.heading = math.degrees(math.atan2(dev.home_lon - st.lon, dev.home_lat - st.lat)) % 360
        st.lat += dist_km * math.cos(math.radians(st.heading)) / 111.0
        st.lon += dist_km * math.sin(math.radians(st.heading)) / (111.0 * max(0.2, math.cos(math.radians(st.lat))))
        st.odometer += dist_km
        st.storage -= 0.02
        if st.storage < 8:
            st.storage = 90.0  # loop recording frees space

        # heartbeat
        local = minute.astimezone(st.tz)
        ambient = 18 + 9 * math.sin((local.hour - 9) / 24 * 2 * math.pi)
        cpu = min(100.0, max(2.0, rng.gauss(35 + st.speed * 0.15, 10)))
        hot = 30 if st.hot_day.get(local.date()) else 0
        temp = min(125.0, max(-40.0, ambient + 18 + cpu * 0.25 + hot + rng.gauss(0, 2)))
        if trip.trip_id not in st.leak:  # first minute of the trip we simulate
            st.leak[trip.trip_id] = rng.uniform(4, 12) if rng.random() < 0.03 else 0.0
        mem = max(8.0, st.mem_budget * rng.uniform(0.35, 0.8) - st.leak[trip.trip_id] * i)
        hb_ts = minute + timedelta(seconds=rng.uniform(0, 3))
        yield self._envelope(st, "heartbeat", hb_ts, cpu_pct=round(cpu, 1), mem_free_mb=int(mem),
                             temp_c=round(temp, 1), storage_free_pct=round(st.storage, 1)), hb_ts

        # LTE
        dead = f.dead_zone and in_dead_zone(st.lat, st.lon)
        bad = dev.bad_lte and f.bad_lte_fraction > 0
        if dead:
            rsrp, sinr, registered = rng.gauss(-125, 4), rng.gauss(-10, 3), rng.random() < 0.05
        elif bad:
            rsrp, sinr, registered = rng.gauss(-110, 6), rng.gauss(-3, 4), rng.random() > 0.35
        else:
            rsrp, sinr, registered = rng.gauss(-88, 8), rng.gauss(13, 5), rng.random() > 0.01
        ip_assigned = registered and rng.random() > 0.005
        lte_ts = minute + timedelta(seconds=rng.uniform(15, 20))
        yield self._envelope(st, "lte_sample", lte_ts, rsrp_dbm=round(min(-44, max(-140, rsrp)), 1),
                             sinr_db=round(min(30, max(-20, sinr)), 1), registered=registered,
                             ip_assigned=ip_assigned), lte_ts

        # GPS
        satellites = max(0, min(16, int(rng.gauss(10, 2.5))))
        fix_valid = satellites >= 4 and rng.random() > 0.02
        gps_ts = minute + timedelta(seconds=rng.uniform(30, 35))
        yield self._envelope(st, "gps_fix", gps_ts, lat=round(st.lat, 6), lon=round(st.lon, 6),
                             speed_kmh=round(st.speed, 1), satellites=satellites, fix_valid=fix_valid), gps_ts

        # sporadic device events
        ev_ts = minute + timedelta(seconds=rng.uniform(40, 59))
        fw = dev.firmware_at(minute)
        if fw == NEW_FIRMWARE and not st.fw_update_reported:
            st.fw_update_reported = True
            yield self._device_event(st, "FW_UPDATE", minute + timedelta(seconds=rng.uniform(4, 9)))
        crash_p = BASE_CRASH_PER_MIN
        if dev.hardware_model == f.crash_model and fw == NEW_FIRMWARE:
            crash_p *= f.crash_multiplier
        r_crash, r_reboot_after, r_reboot, r_sd = rng.random(), rng.random(), rng.random(), rng.random()
        if r_crash < crash_p:
            yield self._device_event(st, "CAM_CRASH", ev_ts)
            if r_reboot_after < 0.5:
                yield self._device_event(st, "REBOOT", minute + timedelta(seconds=rng.uniform(60, 90)))
        elif r_reboot < SPONTANEOUS_REBOOT_PER_MIN:
            yield self._device_event(st, "REBOOT", ev_ts)
        if r_sd < SD_ERROR_PER_MIN:
            yield self._device_event(st, "SD_CARD_ERROR", ev_ts)
        flags = st.trip_flags.setdefault(trip.trip_id, set())
        if mem < st.mem_budget * 0.25 and "LOW_MEMORY" not in flags:
            flags.add("LOW_MEMORY")
            yield self._device_event(st, "LOW_MEMORY", ev_ts)
        if temp > 85 and "OVERHEAT" not in flags:
            flags.add("OVERHEAT")
            yield self._device_event(st, "OVERHEAT", ev_ts)
        if i == trip.minutes - 1:  # trip over: drop per-trip bookkeeping
            st.trip_flags.pop(trip.trip_id, None)
            st.leak.pop(trip.trip_id, None)

    def _device_event(self, st: DeviceState, code: str, ts: datetime):
        severity, message = EVENT_CODES[code]
        return self._envelope(st, "device_event", ts, event_code=code, severity=severity, message=message), ts

    @staticmethod
    def _envelope(st: DeviceState, event_type: str, ts: datetime, **fields) -> dict:
        return {"event_id": st.new_id(), "device_id": st.device.device_id, "event_type": event_type,
                "event_ts": iso(ts), "firmware_version": st.device.firmware_at(ts), **fields}

    # ---- delivery and data-quality faults ----------------------------------------------

    def _deliver(self, st: DeviceState, event: dict, true_ts: datetime) -> list[Message]:
        f, rng = self.faults, st.fault_rng
        # Draw every number every time so one fault's rate never shifts another's stream.
        r_clock, r_clock_kind, r_clock_amount = rng.random(), rng.random(), rng.random()
        r_late, r_delay, r_malformed, r_cut, r_dup, r_dup_delay = (rng.random() for _ in range(6))

        payload = event
        if r_clock < f.clock_error_rate:
            payload = dict(event)
            if r_clock_kind < 0.5:  # RTC reset: device thinks it is shortly after the epoch
                bad = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(hours=1 + r_clock_amount * 72)
            else:  # RTC far in the future
                bad = true_ts + timedelta(days=1 + r_clock_amount * 1000)
            payload["event_ts"] = iso(bad)
            self.faults_injected["clock_error"] += 1

        if r_late < f.late_rate:
            delay = timedelta(minutes=10) + timedelta(hours=f.late_max_hours - 10 / 60) * r_delay
            self.faults_injected["late"] += 1
        else:
            delay = timedelta(seconds=0.2 + 4.8 * r_delay)
        arrival = true_ts + delay

        value = json.dumps(payload, separators=(",", ":"))
        if r_malformed < f.malformed_rate:
            value = value[: 5 + int(r_cut * (len(value) - 10))]  # truncated mid-object
            self.faults_injected["malformed"] += 1

        msgs = [self._message(arrival, event["device_id"], value)]
        if r_dup < f.duplicate_rate:  # producer retry after a lost ack: same bytes, a bit later
            msgs.append(self._message(arrival + timedelta(seconds=1 + 119 * r_dup_delay), event["device_id"], value))
            self.faults_injected["duplicate"] += 1
        return msgs

    def _message(self, arrival: datetime, key: str, value: str) -> Message:
        self._seq += 1
        return Message(arrival, self._seq, key, value)

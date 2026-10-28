"""Silver layer: drop duplicates and apply a watermark to Bronze records.

Records are read in arrival order, in one pass, like a stream. For each record:
1. Duplicate? Keep the first copy and only count the rest.
2. Older than the watermark? Store it in late_arrivals (never dropped silently).
Everything else goes to Silver.

Watermark = highest event_ts seen so far minus the allowed lateness.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

DEFAULT_ALLOWED_LATENESS_SECONDS = 300.0
STREAMS = ("trips", "gps", "payments")


@dataclass
class SilverStats:
    """Counts for one stream. Every input record is counted exactly once."""

    stream: str
    input_count: int = 0
    silver: int = 0
    late: int = 0
    duplicates: int = 0

    @property
    def conserved(self) -> bool:
        """True when nothing was lost: input = silver + late + duplicates."""
        return self.input_count == self.silver + self.late + self.duplicates


@dataclass
class SilverResult:
    """The Silver table, the late_arrivals table and the counts."""

    silver: list[dict[str, object]]
    late_arrivals: list[dict[str, object]]
    stats: SilverStats


def parse_event_ts(value: object) -> datetime:
    """Parse ISO 8601 text into a timezone-aware UTC datetime (naive input is taken as UTC)."""
    moment = datetime.fromisoformat(str(value))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _dedupe_key(record: dict[str, object], stream: str, moment: datetime) -> tuple[object, ...]:
    if stream == "trips":
        return (record["trip_id"], record["event_type"])
    if stream == "payments":
        return (record["payment_id"],)
    return (record["driver_id"], moment)


def process(
    records: Iterable[dict[str, object]],
    stream: str,
    allowed_lateness_seconds: float = DEFAULT_ALLOWED_LATENESS_SECONDS,
) -> SilverResult:
    """Dedupe and apply the watermark to Bronze good records, given in arrival order."""
    if stream not in STREAMS:
        raise ValueError(f"unknown stream {stream!r}; use one of {sorted(STREAMS)}")
    if allowed_lateness_seconds < 0:
        raise ValueError("allowed_lateness_seconds must not be negative")

    tolerance = timedelta(seconds=allowed_lateness_seconds)
    stats = SilverStats(stream=stream)
    silver: list[dict[str, object]] = []
    late_arrivals: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    latest: datetime | None = None

    for record in records:
        stats.input_count += 1
        moment = parse_event_ts(record["event_ts"])
        key = _dedupe_key(record, stream, moment)
        if key in seen:
            stats.duplicates += 1
            continue
        seen.add(key)

        cleaned = {**record, "event_ts": moment.isoformat()}
        if latest is not None and moment < latest - tolerance:
            cleaned["_late_by_seconds"] = (latest - moment).total_seconds()
            cleaned["_watermark"] = (latest - tolerance).isoformat()
            late_arrivals.append(cleaned)
            stats.late += 1
            continue

        silver.append(cleaned)
        stats.silver += 1
        latest = moment if latest is None else max(latest, moment)

    return SilverResult(silver=silver, late_arrivals=late_arrivals, stats=stats)

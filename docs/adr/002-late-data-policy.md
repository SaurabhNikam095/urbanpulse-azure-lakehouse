# ADR-002: Late-data policy, watermark and duplicate handling

- **Status:** Accepted
- **Date:** 28 October 2026
- **Author:** Saurabh Nikam
- **Project:** UrbanPulse on Azure

## Context

Events can arrive late and can arrive more than once. The simulators inject both on purpose (see `simulators/chaos.py`): about 3% of events get an `event_ts` moved 60 to 600 seconds into the past, and about 2% are duplicated. Bad records are a separate case and are quarantined in Bronze with a reason.

The pipeline needs one clear rule for late events and one for duplicates, and the rules must be testable against the chaos manifest. ADR-001 left the late-data policy open ("drop, log or store").

The events have no `event_id`, so duplicates must be found with natural keys.

## Decision

| # | Decision |
|---|----------|
| 1 | **Store** late events in a separate `late_arrivals` table. They are never dropped silently. |
| 2 | **Watermark** = the highest `event_ts` seen so far minus **300 seconds** of allowed lateness. A record older than the watermark is late. |
| 3 | **Dedupe first, then check lateness.** The first copy is kept; later copies are only counted. |
| 4 | Dedupe keys: trips `(trip_id, event_type)`, payments `payment_id`, gps `(driver_id, event_ts)`. |
| 5 | Each `late_arrivals` row keeps the full record plus `_late_by_seconds` and `_watermark`. |

## What happens to each record

| Case | Result |
|------|--------|
| Duplicate key already seen | Dropped and counted |
| New, within 300 seconds of the highest `event_ts` | Silver |
| New, more than 300 seconds behind | `late_arrivals` |

Every input record ends up in exactly one place, so `silver + late + duplicates = input`. The tests check this for all three streams, and the duplicate count is checked against `manifest.duplicates`.

## Consequences

- **Good:** no silent data loss. Late data can be reprocessed later. The counts reconcile with the chaos manifest.
- **Trade-offs:** one more table to maintain. Late records are not in Silver, so anything reading Silver will not see them until they are reprocessed.
- **Risks:** a GPS ping with a shifted `event_ts` could in theory collide with another ping of the same driver (very unlikely with random delays). The set of seen keys grows without limit in this local version. The local logic reads records in arrival order, and moving it to Spark means revisiting the watermark and state handling.

## Alternatives considered

| Option | Why not chosen |
|--------|----------------|
| Drop late events | Silent data loss and nothing to audit |
| Log only | Auditable, but the data itself is lost |
| Allowed lateness of 0 seconds | Every small out-of-order event would be stored as late |
| Allowed lateness of 600 seconds | Would accept every chaos late event, so the late path would never be exercised |

300 seconds sits in the middle of the chaos delay range (60 to 600 seconds), so by design roughly half of the late events are accepted and half are stored as late. This value should be revisited with real arrival data.

## Still open

- A job that reprocesses `late_arrivals` into Silver is not built yet
- Moving the watermark and dedupe state to Spark, with a bounded state size

## Sources and evidence

- Code: `ingestion/silver.py` and `simulators/chaos.py`
- Tests: `tests/test_silver.py` (16 tests) and `tests/test_bronze.py` (27 tests), 99 passed in total
- Related: ADR-001, which listed the late-data policy as open

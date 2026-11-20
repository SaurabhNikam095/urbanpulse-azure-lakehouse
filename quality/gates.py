"""Quality gates: pure threshold checks shared by CI tests and the Databricks notebook.

The block between the gates:begin and gates:end markers is copied word for word into
databricks/quality_gates.py, because a notebook cannot import repo modules. A test fails
if the two copies differ.
"""

# --- gates:begin ---
THRESHOLDS = {
    "quarantine_rate_max": 0.02,  # quarantine rows / Bronze rows, per topic
    "key_null_rate_max": 0.0,  # null rate on key columns
    "fare_null_rate_max": 0.05,  # null fare on completed trips
    "duplicate_rate_max": 0.0,  # duplicate rows on the ADR-002 keys after Silver
}


def _rate(part, whole):
    return part / whole if whole else 0.0


def evaluate(metrics, thresholds=THRESHOLDS):
    """Return a list of breach messages. An empty list means every gate passed."""
    breaches = []

    for topic, m in metrics.get("quarantine", {}).items():
        rate = _rate(m["quarantine"], m["bronze"])
        limit = thresholds["quarantine_rate_max"]
        if rate > limit:
            breaches.append(f"quarantine rate {topic}: {rate:.2%} > {limit:.2%}")

    for name, m in metrics.get("key_nulls", {}).items():
        rate = _rate(m["nulls"], m["rows"])
        limit = thresholds["key_null_rate_max"]
        if rate > limit:
            breaches.append(f"null rate {name}: {rate:.2%} > {limit:.2%}")

    fare = metrics.get("fare_nulls")
    if fare:
        rate = _rate(fare["nulls"], fare["rows"])
        limit = thresholds["fare_null_rate_max"]
        if rate > limit:
            breaches.append(f"null fare on completed trips: {rate:.2%} > {limit:.2%}")

    for name, m in metrics.get("duplicates", {}).items():
        rate = _rate(m["rows"] - m["distinct_keys"], m["rows"])
        limit = thresholds["duplicate_rate_max"]
        if rate > limit:
            breaches.append(f"duplicate rate {name}: {rate:.2%} > {limit:.2%}")

    for topic, m in metrics.get("reconciliation", {}).items():
        kept = m["silver"] + m["late"] + m["quarantine"]
        if "dups" in m:
            gap = m["bronze"] - (kept + m["dups"])
            if gap != 0:
                breaches.append(f"reconciliation {topic}: {gap} rows unaccounted for")
        elif kept > m["bronze"]:
            breaches.append(f"reconciliation {topic}: output {kept} > Bronze {m['bronze']}")

    return breaches


def assert_gates(metrics, thresholds=THRESHOLDS):
    """Print every breach and raise, so the run fails. Print a pass line otherwise."""
    breaches = evaluate(metrics, thresholds)
    for breach in breaches:
        print("GATE BREACH:", breach)
    if breaches:
        raise RuntimeError(f"{len(breaches)} quality gate(s) breached")
    print("all quality gates passed")


# --- gates:end ---

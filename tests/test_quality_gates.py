from pathlib import Path

import pytest

from quality.gates import THRESHOLDS, assert_gates, evaluate

ROOT = Path(__file__).resolve().parents[1]


def _good():
    return {
        "quarantine": {"trips": {"bronze": 100, "quarantine": 2}},
        "key_nulls": {"silver.trips.trip_id": {"rows": 100, "nulls": 0}},
        "fare_nulls": {"rows": 100, "nulls": 5},
        "duplicates": {"silver.trips": {"rows": 10, "distinct_keys": 10}},
        "reconciliation": {
            "trips": {"bronze": 100, "silver": 50, "late": 20, "quarantine": 2, "dups": 28}
        },
    }


def test_good_metrics_pass():
    assert evaluate(_good()) == []


def test_quarantine_above_threshold_breaches():
    metrics = _good()
    metrics["quarantine"]["trips"]["quarantine"] = 3
    assert any("quarantine rate trips" in b for b in evaluate(metrics))


def test_key_null_breaches():
    metrics = _good()
    metrics["key_nulls"]["silver.trips.trip_id"]["nulls"] = 1
    assert any("null rate silver.trips.trip_id" in b for b in evaluate(metrics))


def test_fare_null_above_threshold_breaches():
    metrics = _good()
    metrics["fare_nulls"]["nulls"] = 6
    assert any("null fare" in b for b in evaluate(metrics))


def test_duplicates_breach():
    metrics = _good()
    metrics["duplicates"]["silver.trips"]["distinct_keys"] = 9
    assert any("duplicate rate silver.trips" in b for b in evaluate(metrics))


def test_reconciliation_with_dups_must_be_exact():
    metrics = _good()
    metrics["reconciliation"]["trips"]["dups"] = 27
    assert any("unaccounted" in b for b in evaluate(metrics))


def test_reconciliation_without_dups_output_cannot_exceed_input():
    metrics = _good()
    del metrics["reconciliation"]["trips"]["dups"]
    assert evaluate(metrics) == []
    metrics["reconciliation"]["trips"]["silver"] = 90
    assert any("output" in b for b in evaluate(metrics))


def test_zero_rows_do_not_divide_by_zero():
    metrics = {
        "quarantine": {"gps": {"bronze": 0, "quarantine": 0}},
        "key_nulls": {"x": {"rows": 0, "nulls": 0}},
        "fare_nulls": {"rows": 0, "nulls": 0},
        "duplicates": {"y": {"rows": 0, "distinct_keys": 0}},
    }
    assert evaluate(metrics) == []


def test_custom_thresholds_are_used():
    metrics = _good()
    metrics["quarantine"]["trips"]["quarantine"] = 3
    loose = {**THRESHOLDS, "quarantine_rate_max": 0.10}
    assert evaluate(metrics, loose) == []


def test_assert_gates_raises_on_breach(capsys):
    metrics = _good()
    metrics["key_nulls"]["silver.trips.trip_id"]["nulls"] = 5
    with pytest.raises(RuntimeError):
        assert_gates(metrics)
    assert "GATE BREACH" in capsys.readouterr().out


def test_assert_gates_passes_quietly(capsys):
    assert_gates(_good())
    assert "all quality gates passed" in capsys.readouterr().out


def _block(path):
    text = path.read_text(encoding="utf-8")
    return text.split("# --- gates:begin ---")[1].split("# --- gates:end ---")[0].strip()


def test_notebook_copy_matches_gates_module():
    assert _block(ROOT / "databricks" / "quality_gates.py") == _block(ROOT / "quality" / "gates.py")

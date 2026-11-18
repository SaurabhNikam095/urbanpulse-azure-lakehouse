import re

import pytest

from simulators.run_id import get_run_id, make_id


def test_format():
    assert re.fullmatch(r"TRP-[A-Za-z0-9]{4,16}-\d{8}", make_id("TRP", "RUN1", 1))
    assert make_id("PAY", "RUN1", 12) == "PAY-RUN1-00000012"


def test_two_runs_never_collide():
    a = {make_id("TRP", "RUNA", n) for n in range(1, 500)}
    b = {make_id("TRP", "RUNB", n) for n in range(1, 500)}
    assert a.isdisjoint(b)


def test_same_run_is_deterministic():
    assert [make_id("TRP", "RUN1", n) for n in range(1, 4)] == [
        make_id("TRP", "RUN1", n) for n in range(1, 4)
    ]


def test_duplicate_keeps_original_id():
    original = make_id("TRP", "RUN1", 7)
    chaos_copy = dict(trip_id=original)
    assert chaos_copy["trip_id"] == original


def test_env_override(monkeypatch):
    monkeypatch.setenv("SIM_RUN_ID", "ENVRUN")
    assert get_run_id() == "ENVRUN"
    assert get_run_id("ARG1") == "ARG1"


def test_default_is_timestamp(monkeypatch):
    monkeypatch.delenv("SIM_RUN_ID", raising=False)
    assert re.fullmatch(r"\d{8}T\d{6}", get_run_id())


def test_invalid_run_id():
    with pytest.raises(ValueError):
        get_run_id("bad id!")

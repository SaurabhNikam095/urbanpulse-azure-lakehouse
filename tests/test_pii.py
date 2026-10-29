import pytest

from ingestion.pii import (
    SALT_ENV_VAR,
    MissingSaltError,
    get_pii_salt,
    hash_pii,
)

SALT = b"test-salt-one"


def test_same_input_same_hash():
    assert hash_pii("alice", salt=SALT) == hash_pii("alice", salt=SALT)


def test_different_salt_gives_different_hash():
    assert hash_pii("alice", salt=b"salt-a") != hash_pii("alice", salt=b"salt-b")


def test_different_values_give_different_hashes():
    assert hash_pii("alice", salt=SALT) != hash_pii("bob", salt=SALT)


def test_email_is_normalized():
    a = hash_pii(" A@X.com ", is_email=True, salt=SALT)
    b = hash_pii("a@x.com", is_email=True, salt=SALT)
    assert a == b


def test_generic_value_is_trimmed_but_case_kept():
    assert hash_pii("  Alice ", salt=SALT) == hash_pii("Alice", salt=SALT)
    assert hash_pii("Alice", salt=SALT) != hash_pii("alice", salt=SALT)


def test_none_stays_none():
    assert hash_pii(None, salt=SALT) is None


def test_blank_string_becomes_none():
    assert hash_pii("   ", salt=SALT) is None


def test_missing_salt_raises(monkeypatch):
    monkeypatch.delenv(SALT_ENV_VAR, raising=False)
    with pytest.raises(MissingSaltError):
        hash_pii("alice")


def test_blank_salt_raises(monkeypatch):
    monkeypatch.setenv(SALT_ENV_VAR, "   ")
    with pytest.raises(MissingSaltError):
        get_pii_salt()


def test_raw_value_not_in_output():
    result = hash_pii("alice@example.com", is_email=True, salt=SALT)
    assert "alice" not in result
    assert "example" not in result


def test_hash_is_64_hex_chars():
    result = hash_pii("alice", salt=SALT)
    assert len(result) == 64
    int(result, 16)  # raises if not hex


def test_salt_read_from_environment(monkeypatch):
    monkeypatch.setenv(SALT_ENV_VAR, "env-salt")
    assert hash_pii("alice") == hash_pii("alice", salt=b"env-salt")

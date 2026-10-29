"""PII hashing (pseudonymization) for UrbanPulse.

HMAC-SHA256 with a secret salt. Deterministic: the same value and salt
always give the same hash, so joins across tables still work.
"""

from __future__ import annotations

import hashlib
import hmac
import os

SALT_ENV_VAR = "URBANPULSE_PII_SALT"


class MissingSaltError(RuntimeError):
    """Raised when no PII salt is configured."""


def get_pii_salt() -> bytes:
    """Return the salt. Today: env var. Later: Key Vault (only this changes)."""
    salt = os.environ.get(SALT_ENV_VAR, "")
    if not salt.strip():
        raise MissingSaltError(f"{SALT_ENV_VAR} is not set. Refusing to hash with an empty salt.")
    return salt.encode("utf-8")


def normalize(value: str | None, *, is_email: bool = False) -> str | None:
    """Trim whitespace; lowercase emails. None or blank becomes None."""
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned.lower() if is_email else cleaned


def hash_pii(
    value: str | None,
    *,
    is_email: bool = False,
    salt: bytes | None = None,
) -> str | None:
    """Return the HMAC-SHA256 hex digest of the normalized value."""
    cleaned = normalize(value, is_email=is_email)
    if cleaned is None:
        return None
    key = salt if salt is not None else get_pii_salt()
    if not key:
        raise MissingSaltError("Salt must not be empty.")
    return hmac.new(key, cleaned.encode("utf-8"), hashlib.sha256).hexdigest()

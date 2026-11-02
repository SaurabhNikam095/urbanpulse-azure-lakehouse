"""Register the Avro data contracts in the Event Hubs Schema Registry.

Run from the repo root after `az login`:
    python -m scripts.register_schemas --namespace evhns-urbanpulse-dev-sn095

Registering an identical schema again returns the existing one, so the
script is safe to re-run. Exit code 0 means every schema was registered.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import fastavro
from azure.core.exceptions import HttpResponseError
from azure.identity import AzureCliCredential
from azure.schemaregistry import SchemaRegistryClient

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
DEFAULT_GROUP = "urbanpulse"
# Order matters: trip v1 must be registered before trip v2.
SCHEMA_FILES = [
    "trip_event_v1.avsc",
    "trip_event_v2.avsc",
    "gps_event_v1.avsc",
    "payment_event_v1.avsc",
]


def load_definition(file_name: str) -> tuple[str, str]:
    """Return the schema's full name and its compact JSON text."""
    raw = json.loads((SCHEMA_DIR / file_name).read_text(encoding="utf-8"))
    full_name = fastavro.parse_schema(raw)["name"]
    return full_name, json.dumps(raw, separators=(",", ":"))


def register_all(namespace: str, group: str) -> int:
    """Register every schema file and return the number of failures."""
    client = SchemaRegistryClient(
        fully_qualified_namespace=f"{namespace}.servicebus.windows.net",
        credential=AzureCliCredential(),
    )
    failures = 0
    for file_name in SCHEMA_FILES:
        name, definition = load_definition(file_name)
        try:
            props = client.register_schema(group, name, definition, "Avro")
        except HttpResponseError as error:
            failures += 1
            print(f"FAIL  {file_name} as {name}: {error.message}")
            continue
        print(f"OK    {file_name} as {name} (schema id {props.id})")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True, help="Namespace name only")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="Schema group")
    args = parser.parse_args()
    failures = register_all(args.namespace, args.group)
    if failures:
        print(f"{failures} schema(s) failed.")
        print("A 403 just after a role change usually means: wait and retry.")
        return 1
    print("All schemas registered.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

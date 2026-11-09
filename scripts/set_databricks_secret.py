"""Store the Event Hubs Kafka connection string in a Databricks secret scope.

Run from the repo root after `az login`:
    $env:DATABRICKS_HOST = "https://<workspace-url>"
    python -m scripts.set_databricks_secret --namespace evhns-urbanpulse-dev-sn095

The value goes from Azure straight to Databricks. It is never printed,
logged or written to disk. Re-running replaces the stored value.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

DEFAULT_SCOPE = "urbanpulse"
DEFAULT_KEY = "eventhubs-connection-string"
DEFAULT_RESOURCE_GROUP = "rg-urbanpulse-dev"
AUTH_RULE = "RootManageSharedAccessKey"


def read_connection_string(namespace: str, resource_group: str) -> str:
    """Ask Azure for the namespace connection string without printing it."""
    az = shutil.which("az")
    if az is None:
        raise RuntimeError("Azure CLI (az) was not found on PATH.")
    result = subprocess.run(
        [
            az,
            "eventhubs",
            "namespace",
            "authorization-rule",
            "keys",
            "list",
            "--resource-group",
            resource_group,
            "--namespace-name",
            namespace,
            "--name",
            AUTH_RULE,
            "--query",
            "primaryConnectionString",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    value = result.stdout.strip()
    if result.returncode != 0 or not value:
        raise RuntimeError("Could not read the connection string (is az logged in?).")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--resource-group", default=DEFAULT_RESOURCE_GROUP)
    parser.add_argument("--scope", default=DEFAULT_SCOPE)
    parser.add_argument("--key", default=DEFAULT_KEY)
    args = parser.parse_args()

    try:
        connection_string = read_connection_string(args.namespace, args.resource_group)
        client = WorkspaceClient(auth_type="azure-cli")
        try:
            client.secrets.create_scope(scope=args.scope)
        except ResourceAlreadyExists:
            pass
        client.secrets.put_secret(scope=args.scope, key=args.key, string_value=connection_string)
    except Exception as exc:  # noqa: BLE001 - report any failure, never the value
        print(f"FAIL  {type(exc).__name__}: {exc}")
        return 1
    print(f"OK    stored {args.key} in scope {args.scope}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

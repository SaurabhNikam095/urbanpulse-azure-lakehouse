"""Register the Debezium PostgreSQL connector with Kafka Connect.

Implements the ADR-002 topic reroute: all table changes go into the single `cdc` event hub.
The database password is read from the PG_PASSWORD environment variable, never from a file.
"""

from __future__ import annotations

import os
import sys
import time

import requests

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "urbanpulse-pg-cdc"
JSON_NO_SCHEMA = "org.apache.kafka.connect.json.JsonConverter"


def build_config(password: str) -> dict[str, str]:
    """Return the connector configuration as a flat string map."""
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "upadmin",
        "database.password": password,
        "database.dbname": "urbanpulse",
        "topic.prefix": "urbanpulse",
        "plugin.name": "pgoutput",
        "slot.name": "urbanpulse_slot",
        "publication.name": "urbanpulse_cdc",
        "publication.autocreate.mode": "disabled",
        "table.include.list": "public.drivers,public.riders,public.promo_codes",
        # The slot was created before seeding, so stream from it; no extra snapshot.
        "snapshot.mode": "no_data",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "false",
        "key.converter": JSON_NO_SCHEMA,
        "key.converter.schemas.enable": "false",
        "value.converter": JSON_NO_SCHEMA,
        "value.converter.schemas.enable": "false",
        # ADR-002: reroute every table topic into the one CDC event hub.
        "transforms": "reroute",
        "transforms.reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.reroute.topic.regex": r"urbanpulse\.public\.(.*)",
        "transforms.reroute.topic.replacement": "cdc",
        "transforms.reroute.key.enforce.uniqueness": "true",
    }


def main() -> None:
    password = os.environ.get("PG_PASSWORD")
    if not password:
        sys.exit("Set PG_PASSWORD first.")

    response = requests.put(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
        json=build_config(password),
        timeout=30,
    )
    if response.status_code not in (200, 201):
        sys.exit(f"Connect rejected the config: HTTP {response.status_code}\n{response.text[:600]}")
    print(f"Connector {CONNECTOR_NAME} registered (HTTP {response.status_code}).")

    for _ in range(15):
        time.sleep(2)
        status = requests.get(
            f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=30
        ).json()
        connector_state = status["connector"]["state"]
        task_states = [task["state"] for task in status["tasks"]]
        if (
            connector_state == "RUNNING"
            and task_states
            and all(s == "RUNNING" for s in task_states)
        ):
            break
    print(f"Connector state: {connector_state}; tasks: {task_states}")


if __name__ == "__main__":
    main()

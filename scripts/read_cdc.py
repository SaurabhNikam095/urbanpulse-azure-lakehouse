"""Read the `cdc` event hub from the beginning and summarise the Debezium events.

The Event Hubs connection string is read from the EH_CONN environment variable.
Nothing is committed to the consumer group, so the script can be re-run at any time.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from collections import Counter, deque

from confluent_kafka import Consumer

BOOTSTRAP = "evhns-urbanpulse-dev-sn095.servicebus.windows.net:9093"
TOPIC = "cdc"
FIRST_MESSAGE_WAIT_S = 60
IDLE_S = 20
TAIL = 6
PRIMARY_KEYS = {"drivers": "driver_id", "riders": "rider_id", "promo_codes": "code"}


def describe(event: dict) -> str:
    """Return a one-line summary of a Debezium event."""
    op = event.get("op")
    table = event["source"]["table"]
    before = event.get("before") or {}
    after = event.get("after") or {}
    row = after or before
    key = row.get(PRIMARY_KEYS.get(table, ""), "?")
    line = f"op={op} table={table} key={key}"
    if op == "u":
        changes = {
            name: (before.get(name), after.get(name))
            for name in after
            if before.get(name) != after.get(name) and name != "updated_at"
        }
        line += f" changed={changes}"
    return line


def main() -> None:
    connection_string = os.environ.get("EH_CONN")
    if not connection_string:
        sys.exit("Set EH_CONN first.")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": connection_string,
            "group.id": f"cdc-reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    counts: Counter[tuple[str, str]] = Counter()
    tail: deque[str] = deque(maxlen=TAIL)
    started = last_message = time.monotonic()
    got_any = False

    try:
        while True:
            msg = consumer.poll(5.0)
            now = time.monotonic()
            if msg is None:
                limit = IDLE_S if got_any else FIRST_MESSAGE_WAIT_S
                if now - (last_message if got_any else started) > limit:
                    break
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue
            if msg.value() is None:
                continue
            got_any = True
            last_message = now
            event = json.loads(msg.value())
            counts[(event["source"]["table"], event["op"])] += 1
            tail.append(describe(event))
    finally:
        consumer.close()

    print("Events by table and op:")
    for (table, op), number in sorted(counts.items()):
        print(f"  {table:12} op={op}  {number}")
    print(f"Total: {sum(counts.values())}")
    print("Last events read:")
    for line in tail:
        print(f"  {line}")


if __name__ == "__main__":
    main()

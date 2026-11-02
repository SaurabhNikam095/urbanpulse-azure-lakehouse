"""Send the simulated event streams to Event Hubs through the Kafka API.

Good records are Avro-encoded and sent to the topic of their stream, with the
registered schema id in the content-type header. Records that fail the Bronze
checks go to the dead-letter topic together with the reason. From the repo root:

    python -m ingestion.producer --namespace evhns-urbanpulse-dev-sn095 --dry-run
    python -m ingestion.producer --namespace evhns-urbanpulse-dev-sn095

The connection string is read from the EVENTHUB_CONNECTION_STRING environment
variable and is never printed or written to a file.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any

import fastavro

from ingestion.bronze import _SCHEMA_FILES, SCHEMA_DIR, _schema, _schema_key, check_record
from simulators.chaos import ChaosInjector
from simulators.gps_simulator import GPSSimulator
from simulators.payment_simulator import PaymentSimulator
from simulators.trip_simulator import TripSimulator

DEAD_LETTER_TOPIC = "dead-letter"
DEFAULT_GROUP = "urbanpulse"
DEFAULT_MAX_EVENTS = 5000
KEY_FIELDS = {"trips": "trip_id", "gps": "driver_id", "payments": "payment_id"}
PRODUCE_ATTEMPTS = 20


@dataclass
class SendStats:
    """What was handed to the Kafka client."""

    sent: dict[str, int] = field(default_factory=dict)
    dead_letter: int = 0
    delivery_errors: int = 0

    @property
    def total(self) -> int:
        """Every record ends up in a stream topic or in the dead-letter topic."""
        return sum(self.sent.values()) + self.dead_letter


def encode_avro(record: dict[str, object], stream: str) -> tuple[bytes, str]:
    """Return the Avro bytes of a good record and the key of the schema used."""
    schema_key = _schema_key(record, stream)
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, _schema(schema_key), record)
    return buffer.getvalue(), schema_key


def message_key(record: object, stream: str) -> bytes | None:
    """Return the partition key of a record, or None if it has no usable key."""
    if isinstance(record, dict):
        value = record.get(KEY_FIELDS[stream])
        if value is not None:
            return str(value).encode("utf-8")
    return None


def _produce(producer: Any, **kwargs: Any) -> None:
    """Call producer.produce, waiting briefly if the local queue is full."""
    for _ in range(PRODUCE_ATTEMPTS):
        try:
            producer.produce(**kwargs)
            return
        except BufferError:
            producer.poll(0.5)
    raise RuntimeError("Kafka queue stayed full; stopping instead of losing records")


def send_stream(
    producer: Any,
    stream: str,
    records: list[dict[str, object]],
    schema_ids: dict[str, str],
    stats: SendStats,
) -> None:
    """Send one stream: good records to its topic, bad ones to the dead-letter topic."""

    def on_delivery(error: object, _message: object) -> None:
        if error is not None:
            stats.delivery_errors += 1

    for record in records:
        key = message_key(record, stream)
        reason = check_record(record, stream)
        if reason is None:
            value, schema_key = encode_avro(record, stream)
            topic = stream
            headers = [("content-type", f"avro/binary+{schema_ids[schema_key]}")]
            stats.sent[stream] = stats.sent.get(stream, 0) + 1
        else:
            body = {"stream": stream, "reason": reason, "record": record}
            value = json.dumps(body, default=str).encode("utf-8")
            topic = DEAD_LETTER_TOPIC
            headers = [("error-reason", reason[:200]), ("source-stream", stream)]
            stats.dead_letter += 1
        _produce(
            producer, topic=topic, key=key, value=value, headers=headers, on_delivery=on_delivery
        )
        producer.poll(0)


def build_streams(trips: int, ticks: int, seed: int) -> dict[str, list[dict[str, object]]]:
    """Generate the three streams from the simulators and apply the chaos injector."""
    trip_events = TripSimulator(seed=seed).generate_events(trips)
    pings = GPSSimulator().generate_pings(ticks)
    payments = PaymentSimulator().generate_payments(trip_events)
    clean = {
        "trips": [event.to_dict() for event in trip_events],
        "gps": [event.to_dict() for event in pings],
        "payments": [event.to_dict() for event in payments],
    }
    injector = ChaosInjector(seed=seed)
    streams: dict[str, list[dict[str, object]]] = {}
    for stream, events in clean.items():
        streams[stream], manifest = injector.inject(events, stream)
        print(
            f"{stream}: {manifest.input_count} generated, {len(streams[stream])} to send "
            f"({manifest.bad} bad, {manifest.late} late, {manifest.duplicates} duplicates)"
        )
    return streams


def lookup_schema_ids(namespace: str, group: str) -> dict[str, str]:
    """Look up the registered schema id of each Bronze schema key."""
    from azure.identity import AzureCliCredential
    from azure.schemaregistry import SchemaRegistryClient

    client = SchemaRegistryClient(
        fully_qualified_namespace=f"{namespace}.servicebus.windows.net",
        credential=AzureCliCredential(),
    )
    ids: dict[str, str] = {}
    for key, file_name in _SCHEMA_FILES.items():
        raw = json.loads((SCHEMA_DIR / file_name).read_text(encoding="utf-8"))
        name = fastavro.parse_schema(raw)["name"]
        definition = json.dumps(raw, separators=(",", ":"))
        ids[key] = client.get_schema_properties(group, name, definition, "Avro").id
    return ids


def build_producer(namespace: str, connection_string: str) -> Any:
    """Create a Kafka producer for the Event Hubs Kafka endpoint."""
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": connection_string,
            "client.id": "urbanpulse-producer",
            "linger.ms": 50,
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True, help="Namespace name only")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="Schema group")
    parser.add_argument("--trips", type=int, default=250, help="Trips to simulate")
    parser.add_argument("--ticks", type=int, default=20, help="GPS ticks to simulate")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS)
    parser.add_argument("--dry-run", action="store_true", help="Check only, send nothing")
    args = parser.parse_args(argv)

    streams = build_streams(args.trips, args.ticks, args.seed)
    total = sum(len(records) for records in streams.values())
    print(f"total to send: {total}")
    if total > args.max_events:
        print(f"Refusing to send: {total} is above --max-events {args.max_events}.")
        return 1

    if args.dry_run:
        for stream, records in streams.items():
            bad = sum(check_record(record, stream) is not None for record in records)
            print(f"dry-run {stream}: {len(records) - bad} good, {bad} for dead-letter")
        print("Dry run only: nothing was sent.")
        return 0

    connection_string = os.environ.get("EVENTHUB_CONNECTION_STRING")
    if not connection_string:
        print("EVENTHUB_CONNECTION_STRING is not set.")
        return 2

    schema_ids = lookup_schema_ids(args.namespace, args.group)
    producer = build_producer(args.namespace, connection_string)
    stats = SendStats()
    for stream, records in streams.items():
        send_stream(producer, stream, records, schema_ids, stats)
    remaining = producer.flush(60)

    print(f"sent {stats.sent}, dead-letter {stats.dead_letter}, total {stats.total}")
    if remaining or stats.delivery_errors:
        print(f"Problem: {remaining} not delivered, {stats.delivery_errors} delivery errors.")
        return 1
    print("All records delivered.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

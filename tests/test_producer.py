"""Tests for the Kafka producer, using a fake client so no cloud is needed."""

from __future__ import annotations

import io
import json

import fastavro

from ingestion import producer
from ingestion.bronze import _schema
from simulators.trip_simulator import TripSimulator

SCHEMA_IDS = {"trips_v1": "11", "trips_v2": "12", "gps": "13", "payments": "14"}


class FakeProducer:
    """Records what would have been sent to Kafka."""

    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None) -> None:
        self.messages.append(
            {"topic": topic, "key": key, "value": value, "headers": dict(headers or [])}
        )

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 0


def _good_trip() -> dict[str, object]:
    return TripSimulator(seed=1).generate_events(1)[0].to_dict()


def test_encode_avro_round_trip() -> None:
    record = _good_trip()
    data, schema_key = producer.encode_avro(record, "trips")
    decoded = fastavro.schemaless_reader(io.BytesIO(data), _schema(schema_key))
    assert schema_key == "trips_v1"
    assert decoded == record


def test_good_record_goes_to_its_stream_with_schema_id_header() -> None:
    fake, stats = FakeProducer(), producer.SendStats()
    producer.send_stream(fake, "trips", [_good_trip()], SCHEMA_IDS, stats)
    message = fake.messages[0]
    assert message["topic"] == "trips"
    assert message["headers"]["content-type"] == "avro/binary+11"


def test_bad_record_goes_to_dead_letter_with_reason() -> None:
    fake, stats = FakeProducer(), producer.SendStats()
    producer.send_stream(fake, "trips", [{"trip_id": 123}], SCHEMA_IDS, stats)
    message = fake.messages[0]
    body = json.loads(message["value"])
    assert message["topic"] == producer.DEAD_LETTER_TOPIC
    assert body["stream"] == "trips"
    assert body["reason"].startswith("schema:")
    assert body["reason"].startswith(message["headers"]["error-reason"])


def test_stats_count_good_and_bad_records() -> None:
    fake, stats = FakeProducer(), producer.SendStats()
    records = [_good_trip(), _good_trip(), {"trip_id": 123}]
    producer.send_stream(fake, "trips", records, SCHEMA_IDS, stats)
    assert stats.sent == {"trips": 2}
    assert stats.dead_letter == 1
    assert stats.total == len(fake.messages) == 3


def test_message_key_uses_stream_id_and_tolerates_bad_records() -> None:
    record = _good_trip()
    assert producer.message_key(record, "trips") == str(record["trip_id"]).encode("utf-8")
    assert producer.message_key("not a dict", "trips") is None
    assert producer.message_key({"other": 1}, "gps") is None


def test_dry_run_sends_nothing_and_succeeds(capsys) -> None:
    code = producer.main(["--namespace", "x", "--trips", "2", "--ticks", "1", "--dry-run"])
    assert code == 0
    assert "Dry run only" in capsys.readouterr().out

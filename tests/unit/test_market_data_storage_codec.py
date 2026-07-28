from __future__ import annotations

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.market_data.models import serialize_envelope
from tx_trade.storage.codec import (
    decode_envelope,
    decode_storage_value,
    encode_envelope,
    encode_storage_value,
    record_sha256,
)


def test_all_registered_payloads_roundtrip_through_codec() -> None:
    for envelope in make_offline_fixture_envelopes():
        payload, raw, checksum = encode_envelope(envelope)
        row = {
            "payload_json": payload,
            "raw_json": raw,
            "payload_sha256": checksum,
            **{
                name: getattr(envelope, name)
                for name in (
                    "schema_version",
                    "ingest_sequence",
                    "connection_generation",
                    "sequence",
                    "broker_sequence",
                    "dedupe_key",
                    "metadata_version",
                )
            },
            "event_type": envelope.event_type.value,
            "source": envelope.source,
            "source_mode": envelope.source_mode.value,
            "session_id": str(envelope.session_id),
            "event_at": envelope.event_at.isoformat() if envelope.event_at else None,
            "received_at": envelope.received_at.isoformat(),
            "trading_day": envelope.trading_day.isoformat() if envelope.trading_day else None,
        }
        row["record_sha256"] = record_sha256(row)
        assert serialize_envelope(decode_envelope(row)) == serialize_envelope(envelope)


def test_codec_rejects_checksum_mismatch() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    payload, raw, _ = encode_envelope(envelope)
    row = {
        "payload_json": payload,
        "raw_json": raw,
        "payload_sha256": "0" * 64,
        "schema_version": envelope.schema_version,
        "event_type": envelope.event_type.value,
        "source": envelope.source,
        "source_mode": envelope.source_mode.value,
        "session_id": str(envelope.session_id),
        "ingest_sequence": envelope.ingest_sequence,
        "connection_generation": envelope.connection_generation,
        "sequence": envelope.sequence,
        "broker_sequence": None,
        "dedupe_key": envelope.dedupe_key,
        "event_at": envelope.event_at.isoformat(),
        "received_at": envelope.received_at.isoformat(),
        "trading_day": None,
        "metadata_version": None,
    }
    row["record_sha256"] = record_sha256(row)
    try:
        decode_envelope(row)
    except ValueError as exc:
        assert "checksum" in str(exc)
    else:
        raise AssertionError("checksum mismatch was accepted")


def test_storage_tags_do_not_collide_with_user_base64_shaped_mapping() -> None:
    value = {
        "encoding": "base64",
        "data": "not-bytes",
        "nested": [
            {"$tx-storage-type": "bytes", "data": "still-a-mapping"},
            b"\x00\xff",
        ],
    }
    assert decode_storage_value(encode_storage_value(value)) == value

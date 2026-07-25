"""SQLite readback adapter with integrity verification."""

from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID

from tx_trade.market_data.models import MarketDataEnvelope, SCHEMA_VERSION
from tx_trade.market_data.ports import ReadbackIntegrityReport

from .codec import decode_envelope
from .sqlite_repository import SQLiteMarketDataRepository


class SQLiteReplaySource:
    def __init__(self, repository: SQLiteMarketDataRepository) -> None:
        self._repository = repository
        self._session_id: UUID | None = None

    def open(self, session_id: UUID) -> None:
        if self._repository.get_session(session_id) is None:
            raise KeyError(f"recording session not found: {session_id}")
        self._session_id = session_id

    def _open_session(self) -> UUID:
        if self._session_id is None:
            raise RuntimeError("replay source is not open")
        return self._session_id

    def iter_events(
        self, *, after_ingest_sequence: int | None = None
    ) -> Iterator[MarketDataEnvelope]:
        return self._repository.iter_events(
            self._open_session(),
            after_ingest_sequence=after_ingest_sequence,
        )

    def verify_integrity(self) -> ReadbackIntegrityReport:
        session_id = self._open_session()
        session = self._repository.get_session(session_id)
        errors: list[str] = []
        rows = self._repository.iter_event_rows(session_id)
        previous: int | None = None
        count = 0
        first: int | None = None
        last: int | None = None
        for index, row in enumerate(rows):
            count += 1
            row_sequence = row["ingest_sequence"]
            if first is None:
                first = row_sequence
            last = row_sequence
            try:
                envelope = decode_envelope(row)
                if envelope.session_id != session_id:
                    errors.append(f"event {index} has a different session")
                if envelope.schema_version != SCHEMA_VERSION:
                    errors.append(f"event {index} has wrong schema version")
                if session and (
                    envelope.source != session.source
                    or envelope.source_mode != session.source_mode
                ):
                    errors.append(f"event {index} source metadata mismatch")
                if previous is not None and envelope.ingest_sequence <= previous:
                    errors.append("ingest_sequence is not strictly increasing")
                previous = envelope.ingest_sequence
            except Exception as exc:
                errors.append(f"event {index} integrity failure: {exc}")
        expected_checkpoint = -1 if last is None else last
        if session and session.last_ingest_sequence != expected_checkpoint:
            errors.append(
                "session checkpoint does not match authoritative event log"
            )
        return ReadbackIntegrityReport(
            session_id=session_id,
            event_count=count,
            first_ingest_sequence=first,
            last_ingest_sequence=last,
            is_valid=not errors,
            errors=tuple(errors),
        )

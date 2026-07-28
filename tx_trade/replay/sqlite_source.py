"""Fail-closed preparation of a Phase 1 SQLite recording for replay."""

from __future__ import annotations

from uuid import UUID

from tx_trade.market_data.models import SCHEMA_VERSION
from tx_trade.storage.readback import SQLiteReplaySource
from tx_trade.storage.sqlite_repository import SQLiteMarketDataRepository

from .contracts import (
    ReplayError,
    ReplayFailureCode,
    ReplaySessionDescriptor,
)


def prepare_sqlite_replay_source(
    repository: SQLiteMarketDataRepository,
    session_id: UUID,
) -> tuple[SQLiteReplaySource, ReplaySessionDescriptor]:
    """Open and validate a complete SQLite recording before it can publish."""

    source_lookup_failed = False
    try:
        session = repository.get_session(session_id)
    except Exception:
        source_lookup_failed = True
        session = None
    if source_lookup_failed:
        raise ReplayError(ReplayFailureCode.SOURCE_FAILED)
    if session is None:
        raise ReplayError(ReplayFailureCode.SESSION_NOT_FOUND)
    if session.status != "complete":
        raise ReplayError(ReplayFailureCode.SESSION_NOT_COMPLETE)
    if session.schema_version != SCHEMA_VERSION:
        raise ReplayError(ReplayFailureCode.SCHEMA_MISMATCH)

    source = SQLiteReplaySource(repository)
    source_open_failed = False
    try:
        source.open(session_id)
    except Exception:
        source_open_failed = True
    if source_open_failed:
        raise ReplayError(ReplayFailureCode.SOURCE_FAILED)

    integrity_check_failed = False
    try:
        report = source.verify_integrity()
    except Exception:
        integrity_check_failed = True
        report = None
    if integrity_check_failed or report is None or not report.is_valid:
        raise ReplayError(ReplayFailureCode.INTEGRITY_FAILED)
    if report.event_count == 0:
        raise ReplayError(ReplayFailureCode.EMPTY_SESSION)

    first = report.first_ingest_sequence
    last = report.last_ingest_sequence
    if first is None or last is None:
        raise ReplayError(ReplayFailureCode.INTEGRITY_FAILED)
    return source, ReplaySessionDescriptor(
        session_id=session.session_id,
        status=session.status,
        schema_version=session.schema_version,
        event_count=report.event_count,
        first_ingest_sequence=first,
        last_ingest_sequence=last,
    )

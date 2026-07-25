"""SQLite authoritative event log and typed query projections."""

from __future__ import annotations

import re
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from tx_trade.market_data.models import (
    AdapterDiagnostic,
    ConnectionStatus,
    EventType,
    Instrument,
    MarketDataEnvelope,
    Quote,
    SCHEMA_VERSION,
    SourceMode,
    TAIPEI,
    Tick,
)
from tx_trade.market_data.ports import RecordingSession

from .codec import (
    canonical_json,
    decode_envelope,
    encode_envelope,
    record_sha256,
)


class StorageError(RuntimeError):
    pass


class SchemaMismatchError(StorageError):
    pass


class IntegrityError(StorageError):
    pass


class DuplicateSequenceError(StorageError):
    pass


@dataclass(frozen=True, slots=True)
class RepositoryStats:
    persisted_events: int
    duplicate_events: int
    projection_failures: int


@dataclass(frozen=True, slots=True)
class StoredSession:
    session_id: UUID
    schema_version: int
    source: str
    source_mode: SourceMode
    status: str
    last_ingest_sequence: int


class SQLiteMarketDataRepository:
    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_ms: int = 5000,
        recover_incomplete_sessions: bool = False,
    ) -> None:
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a non-negative integer")
        self._path = Path(db_path)
        self._lock = threading.RLock()
        self._closed = False
        self._stats = RepositoryStats(0, 0, 0)
        self._raw_only_sessions: set[UUID] = set()
        self._connection = sqlite3.connect(
            self._path, check_same_thread=False, isolation_level=None
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            if str(self._path) != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
            self._initialize_or_validate()
            if recover_incomplete_sessions:
                self._connection.execute(
                    "UPDATE recording_sessions SET status='incomplete' "
                    "WHERE status='recording'"
                )
        except Exception:
            self._connection.close()
            raise

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    def _initialize_or_validate(self) -> None:
        count = self._connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
        if count == 0:
            schema_path = Path(__file__).with_name("schema.sql")
            self._connection.executescript(schema_path.read_text(encoding="utf-8"))
            applied_at = datetime.now(TAIPEI).isoformat()
            self._connection.execute(
                "INSERT INTO schema_meta(version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, applied_at),
            )
            self._validate_schema_objects()
            return
        try:
            rows = self._connection.execute(
                "SELECT version FROM schema_meta ORDER BY version"
            ).fetchall()
        except sqlite3.Error as exc:
            raise SchemaMismatchError("schema_meta is missing") from exc
        if [row[0] for row in rows] != [SCHEMA_VERSION]:
            raise SchemaMismatchError(
                f"schema migration history mismatch: expected [{SCHEMA_VERSION}]"
            )
        self._validate_schema_objects()

    def _validate_schema_objects(self) -> None:
        required_columns = {
            "schema_meta": {"version", "applied_at"},
            "recording_sessions": {
                "session_id", "schema_version", "source", "source_mode",
                "started_at", "ended_at", "trading_day", "status",
                "config_fingerprint", "last_ingest_sequence", "dropped_tick_count",
            },
            "event_log": {
                "event_id", "session_id", "ingest_sequence", "schema_version",
                "event_type", "source", "source_mode", "connection_generation",
                "sequence", "broker_sequence", "dedupe_key", "event_at",
                "trading_day", "received_at", "metadata_version", "payload_json",
                "raw_json", "payload_sha256", "record_sha256",
            },
            "instruments": {
                "instrument_id", "metadata_version", "symbol", "venue",
                "market_no", "stock_idx", "display_name", "asset_class",
                "currency", "price_scale_text", "quantity_scale_text",
                "updated_at", "raw_payload_json",
            },
            "quotes": {
                "quote_id", "event_id", "session_id", "ingest_sequence",
                "schema_version", "connection_generation", "sequence",
                "dedupe_key", "instrument_id", "metadata_version",
                "market_no_raw", "stock_idx_raw", "bid_raw", "ask_raw",
                "last_raw", "bid_qty_raw", "ask_qty_raw", "last_qty_raw",
                "bid_normalized_text", "ask_normalized_text",
                "last_normalized_text", "event_at", "trading_day", "received_at",
                "is_simulated", "is_long_callback",
            },
            "ticks": {
                "tick_id", "event_id", "session_id", "ingest_sequence",
                "schema_version", "connection_generation", "sequence",
                "dedupe_key", "instrument_id", "metadata_version",
                "market_no_raw", "stock_idx_raw", "source_pointer_raw",
                "date_raw", "time_hms_raw", "time_subsecond_raw", "bid_raw",
                "ask_raw", "close_raw", "quantity_raw", "simulate_raw",
                "bid_normalized_text", "ask_normalized_text",
                "close_normalized_text", "quantity_normalized_text", "event_at",
                "trading_day", "received_at", "is_simulated", "is_long_callback",
            },
            "connection_events": {
                "connection_event_id", "event_id", "session_id",
                "ingest_sequence", "schema_version", "connection_generation",
                "sequence", "dedupe_key", "state", "broker_kind_raw",
                "broker_code_raw", "message", "is_ready", "changed_at",
                "received_at", "trading_day",
            },
        }
        for table, expected in required_columns.items():
            table_info = list(
                self._connection.execute(f"PRAGMA table_info({table})")
            )
            actual = {row["name"] for row in table_info}
            if not expected <= actual:
                missing = sorted(expected - actual)
                raise SchemaMismatchError(
                    f"table {table} is missing required columns: {missing}"
                )
        required_primary_keys = {
            "schema_meta": ("version",),
            "recording_sessions": ("session_id",),
            "event_log": ("event_id",),
            "instruments": ("instrument_id", "metadata_version"),
            "quotes": ("quote_id",),
            "ticks": ("tick_id",),
            "connection_events": ("connection_event_id",),
        }
        required_not_null = {
            "schema_meta": {"applied_at"},
            "recording_sessions": {
                "schema_version", "source", "source_mode", "started_at", "status",
                "config_fingerprint", "last_ingest_sequence", "dropped_tick_count",
            },
            "event_log": {
                "session_id", "ingest_sequence", "schema_version", "event_type",
                "source", "source_mode", "connection_generation", "sequence",
                "dedupe_key", "received_at", "payload_json", "payload_sha256",
                "record_sha256",
            },
            "instruments": {
                "instrument_id", "metadata_version", "symbol", "venue", "updated_at",
            },
            "quotes": {
                "event_id", "session_id", "ingest_sequence", "schema_version",
                "connection_generation", "sequence", "dedupe_key", "instrument_id",
                "market_no_raw", "stock_idx_raw", "bid_raw", "ask_raw", "last_raw",
                "received_at", "is_long_callback",
            },
            "ticks": {
                "event_id", "session_id", "ingest_sequence", "schema_version",
                "connection_generation", "sequence", "dedupe_key", "instrument_id",
                "market_no_raw", "stock_idx_raw", "source_pointer_raw", "date_raw",
                "time_hms_raw", "time_subsecond_raw", "bid_raw", "ask_raw",
                "close_raw", "quantity_raw", "simulate_raw", "received_at",
                "is_long_callback",
            },
            "connection_events": {
                "event_id", "session_id", "ingest_sequence", "schema_version",
                "connection_generation", "sequence", "dedupe_key", "state",
                "is_ready", "changed_at", "received_at",
            },
        }
        for table, expected_pk in required_primary_keys.items():
            info = list(self._connection.execute(f"PRAGMA table_info({table})"))
            actual_pk = tuple(
                row["name"] for row in sorted(
                    (row for row in info if row["pk"]), key=lambda row: row["pk"]
                )
            )
            if actual_pk != expected_pk:
                raise SchemaMismatchError(
                    f"table {table} has wrong primary key: {actual_pk}"
                )
            actual_not_null = {row["name"] for row in info if row["notnull"]}
            missing = required_not_null[table] - actual_not_null
            if missing:
                raise SchemaMismatchError(
                    f"table {table} has nullable required columns: {sorted(missing)}"
                )
        required_indices = {
            "idx_sessions_trading_day_started": (
                "trading_day", "started_at",
            ),
            "idx_event_log_readback": ("session_id", "ingest_sequence"),
            "idx_event_log_type_day": ("event_type", "trading_day"),
            "idx_instruments_symbol_version": (
                "venue", "symbol", "metadata_version",
            ),
            "idx_instruments_market_stock": (
                "market_no", "stock_idx", "updated_at",
            ),
            "idx_quotes_instrument_event": (
                "instrument_id", "trading_day", "event_at", "quote_id",
            ),
            "idx_ticks_readback": ("session_id", "ingest_sequence", "tick_id"),
            "idx_ticks_instrument_day": (
                "instrument_id", "trading_day", "event_at", "tick_id",
            ),
            "idx_ticks_source_ptr": (
                "session_id", "connection_generation", "instrument_id",
                "source_pointer_raw",
            ),
            "idx_connection_events_session_time": (
                "session_id", "changed_at", "connection_event_id",
            ),
        }
        actual_indices = {
            row["name"]
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        if not set(required_indices) <= actual_indices:
            raise SchemaMismatchError(
                f"schema is missing required indices: "
                f"{sorted(set(required_indices) - actual_indices)}"
            )
        for name, expected_columns in required_indices.items():
            index_owner = self._connection.execute(
                "SELECT tbl_name FROM sqlite_master "
                "WHERE type='index' AND name=?",
                (name,),
            ).fetchone()["tbl_name"]
            index_metadata = next(
                row
                for row in self._connection.execute(
                    f"PRAGMA index_list({index_owner})"
                )
                if row["name"] == name
            )
            if index_metadata["unique"] != 0:
                raise SchemaMismatchError(
                    f"lookup index {name} must be non-unique"
                )
            actual_columns = tuple(
                row["name"]
                for row in self._connection.execute(f"PRAGMA index_info({name})")
            )
            if actual_columns != expected_columns:
                raise SchemaMismatchError(
                    f"index {name} has wrong columns: {actual_columns}"
                )
        required_uniques = {
            "event_log": {
                ("session_id", "ingest_sequence"),
                ("session_id", "dedupe_key"),
            },
            "quotes": {
                ("event_id",),
                ("session_id", "ingest_sequence"),
                ("session_id", "dedupe_key"),
            },
            "ticks": {
                ("event_id",),
                ("session_id", "ingest_sequence"),
                ("session_id", "dedupe_key"),
            },
            "connection_events": {
                ("event_id",),
                ("session_id", "ingest_sequence"),
                ("session_id", "dedupe_key"),
            },
        }
        for table, expected in required_uniques.items():
            actual: set[tuple[str, ...]] = set()
            for index in self._connection.execute(f"PRAGMA index_list({table})"):
                if index["unique"]:
                    actual.add(tuple(
                        row["name"]
                        for row in self._connection.execute(
                            f"PRAGMA index_info({index['name']})"
                        )
                    ))
            if not expected <= actual:
                raise SchemaMismatchError(
                    f"table {table} is missing required UNIQUE constraints"
                )
        expected_foreign_keys = {
            "event_log": {
                ("recording_sessions", "session_id", "session_id"),
            },
            "quotes": {
                ("event_log", "event_id", "event_id"),
                ("recording_sessions", "session_id", "session_id"),
            },
            "ticks": {
                ("event_log", "event_id", "event_id"),
                ("recording_sessions", "session_id", "session_id"),
            },
            "connection_events": {
                ("event_log", "event_id", "event_id"),
                ("recording_sessions", "session_id", "session_id"),
            },
        }
        for table, expected in expected_foreign_keys.items():
            actual = {
                (row["table"], row["from"], row["to"])
                for row in self._connection.execute(
                    f"PRAGMA foreign_key_list({table})"
                )
            }
            if not expected <= actual:
                raise SchemaMismatchError(
                    f"table {table} is missing required foreign keys"
                )
        required_enum_checks = {
            ("recording_sessions", "source_mode"): {
                "offline", "replay", "live",
            },
            ("recording_sessions", "status"): {
                "recording", "complete", "degraded", "failed", "incomplete",
            },
            ("event_log", "source_mode"): {"offline", "replay", "live"},
            ("event_log", "event_type"): {
                "connection_status", "server_time", "instrument", "quote",
                "tick", "adapter_diagnostic",
            },
            ("connection_events", "state"): {
                "new", "starting", "com_ready", "logging_in", "logged_in",
                "entering_monitor", "connected", "stocks_ready", "subscribed",
                "disconnected", "reconnecting", "stopping", "error", "stopped",
            },
        }
        for (table, column), expected_literals in required_enum_checks.items():
            row = self._connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            normalized = re.sub(r"\s+", " ", row["sql"].lower())
            matches = re.findall(
                rf"check\s*\(\s*{re.escape(column)}\s+in\s*"
                rf"\(\s*([^)]*?)\s*\)\s*\)",
                normalized,
            )
            literal_sets = [
                set(re.findall(r"'([^']*)'", match))
                for match in matches
            ]
            if literal_sets != [expected_literals]:
                raise SchemaMismatchError(
                    f"table {table} has wrong CHECK set for {column}"
                )
        for table in ("quotes", "ticks"):
            row = self._connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            normalized = re.sub(r"\s+", " ", row["sql"].lower())
            required_boolean_checks = (
                r"check\s*\(\s*is_simulated\s+in\s*\(\s*0\s*,\s*1\s*\)"
                r"\s+or\s+is_simulated\s+is\s+null\s*\)",
                r"check\s*\(\s*is_long_callback\s+in\s*"
                r"\(\s*0\s*,\s*1\s*\)\s*\)",
            )
            for pattern in required_boolean_checks:
                if re.search(pattern, normalized) is None:
                    raise SchemaMismatchError(
                        f"table {table} is missing boolean CHECK constraints"
                    )

    def _require_open(self) -> None:
        if self._closed:
            raise StorageError("repository is closed")

    def stats(self) -> RepositoryStats:
        with self._lock:
            return self._stats

    def begin_session(self, session: RecordingSession) -> None:
        if session.source_mode is SourceMode.REPLAY:
            raise ValueError("replay sessions cannot be recorded")
        if session.schema_version != SCHEMA_VERSION:
            raise SchemaMismatchError(
                f"session schema version must be {SCHEMA_VERSION}"
            )
        with self._lock:
            self._require_open()
            try:
                self._connection.execute(
                    """INSERT INTO recording_sessions(
                    session_id,schema_version,source,source_mode,started_at,
                    trading_day,status,config_fingerprint)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        str(session.session_id),
                        session.schema_version,
                        session.source,
                        session.source_mode.value,
                        session.started_at.isoformat(),
                        session.trading_day.isoformat()
                        if session.trading_day else None,
                        "recording",
                        session.config_fingerprint,
                    ),
                )
            except sqlite3.Error as exc:
                raise StorageError("could not begin recording session") from exc

    def get_session(self, session_id: UUID) -> StoredSession | None:
        with self._lock:
            self._require_open()
            row = self._connection.execute(
                """SELECT session_id,schema_version,source,source_mode,status,
                last_ingest_sequence FROM recording_sessions WHERE session_id=?""",
                (str(session_id),),
            ).fetchone()
            if row is None:
                return None
            return StoredSession(
                UUID(row["session_id"]),
                row["schema_version"],
                row["source"],
                SourceMode(row["source_mode"]),
                row["status"],
                row["last_ingest_sequence"],
            )

    def _prepare(
        self, events: Sequence[MarketDataEnvelope]
    ) -> tuple[StoredSession, list[MarketDataEnvelope], int]:
        if not events:
            raise ValueError("events must not be empty")
        session_id = events[0].session_id
        session = self.get_session(session_id)
        if session is None:
            raise StorageError("recording session does not exist")
        if (
            session.status != "recording"
            and session.session_id not in self._raw_only_sessions
        ):
            raise StorageError("recording session is not active")
        previous = -1
        new: list[MarketDataEnvelope] = []
        duplicates = 0
        seen_dedupe: set[str] = set()
        for event in events:
            if type(event) is not MarketDataEnvelope:
                raise TypeError("events must contain MarketDataEnvelope")
            if (
                event.session_id != session_id
                or event.schema_version != session.schema_version
                or event.source != session.source
                or event.source_mode != session.source_mode
            ):
                raise StorageError("envelope does not match recording session")
            if event.ingest_sequence <= previous:
                raise DuplicateSequenceError("batch sequence is not strictly increasing")
            previous = event.ingest_sequence
            existing_dedupe = (
                event.dedupe_key in seen_dedupe
                or self._connection.execute(
                    "SELECT 1 FROM event_log "
                    "WHERE session_id=? AND dedupe_key=?",
                    (str(session_id), event.dedupe_key),
                ).fetchone()
                is not None
            )
            if existing_dedupe:
                duplicates += 1
                continue
            collision = self._connection.execute(
                "SELECT dedupe_key FROM event_log "
                "WHERE session_id=? AND ingest_sequence=?",
                (str(session_id), event.ingest_sequence),
            ).fetchone()
            if collision is not None:
                raise DuplicateSequenceError("ingest sequence collision")
            if event.ingest_sequence <= session.last_ingest_sequence:
                raise DuplicateSequenceError("ingest sequence precedes checkpoint")
            new.append(event)
            seen_dedupe.add(event.dedupe_key)
        return session, new, duplicates

    def append_batch(self, events: Sequence[MarketDataEnvelope]) -> None:
        with self._lock:
            self._require_open()
            session, new, duplicates = self._prepare(events)
            if not new:
                self._stats = RepositoryStats(
                    self._stats.persisted_events,
                    self._stats.duplicate_events + duplicates,
                    self._stats.projection_failures,
                )
                return
            if session.session_id in self._raw_only_sessions:
                self._append_raw_only(session, new, duplicates)
                return
            begun = False
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                begun = True
                for event in new:
                    event_id = self._insert_event(self._connection, event)
                    try:
                        self._insert_projection(self._connection, event_id, event)
                    except sqlite3.Error as exc:
                        raise _ProjectionFailure from exc
                self._update_checkpoint(session.session_id, new[-1].ingest_sequence)
                self._connection.execute("COMMIT")
                begun = False
                projection_failures = 0
            except _ProjectionFailure:
                if begun:
                    self._connection.execute("ROLLBACK")
                projection_failures = self._recover_projection_batch(session, new)
            except Exception as exc:
                if begun:
                    self._connection.execute("ROLLBACK")
                if isinstance(exc, StorageError):
                    raise
                raise StorageError("authoritative event batch could not be stored") from exc
            self._stats = RepositoryStats(
                self._stats.persisted_events + len(new),
                self._stats.duplicate_events + duplicates,
                self._stats.projection_failures + projection_failures,
            )

    def _append_raw_only(
        self,
        session: StoredSession,
        events: list[MarketDataEnvelope],
        duplicates: int,
    ) -> None:
        begun = False
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            begun = True
            for event in events:
                self._insert_event(self._connection, event)
            self._update_checkpoint(session.session_id, events[-1].ingest_sequence)
            self._connection.execute("COMMIT")
            begun = False
        except Exception as exc:
            if begun:
                self._connection.execute("ROLLBACK")
            if isinstance(exc, StorageError):
                raise
            raise StorageError("raw-only event batch could not be stored") from exc
        self._stats = RepositoryStats(
            self._stats.persisted_events + len(events),
            self._stats.duplicate_events + duplicates,
            self._stats.projection_failures,
        )

    def _insert_event(
        self, connection: sqlite3.Connection, event: MarketDataEnvelope
    ) -> int:
        payload_json, raw_json, checksum = encode_envelope(event)
        values: dict[str, Any] = {
            "session_id": str(event.session_id),
            "ingest_sequence": event.ingest_sequence,
            "schema_version": event.schema_version,
            "event_type": event.event_type.value,
            "source": event.source,
            "source_mode": event.source_mode.value,
            "connection_generation": event.connection_generation,
            "sequence": event.sequence,
            "broker_sequence": event.broker_sequence,
            "dedupe_key": event.dedupe_key,
            "event_at": event.event_at.isoformat() if event.event_at else None,
            "trading_day": (
                event.trading_day.isoformat() if event.trading_day else None
            ),
            "received_at": event.received_at.isoformat(),
            "metadata_version": event.metadata_version,
            "payload_json": payload_json,
            "raw_json": raw_json,
            "payload_sha256": checksum,
        }
        record_checksum = record_sha256(values)
        try:
            cursor = connection.execute(
                """INSERT INTO event_log(
                session_id,ingest_sequence,schema_version,event_type,source,
                source_mode,connection_generation,sequence,broker_sequence,
                dedupe_key,event_at,trading_day,received_at,metadata_version,
                payload_json,raw_json,payload_sha256,record_sha256)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    *(values[field] for field in (
                        "session_id", "ingest_sequence", "schema_version",
                        "event_type", "source", "source_mode",
                        "connection_generation", "sequence", "broker_sequence",
                        "dedupe_key", "event_at", "trading_day", "received_at",
                        "metadata_version", "payload_json", "raw_json",
                        "payload_sha256",
                    )),
                    record_checksum,
                ),
            )
        except sqlite3.Error as exc:
            raise StorageError("authoritative event insert failed") from exc
        return int(cursor.lastrowid)

    def _insert_projection(
        self,
        connection: sqlite3.Connection,
        event_id: int,
        envelope: MarketDataEnvelope,
    ) -> None:
        self._insert_projection_unchecked(connection, event_id, envelope)

    def _insert_projection_unchecked(
        self, c: sqlite3.Connection, event_id: int, e: MarketDataEnvelope
    ) -> None:
        p = e.payload
        if isinstance(p, Instrument):
            c.execute(
                """INSERT INTO instruments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(instrument_id,metadata_version) DO UPDATE SET
                symbol=excluded.symbol,venue=excluded.venue,market_no=excluded.market_no,
                stock_idx=excluded.stock_idx,display_name=excluded.display_name,
                asset_class=excluded.asset_class,currency=excluded.currency,
                price_scale_text=excluded.price_scale_text,
                quantity_scale_text=excluded.quantity_scale_text,
                updated_at=excluded.updated_at,raw_payload_json=excluded.raw_payload_json""",
                (
                    p.instrument_id,p.metadata_version,p.symbol,p.venue,p.market_no,
                    p.stock_idx,p.display_name,p.asset_class,p.currency,
                    str(p.price_scale) if p.price_scale is not None else None,
                    str(p.quantity_scale) if p.quantity_scale is not None else None,
                    p.updated_at.isoformat(),
                    canonical_json(p.raw_payload) if p.raw_payload is not None else None,
                ),
            )
        elif isinstance(p, Quote):
            c.execute(
                """INSERT INTO quotes(
                event_id,session_id,ingest_sequence,schema_version,
                connection_generation,sequence,dedupe_key,instrument_id,
                metadata_version,market_no_raw,stock_idx_raw,bid_raw,ask_raw,last_raw,
                bid_qty_raw,ask_qty_raw,last_qty_raw,bid_normalized_text,
                ask_normalized_text,last_normalized_text,event_at,trading_day,
                received_at,is_simulated,is_long_callback)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id,str(e.session_id),e.ingest_sequence,e.schema_version,
                    e.connection_generation,e.sequence,e.dedupe_key,p.instrument_id,
                    e.metadata_version,p.market_no_raw,p.stock_idx_raw,p.bid_raw,
                    p.ask_raw,p.last_raw,p.bid_qty_raw,p.ask_qty_raw,p.last_qty_raw,
                    _d(p.bid_normalized),_d(p.ask_normalized),_d(p.last_normalized),
                    _dt(p.event_at),_date(p.trading_day),p.received_at.isoformat(),
                    _bool(p.is_simulated),int(p.is_long_callback),
                ),
            )
        elif isinstance(p, Tick):
            c.execute(
                """INSERT INTO ticks(
                event_id,session_id,ingest_sequence,schema_version,
                connection_generation,sequence,dedupe_key,instrument_id,
                metadata_version,market_no_raw,stock_idx_raw,source_pointer_raw,
                date_raw,time_hms_raw,time_subsecond_raw,bid_raw,ask_raw,close_raw,
                quantity_raw,simulate_raw,bid_normalized_text,ask_normalized_text,
                close_normalized_text,quantity_normalized_text,event_at,trading_day,
                received_at,is_simulated,is_long_callback)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id,str(e.session_id),e.ingest_sequence,e.schema_version,
                    e.connection_generation,e.sequence,e.dedupe_key,p.instrument_id,
                    e.metadata_version,p.market_no_raw,p.stock_idx_raw,
                    p.source_pointer_raw,p.date_raw,p.time_hms_raw,
                    p.time_subsecond_raw,p.bid_raw,p.ask_raw,p.close_raw,
                    p.quantity_raw,p.simulate_raw,_d(p.bid_normalized),
                    _d(p.ask_normalized),_d(p.close_normalized),
                    _d(p.quantity_normalized),_dt(p.event_at),_date(p.trading_day),
                    p.received_at.isoformat(),_bool(p.is_simulated),
                    int(p.is_long_callback),
                ),
            )
        elif isinstance(p, ConnectionStatus):
            c.execute(
                """INSERT INTO connection_events(
                event_id,session_id,ingest_sequence,schema_version,
                connection_generation,sequence,dedupe_key,state,broker_kind_raw,
                broker_code_raw,message,is_ready,changed_at,received_at,trading_day)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id,str(e.session_id),e.ingest_sequence,e.schema_version,
                    e.connection_generation,e.sequence,e.dedupe_key,p.state.value,
                    p.broker_kind_raw,p.broker_code_raw,p.message,int(p.is_ready),
                    p.changed_at.isoformat(),e.received_at.isoformat(),
                    _date(e.trading_day),
                ),
            )
        elif isinstance(p, AdapterDiagnostic):
            return

    def _recover_projection_batch(
        self, session: StoredSession, events: list[MarketDataEnvelope]
    ) -> int:
        failures = 0
        begun = False
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            begun = True
            for index, event in enumerate(events):
                event_id = self._insert_event(self._connection, event)
                savepoint = f"projection_{index}"
                self._connection.execute(f"SAVEPOINT {savepoint}")
                try:
                    self._insert_projection(self._connection, event_id, event)
                    self._connection.execute(f"RELEASE {savepoint}")
                except sqlite3.Error:
                    self._connection.execute(f"ROLLBACK TO {savepoint}")
                    self._connection.execute(f"RELEASE {savepoint}")
                    failures += 1
            self._update_checkpoint(session.session_id, events[-1].ingest_sequence)
            self._connection.execute(
                "UPDATE recording_sessions SET status='incomplete' WHERE session_id=?",
                (str(session.session_id),),
            )
            self._connection.execute("COMMIT")
            begun = False
        except Exception as exc:
            if begun:
                self._connection.execute("ROLLBACK")
            raise StorageError("projection recovery could not preserve event log") from exc
        self._raw_only_sessions.add(session.session_id)
        return failures

    def _update_checkpoint(self, session_id: UUID, sequence: int) -> None:
        self._connection.execute(
            "UPDATE recording_sessions SET last_ingest_sequence=? WHERE session_id=?",
            (sequence, str(session_id)),
        )

    def end_session(
        self, session_id: UUID, ended_at: datetime, status: str
    ) -> None:
        """Finalize a session.

        A session degraded into this instance's raw-only recovery mode is
        always finalized as ``incomplete``, regardless of the requested
        terminal status. It can never be promoted after a projection failure.
        """
        if status not in {"complete", "degraded", "failed", "incomplete"}:
            raise ValueError("invalid terminal session status")
        if getattr(ended_at.tzinfo, "key", None) != TAIPEI.key:
            raise ValueError("ended_at must use Asia/Taipei timezone")
        with self._lock:
            self._require_open()
            raw_only = session_id in self._raw_only_sessions
            final_status = "incomplete" if raw_only else status
            expected_current = "incomplete" if raw_only else "recording"
            cursor = self._connection.execute(
                "UPDATE recording_sessions SET ended_at=?,status=? "
                "WHERE session_id=? AND status=?",
                (
                    ended_at.isoformat(),
                    final_status,
                    str(session_id),
                    expected_current,
                ),
            )
            if cursor.rowcount != 1:
                raise StorageError("recording session is missing or not active")
            if raw_only:
                self._raw_only_sessions.remove(session_id)

    def iter_event_rows(
        self,
        session_id: UUID,
        *,
        after_ingest_sequence: int | None = None,
        event_types: set[EventType] | None = None,
    ) -> Iterator[sqlite3.Row]:
        cursor = -1 if after_ingest_sequence is None else after_ingest_sequence
        if type(cursor) is not int or cursor < -1:
            raise ValueError("after_ingest_sequence must be non-negative or None")
        type_values = (
            sorted(item.value for item in event_types)
            if event_types
            else []
        )
        type_clause = ""
        if event_types:
            type_clause = (
                f" AND event_type IN ({','.join('?' for _ in type_values)})"
            )

        def rows() -> Iterator[sqlite3.Row]:
            page_cursor = cursor
            while True:
                params: list[Any] = [str(session_id), page_cursor, *type_values]
                sql = (
                    "SELECT * FROM event_log "
                    "WHERE session_id=? AND ingest_sequence>?"
                    f"{type_clause} ORDER BY ingest_sequence ASC LIMIT 256"
                )
                with self._lock:
                    self._require_open()
                    page = self._connection.execute(sql, params).fetchall()
                if not page:
                    return
                for row in page:
                    yield row
                page_cursor = page[-1]["ingest_sequence"]

        return rows()

    def iter_events(
        self,
        session_id: UUID,
        *,
        after_ingest_sequence: int | None = None,
        event_types: set[EventType] | None = None,
    ) -> Iterator[MarketDataEnvelope]:
        rows = self.iter_event_rows(
            session_id,
            after_ingest_sequence=after_ingest_sequence,
            event_types=event_types,
        )
        def decoded() -> Iterator[MarketDataEnvelope]:
            for row in rows:
                try:
                    yield decode_envelope(row)
                except Exception as exc:
                    raise IntegrityError("stored envelope failed validation") from exc
        return decoded()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def __enter__(self) -> SQLiteMarketDataRepository:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class _ProjectionFailure(Exception):
    pass


def _d(value: object | None) -> str | None:
    return None if value is None else str(value)


def _dt(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _date(value: object | None) -> str | None:
    return None if value is None else value.isoformat()  # type: ignore[union-attr]


def _bool(value: bool | None) -> int | None:
    return None if value is None else int(value)

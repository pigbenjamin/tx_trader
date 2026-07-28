"""Fail-closed SQLite persistence for durable research-paper runs."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import threading
from collections.abc import Mapping
from datetime import datetime
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from types import TracebackType
from uuid import UUID
from zoneinfo import ZoneInfo

from .contracts import (
    CheckpointKind,
    CompleteResearchRun,
    DurableBatchDisposition,
    ResearchDurableBatch,
    ResearchDurableBatchResult,
    ResearchHydrationState,
    ResearchOutboxRecord,
    ResearchOutboxRecordType,
    ResearchPersistenceError,
    ResearchPersistenceErrorCode,
    ResearchRunIdentity,
    ResearchRunState,
    ResearchRunStatus,
    StrategyFingerprint,
    VersionedCheckpoint,
)

APPLICATION_ID = 1415074898
SCHEMA_VERSION = 1
_SCHEMA_TOKEN = "__SCHEMA_CHECKSUM__"
_TAIPEI = ZoneInfo("Asia/Taipei")

_REQUIRED_COLUMNS: Mapping[str, frozenset[str]] = {
    "paper_schema_migrations": frozenset({"version", "checksum"}),
    "research_runs": frozenset(
        {
            "paper_run_id",
            "source_session_id",
            "source_schema_version",
            "source_event_count",
            "source_first_sequence",
            "source_last_sequence",
            "source_content_fingerprint",
            "research_config_fingerprint",
            "execution_config_fingerprint",
            "strategy_fingerprints_json",
            "output_schema_version",
            "broker_algorithm_version",
            "identity_fingerprint",
            "status",
            "state_version",
            "committed_cursor",
            "committed_batch_count",
            "broker_checkpoint_schema_version",
            "broker_checkpoint",
            "broker_checkpoint_sha256",
            "coordinator_checkpoint_schema_version",
            "coordinator_checkpoint",
            "coordinator_checkpoint_sha256",
            "created_at",
            "updated_at",
            "completed_at",
        }
    ),
    "research_batches": frozenset(
        {
            "paper_run_id",
            "source_session_id",
            "source_ingest_sequence",
            "envelope_fingerprint",
            "decision_fingerprint",
            "batch_fingerprint",
            "applied_state_version",
            "committed_at",
        }
    ),
    "research_outbox": frozenset(
        {
            "paper_run_id",
            "output_sequence",
            "record_type",
            "source_ingest_sequence",
            "paper_sequence",
            "payload",
            "payload_sha256",
            "payload_bytes",
            "created_state_version",
        }
    ),
}
_REQUIRED_INDEXES = frozenset(
    {
        "idx_research_batches_state_version",
        "idx_research_outbox_market_source",
        "idx_research_outbox_paper_sequence",
        "idx_research_outbox_single_summary",
    }
)


def _schema_material() -> tuple[str, str]:
    raw = Path(__file__).with_name("paper_schema.sql").read_text(encoding="utf-8")
    if raw.count(_SCHEMA_TOKEN) != 1:
        raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
    checksum = f"sha256:{sha256(raw.encode('utf-8')).hexdigest()}"
    return raw.replace(_SCHEMA_TOKEN, checksum, 1), checksum


@lru_cache(maxsize=1)
def _expected_schema_signature() -> tuple[tuple[str, str, str], ...]:
    script, _ = _schema_material()
    connection = sqlite3.connect(":memory:", isolation_level=None)
    try:
        connection.executescript(script)
        return tuple(
            (row[0], row[1], row[2])
            for row in connection.execute(
                """SELECT type, name, sql FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'index')
                ORDER BY type, name"""
            )
        )
    finally:
        connection.close()


def _timestamp(value: datetime) -> str:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
        or getattr(value.tzinfo, "key", None) != "Asia/Taipei"
    ):
        raise TypeError("timestamp must be an Asia/Taipei datetime")
    return value.isoformat()


def _write_error(exc: sqlite3.Error) -> ResearchPersistenceError:
    code = getattr(exc, "sqlite_errorcode", None)
    if type(code) is int and code & 0xFF == sqlite3.SQLITE_FULL:
        return ResearchPersistenceError(ResearchPersistenceErrorCode.CAPACITY)
    return ResearchPersistenceError(ResearchPersistenceErrorCode.IO_FAILURE)


def _parse_timestamp(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != _TAIPEI.utcoffset(parsed):
        raise ValueError
    return parsed.astimezone(_TAIPEI)


class SQLiteResearchStateRepository:
    """Transactional state with a main-DB page cap.

    ``max_main_database_bytes`` bounds SQLite's logical main database through
    ``max_page_count``. WAL and SHM sidecars are deliberately excluded; this is
    not a hard cap on total filesystem usage.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        max_main_database_bytes: int,
        busy_timeout_ms: int = 5000,
        create_new: bool = False,
        forbidden_file_identity: tuple[int, int] | None = None,
    ) -> None:
        if str(db_path) == ":memory:":
            raise ValueError("in-memory research state is not supported")
        if type(max_main_database_bytes) is not int or max_main_database_bytes <= 0:
            raise ValueError("max_main_database_bytes must be a positive integer")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a non-negative integer")
        if type(create_new) is not bool:
            raise TypeError("create_new must be bool")
        if forbidden_file_identity is not None and (
            type(forbidden_file_identity) is not tuple
            or len(forbidden_file_identity) != 2
            or any(type(item) is not int or item < 0 for item in forbidden_file_identity)
        ):
            raise TypeError("forbidden_file_identity must be two non-negative integers")
        self._path = Path(db_path)
        self._lock = threading.RLock()
        self._closed = False
        self._poisoned = False
        reservation_fd: int | None = None
        existed = not create_new
        try:
            reservation_fd = self._reserve_and_verify_path(
                create_new=create_new,
                forbidden_file_identity=forbidden_file_identity,
            )
            self._connection = sqlite3.connect(
                self._path,
                isolation_level=None,
                check_same_thread=False,
            )
            self._verify_reserved_identity(reservation_fd)
            self._connection.row_factory = sqlite3.Row
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.execute("PRAGMA journal_mode = WAL")
            page_size = int(self._connection.execute("PRAGMA page_size").fetchone()[0])
            max_pages = max_main_database_bytes // page_size
            if max_pages < 1:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CAPACITY)
            applied_max_pages = int(
                self._connection.execute(f"PRAGMA max_page_count = {max_pages}").fetchone()[0]
            )
            if applied_max_pages > max_pages:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CAPACITY)
            if existed:
                self._validate_schema()
            else:
                self._initialize_schema()
        except ResearchPersistenceError:
            self._close_after_failure()
            raise
        except sqlite3.DatabaseError as exc:
            self._close_after_failure()
            if (
                type(getattr(exc, "sqlite_errorcode", None)) is int
                and exc.sqlite_errorcode & 0xFF == sqlite3.SQLITE_FULL
            ):
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CAPACITY) from None
            raise ResearchPersistenceError(
                ResearchPersistenceErrorCode.CORRUPT
                if existed
                else ResearchPersistenceErrorCode.IO_FAILURE
            ) from None
        except (OSError, ValueError, TypeError):
            self._close_after_failure()
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.IO_FAILURE) from None
        finally:
            if reservation_fd is not None:
                try:
                    os.close(reservation_fd)
                except OSError:
                    pass

    @property
    def max_main_database_bytes(self) -> int:
        """Return this connection's effective logical main-database cap."""

        with self._lock:
            self._ensure_open()
            page_size = int(self._connection.execute("PRAGMA page_size").fetchone()[0])
            max_pages = int(self._connection.execute("PRAGMA max_page_count").fetchone()[0])
            return page_size * max_pages

    def _reserve_and_verify_path(
        self,
        *,
        create_new: bool,
        forbidden_file_identity: tuple[int, int] | None,
    ) -> int:
        binary = getattr(os, "O_BINARY", 0)
        noinherit = getattr(os, "O_NOINHERIT", 0)
        if create_new:
            try:
                descriptor = os.open(
                    self._path,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | binary | noinherit,
                    0o600,
                )
            except FileExistsError:
                raise ResearchPersistenceError(
                    ResearchPersistenceErrorCode.ALREADY_EXISTS
                ) from None
            except OSError:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.IO_FAILURE) from None
        else:
            try:
                before = os.lstat(self._path)
                if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
                descriptor = os.open(self._path, os.O_RDONLY | binary | noinherit)
            except FileNotFoundError:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.NOT_FOUND) from None
            except ResearchPersistenceError:
                raise
            except OSError:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.IO_FAILURE) from None
        try:
            self._verify_reserved_identity(descriptor)
            reserved = os.fstat(descriptor)
            if forbidden_file_identity == (reserved.st_dev, reserved.st_ino):
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def _verify_reserved_identity(self, descriptor: int) -> None:
        try:
            reserved = os.fstat(descriptor)
            current = os.lstat(self._path)
        except OSError:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT) from None
        if (
            stat.S_ISLNK(current.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or not stat.S_ISREG(reserved.st_mode)
            or reserved.st_dev != current.st_dev
            or reserved.st_ino != current.st_ino
        ):
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)

    def _close_after_failure(self) -> None:
        connection = getattr(self, "_connection", None)
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        self._closed = True

    def _initialize_schema(self) -> None:
        script, _ = _schema_material()
        try:
            self._connection.executescript(script)
            self._validate_schema()
        except Exception:
            try:
                self._connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def _validate_schema(self) -> None:
        _, checksum = _schema_material()
        if int(self._connection.execute("PRAGMA application_id").fetchone()[0]) != APPLICATION_ID:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
        if int(self._connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
        quick = self._connection.execute("PRAGMA quick_check").fetchone()
        if quick is None or quick[0] != "ok":
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.CORRUPT)
        objects = {
            (row["type"], row["name"])
            for row in self._connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        if ("table", "paper_schema_migrations") not in objects:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
        rows = self._connection.execute(
            "SELECT version, checksum FROM paper_schema_migrations ORDER BY version"
        ).fetchall()
        if [(row[0], row[1]) for row in rows] != [(SCHEMA_VERSION, checksum)]:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
        for table, expected in _REQUIRED_COLUMNS.items():
            if ("table", table) not in objects:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
            actual = frozenset(
                row["name"] for row in self._connection.execute(f"PRAGMA table_info({table})")
            )
            if actual != expected:
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
        if not all(("index", index) in objects for index in _REQUIRED_INDEXES):
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)
        signature = tuple(
            (row[0], row[1], row[2])
            for row in self._connection.execute(
                """SELECT type, name, sql FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%' AND type IN ('table', 'index')
                ORDER BY type, name"""
            )
        )
        if signature != _expected_schema_signature():
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.SCHEMA_MISMATCH)

    def _ensure_open(self) -> None:
        if self._closed or self._poisoned:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.CLOSED)

    def _transaction(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

    def _rollback(self) -> None:
        try:
            self._connection.execute("ROLLBACK")
        except sqlite3.Error:
            self._poisoned = True
            self._close_after_failure()

    def _commit(self) -> None:
        try:
            self._connection.execute("COMMIT")
        except sqlite3.Error as exc:
            self._poisoned = True
            self._close_after_failure()
            raise _write_error(exc) from None

    def create_run(
        self,
        identity: ResearchRunIdentity,
        broker_checkpoint: VersionedCheckpoint,
        coordinator_checkpoint: VersionedCheckpoint,
        created_at: datetime,
    ) -> ResearchHydrationState:
        if type(identity) is not ResearchRunIdentity:
            raise TypeError("identity must be ResearchRunIdentity")
        if broker_checkpoint.kind is not CheckpointKind.BROKER:
            raise ValueError("broker_checkpoint must be a broker checkpoint")
        if coordinator_checkpoint.kind is not CheckpointKind.COORDINATOR:
            raise ValueError("coordinator_checkpoint must be a coordinator checkpoint")
        timestamp = _timestamp(created_at)
        strategies = json.dumps(
            [
                {"fingerprint": item.fingerprint, "strategy_id": item.strategy_id}
                for item in identity.strategy_fingerprints
            ],
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock:
            self._ensure_open()
            try:
                self._transaction()
                self._connection.execute(
                    """INSERT INTO research_runs (
                    paper_run_id, source_session_id, source_schema_version,
                    source_event_count, source_first_sequence, source_last_sequence,
                    source_content_fingerprint, research_config_fingerprint,
                    execution_config_fingerprint, strategy_fingerprints_json,
                    output_schema_version, broker_algorithm_version,
                    identity_fingerprint, status, state_version, committed_cursor,
                    committed_batch_count, broker_checkpoint_schema_version,
                    broker_checkpoint, broker_checkpoint_sha256,
                    coordinator_checkpoint_schema_version, coordinator_checkpoint,
                    coordinator_checkpoint_sha256, created_at, updated_at, completed_at
                    ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        str(identity.paper_run_id),
                        str(identity.source_session_id),
                        identity.source_schema_version,
                        identity.source_event_count,
                        identity.source_first_sequence,
                        identity.source_last_sequence,
                        identity.source_content_fingerprint,
                        identity.research_config_fingerprint,
                        identity.execution_config_fingerprint,
                        strategies,
                        identity.output_schema_version,
                        identity.broker_algorithm_version,
                        identity.identity_fingerprint,
                        ResearchRunStatus.ACTIVE.value,
                        0,
                        None,
                        0,
                        broker_checkpoint.schema_version,
                        broker_checkpoint.payload,
                        broker_checkpoint.payload_sha256,
                        coordinator_checkpoint.schema_version,
                        coordinator_checkpoint.payload,
                        coordinator_checkpoint.payload_sha256,
                        timestamp,
                        timestamp,
                        None,
                    ),
                )
                self._commit()
            except sqlite3.IntegrityError:
                self._rollback()
                raise ResearchPersistenceError(
                    ResearchPersistenceErrorCode.ALREADY_EXISTS
                ) from None
            except ResearchPersistenceError:
                raise
            except sqlite3.Error as exc:
                self._rollback()
                raise _write_error(exc) from None
            return self.load_run(identity.paper_run_id)

    def _decode_run(self, row: sqlite3.Row) -> ResearchHydrationState:
        strategies_raw = json.loads(row["strategy_fingerprints_json"])
        if type(strategies_raw) is not list:
            raise ValueError
        strategies = tuple(
            StrategyFingerprint(strategy_id=item["strategy_id"], fingerprint=item["fingerprint"])
            for item in strategies_raw
            if type(item) is dict and set(item) == {"strategy_id", "fingerprint"}
        )
        if len(strategies) != len(strategies_raw):
            raise ValueError
        identity = ResearchRunIdentity(
            paper_run_id=UUID(row["paper_run_id"]),
            source_session_id=UUID(row["source_session_id"]),
            source_schema_version=row["source_schema_version"],
            source_event_count=row["source_event_count"],
            source_first_sequence=row["source_first_sequence"],
            source_last_sequence=row["source_last_sequence"],
            source_content_fingerprint=row["source_content_fingerprint"],
            research_config_fingerprint=row["research_config_fingerprint"],
            execution_config_fingerprint=row["execution_config_fingerprint"],
            strategy_fingerprints=strategies,
            output_schema_version=row["output_schema_version"],
            broker_algorithm_version=row["broker_algorithm_version"],
        )
        if identity.identity_fingerprint != row["identity_fingerprint"]:
            raise ValueError
        broker = VersionedCheckpoint(
            kind=CheckpointKind.BROKER,
            schema_version=row["broker_checkpoint_schema_version"],
            payload=bytes(row["broker_checkpoint"]),
            payload_sha256=row["broker_checkpoint_sha256"],
        )
        coordinator = VersionedCheckpoint(
            kind=CheckpointKind.COORDINATOR,
            schema_version=row["coordinator_checkpoint_schema_version"],
            payload=bytes(row["coordinator_checkpoint"]),
            payload_sha256=row["coordinator_checkpoint_sha256"],
        )
        state = ResearchRunState(
            identity=identity,
            status=ResearchRunStatus(row["status"]),
            state_version=row["state_version"],
            committed_cursor=row["committed_cursor"],
            committed_batch_count=row["committed_batch_count"],
            created_at=_parse_timestamp(row["created_at"]),
            updated_at=_parse_timestamp(row["updated_at"]),
            completed_at=(
                None if row["completed_at"] is None else _parse_timestamp(row["completed_at"])
            ),
        )
        return ResearchHydrationState(state, broker, coordinator)

    def load_run(self, paper_run_id: UUID) -> ResearchHydrationState:
        if type(paper_run_id) is not UUID:
            raise TypeError("paper_run_id must be UUID")
        with self._lock:
            self._ensure_open()
            try:
                row = self._connection.execute(
                    "SELECT * FROM research_runs WHERE paper_run_id=?",
                    (str(paper_run_id),),
                ).fetchone()
                if row is None:
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.NOT_FOUND)
                hydration = self._decode_run(row)
                ledger = self._connection.execute(
                    """SELECT COUNT(*) AS count,
                    MIN(source_ingest_sequence) AS first_sequence,
                    MAX(source_ingest_sequence) AS last_sequence,
                    MAX(applied_state_version) AS last_version
                    FROM research_batches WHERE paper_run_id=?""",
                    (str(paper_run_id),),
                ).fetchone()
                state = hydration.run_state
                if (
                    ledger["count"] != state.committed_batch_count
                    or (ledger["count"] == 0 and state.committed_cursor is not None)
                    or (
                        ledger["count"] > 0
                        and (
                            ledger["last_sequence"] != state.committed_cursor
                            or ledger["first_sequence"] < state.identity.source_first_sequence
                            or ledger["last_version"] != state.committed_batch_count
                        )
                    )
                    or state.state_version
                    != state.committed_batch_count
                    + (1 if state.status is ResearchRunStatus.COMPLETE else 0)
                ):
                    raise ValueError
                return hydration
            except ResearchPersistenceError:
                raise
            except (
                sqlite3.Error,
                ValueError,
                TypeError,
                KeyError,
                json.JSONDecodeError,
                RecursionError,
            ):
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CORRUPT) from None

    def commit_batch(
        self, batch: ResearchDurableBatch, committed_at: datetime
    ) -> ResearchDurableBatchResult:
        if type(batch) is not ResearchDurableBatch:
            raise TypeError("batch must be ResearchDurableBatch")
        timestamp = _timestamp(committed_at)
        with self._lock:
            self._ensure_open()
            try:
                self._transaction()
                existing = self._connection.execute(
                    "SELECT batch_fingerprint FROM research_batches "
                    "WHERE paper_run_id=? AND source_ingest_sequence=?",
                    (str(batch.paper_run_id), batch.source_ingest_sequence),
                ).fetchone()
                if existing is not None:
                    if existing[0] != batch.batch_fingerprint:
                        raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
                    self._rollback()
                    return ResearchDurableBatchResult(
                        DurableBatchDisposition.DUPLICATE,
                        self.load_run(batch.paper_run_id).run_state,
                    )
                run = self._connection.execute(
                    "SELECT * FROM research_runs WHERE paper_run_id=?",
                    (str(batch.paper_run_id),),
                ).fetchone()
                if run is None:
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.NOT_FOUND)
                if (
                    run["status"] != ResearchRunStatus.ACTIVE.value
                    or run["source_session_id"] != str(batch.source_session_id)
                    or run["state_version"] != batch.expected_state_version
                    or run["committed_cursor"] != batch.expected_previous_cursor
                    or batch.source_ingest_sequence
                    <= (
                        run["committed_cursor"]
                        if run["committed_cursor"] is not None
                        else run["source_first_sequence"] - 1
                    )
                    or batch.source_ingest_sequence > run["source_last_sequence"]
                ):
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
                next_version = run["state_version"] + 1
                self._connection.execute(
                    "INSERT INTO research_batches VALUES (?,?,?,?,?,?,?,?)",
                    (
                        str(batch.paper_run_id),
                        str(batch.source_session_id),
                        batch.source_ingest_sequence,
                        batch.envelope_fingerprint,
                        batch.decision_fingerprint,
                        batch.batch_fingerprint,
                        next_version,
                        timestamp,
                    ),
                )
                for record in batch.outbox_records:
                    self._connection.execute(
                        "INSERT INTO research_outbox VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            str(record.paper_run_id),
                            record.output_sequence,
                            record.record_type.value,
                            record.source_ingest_sequence,
                            record.paper_sequence,
                            record.payload,
                            record.payload_sha256,
                            len(record.payload),
                            next_version,
                        ),
                    )
                changed = self._connection.execute(
                    """UPDATE research_runs SET
                    state_version=?, committed_cursor=?,
                    committed_batch_count=committed_batch_count+1,
                    broker_checkpoint_schema_version=?, broker_checkpoint=?,
                    broker_checkpoint_sha256=?,
                    coordinator_checkpoint_schema_version=?,
                    coordinator_checkpoint=?, coordinator_checkpoint_sha256=?,
                    updated_at=?
                    WHERE paper_run_id=? AND status='active' AND state_version=?""",
                    (
                        next_version,
                        batch.source_ingest_sequence,
                        batch.broker_checkpoint.schema_version,
                        batch.broker_checkpoint.payload,
                        batch.broker_checkpoint.payload_sha256,
                        batch.coordinator_checkpoint.schema_version,
                        batch.coordinator_checkpoint.payload,
                        batch.coordinator_checkpoint.payload_sha256,
                        timestamp,
                        str(batch.paper_run_id),
                        batch.expected_state_version,
                    ),
                ).rowcount
                if changed != 1:
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
                self._commit()
            except ResearchPersistenceError:
                self._rollback()
                raise
            except sqlite3.IntegrityError:
                self._rollback()
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT) from None
            except sqlite3.Error as exc:
                self._rollback()
                raise _write_error(exc) from None
            return ResearchDurableBatchResult(
                DurableBatchDisposition.APPLIED,
                self.load_run(batch.paper_run_id).run_state,
            )

    def complete_run(self, request: CompleteResearchRun) -> ResearchRunState:
        if type(request) is not CompleteResearchRun:
            raise TypeError("request must be CompleteResearchRun")
        timestamp = _timestamp(request.completed_at)
        with self._lock:
            self._ensure_open()
            try:
                self._transaction()
                run = self._connection.execute(
                    "SELECT * FROM research_runs WHERE paper_run_id=?",
                    (str(request.paper_run_id),),
                ).fetchone()
                if run is None:
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.NOT_FOUND)
                if (
                    run["status"] != ResearchRunStatus.ACTIVE.value
                    or run["state_version"] != request.expected_state_version
                    or run["committed_cursor"] != request.expected_previous_cursor
                    or run["committed_cursor"] != run["source_last_sequence"]
                    or run["committed_batch_count"] != run["source_event_count"]
                ):
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
                maximum = self._connection.execute(
                    "SELECT MAX(output_sequence) FROM research_outbox WHERE paper_run_id=?",
                    (str(request.paper_run_id),),
                ).fetchone()[0]
                existing_outbox = self._connection.execute(
                    """SELECT output_sequence, record_type
                    FROM research_outbox WHERE paper_run_id=?
                    ORDER BY output_sequence""",
                    (str(request.paper_run_id),),
                ).fetchall()
                sequences = tuple(row["output_sequence"] for row in existing_outbox)
                types = tuple(row["record_type"] for row in existing_outbox)
                if (
                    maximum is None
                    or sequences != tuple(range(len(sequences)))
                    or request.summary_record.output_sequence != len(sequences)
                    or any(item == ResearchOutboxRecordType.SUMMARY.value for item in types)
                    or any(
                        left == ResearchOutboxRecordType.PAPER.value
                        and right == ResearchOutboxRecordType.MARKET.value
                        for left, right in zip(types, types[1:], strict=False)
                    )
                ):
                    raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
                next_version = run["state_version"] + 1
                record = request.summary_record
                self._connection.execute(
                    "INSERT INTO research_outbox VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        str(record.paper_run_id),
                        record.output_sequence,
                        record.record_type.value,
                        None,
                        None,
                        record.payload,
                        record.payload_sha256,
                        len(record.payload),
                        next_version,
                    ),
                )
                self._connection.execute(
                    """UPDATE research_runs SET status='complete', state_version=?,
                    updated_at=?, completed_at=? WHERE paper_run_id=?""",
                    (next_version, timestamp, timestamp, str(request.paper_run_id)),
                )
                self._commit()
            except ResearchPersistenceError:
                self._rollback()
                raise
            except sqlite3.IntegrityError:
                self._rollback()
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT) from None
            except sqlite3.Error as exc:
                self._rollback()
                raise _write_error(exc) from None
            return self.load_run(request.paper_run_id).run_state

    def read_outbox(self, paper_run_id: UUID) -> tuple[ResearchOutboxRecord, ...]:
        hydration = self.load_run(paper_run_id)
        if hydration.run_state.status is not ResearchRunStatus.COMPLETE:
            raise ResearchPersistenceError(ResearchPersistenceErrorCode.CONFLICT)
        with self._lock:
            self._ensure_open()
            try:
                rows = self._connection.execute(
                    "SELECT * FROM research_outbox WHERE paper_run_id=? ORDER BY output_sequence",
                    (str(paper_run_id),),
                ).fetchall()
                records = tuple(
                    ResearchOutboxRecord(
                        paper_run_id=paper_run_id,
                        output_sequence=row["output_sequence"],
                        record_type=ResearchOutboxRecordType(row["record_type"]),
                        source_ingest_sequence=row["source_ingest_sequence"],
                        paper_sequence=row["paper_sequence"],
                        payload=bytes(row["payload"]),
                        payload_sha256=row["payload_sha256"],
                    )
                    for row in rows
                )
                sequences = tuple(record.output_sequence for record in records)
                if sequences != tuple(range(len(records))):
                    raise ValueError
                types = tuple(record.record_type for record in records)
                if (
                    not types
                    or types[-1] is not ResearchOutboxRecordType.SUMMARY
                    or types.count(ResearchOutboxRecordType.SUMMARY) != 1
                    or any(
                        left is ResearchOutboxRecordType.PAPER
                        and right is ResearchOutboxRecordType.MARKET
                        for left, right in zip(types, types[1:], strict=False)
                    )
                ):
                    raise ValueError
                return records
            except (sqlite3.Error, ValueError, TypeError):
                raise ResearchPersistenceError(ResearchPersistenceErrorCode.CORRUPT) from None

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._connection.close()
            except sqlite3.Error:
                pass
            self._closed = True

    def __enter__(self) -> SQLiteResearchStateRepository:
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

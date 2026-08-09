"""Fail-closed SQLite implementation of the durable live-order journal.

Importing this module is side-effect free.  A database is opened only when
``SqliteLiveOrderJournal`` is instantiated.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Context, Decimal, localcontext
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from threading import RLock
from typing import Iterator, cast

from .live_contracts import (
    AmendOrderCommand,
    CancelOrderCommand,
    CorrelationStatus,
    DecreaseOrderCommand,
    DispatchReceipt,
    FingerprintDomain,
    LiveCommand,
    LiveFailureCode,
    LiveFill,
    LiveOrder,
    LiveOrderState,
    LiveSide,
    NewOrderCommand,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    StrategyPositionAttribution,
    broker_semantic_fingerprint,
    canonical_bytes,
    payload_fingerprint,
)
from .live_journal_codec import (
    MAX_CODEC_PAYLOAD_BYTES,
    LiveJournalCodecError,
    decode_journal_value,
    encode_journal_value,
    journal_digest,
)
from .live_journal_contracts import (
    CommandRegistrationResult,
    DispatchReceiptRecordResult,
    DurableReconciliationRequirement,
    JournalOpenMode,
    LiveJournalCapacityError,
    LiveJournalClosedError,
    LiveJournalConflictError,
    LiveJournalIdentity,
    LiveJournalIntegrityError,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
    ReceiptRecordDisposition,
    RegistrationDisposition,
)
from .live_ports import (
    AmbiguousObservation,
    DispatchClaim,
    DispatchClaimDisposition,
    EventApplicationDisposition,
    EventApplicationResult,
    JournalAppendDisposition,
    JournalAppendResult,
    RawBrokerObservation,
)
from .live_reconciliation_contracts import LocalReconciliationSnapshot, exact_decimal_sum
from .live_reconciliation import assess_reconciliation
from .live_reconciliation_commit_contracts import (
    ClaimResolution,
    DurableReconciliationCommitRequest,
    DurableReconciliationCommitResult,
    ObservationResolution,
    ReconciliationCommitDisposition,
)
from .live_reconciliation_projection import project_authoritative_orders
from .live_reconciliation_projection_contracts import OrderProjectionDisposition
from .live_state_machine import (
    AppliedEvent,
    AppliedEventLedger,
    ReductionDisposition,
    reduce_broker_fill_event,
    reduce_broker_order_event,
    reduce_dispatch,
)

APPLICATION_ID = 1_415_074_890
DATABASE_SCHEMA_VERSION = 2
DEFAULT_BUSY_TIMEOUT_MS = 5_000
DEFAULT_MAX_MAIN_DATABASE_BYTES = 256 * 1024 * 1024

_SQLITE_CONNECT = sqlite3.connect

_ORDER_DOMAIN = "tx_trade.live.journal.order.v1"
_IDENTITY_DOMAIN = "tx_trade.live.journal.identity.v1"
_COMMAND_DOMAIN = "tx_trade.live.journal.command.v1"
_CLAIM_DOMAIN = "tx_trade.live.journal.dispatch-claim.v1"
_RECEIPT_DOMAIN = "tx_trade.live.journal.receipt.v1"
_RAW_DOMAIN = "tx_trade.live.journal.raw-observation.v1"
_EVENT_DOMAIN = "tx_trade.live.journal.normalized-event.v1"
_FILL_DOMAIN = "tx_trade.live.journal.fill.v1"
_RECONCILIATION_DOMAIN = "tx_trade.live.journal.reconciliation.v1"
_APPLICATION_FACT_DOMAIN = "tx_trade.live.journal.application-fact.v1"
_RESOLUTION_FACT_DOMAIN = "tx_trade.live.journal.observation-resolution.v1"
_RECOVERY_BLOCKER_DOMAIN = b"tx_trade.live.journal.recovery-blocker.v1"
_SCHEMA_MIGRATION_FACT_DOMAIN = "tx_trade.live.journal.schema-migration.v2"
_COMMIT_REQUEST_DOMAIN = "tx_trade.live.journal.reconciliation-commit-request.v2"
_COMMIT_FACT_DOMAIN = "tx_trade.live.journal.reconciliation-commit.v2"
_CLAIM_RESOLUTION_DOMAIN = "tx_trade.live.journal.dispatch-claim-resolution.v2"
_OBSERVATION_RECONCILIATION_DOMAIN = (
    "tx_trade.live.journal.observation-reconciliation-resolution.v2"
)
_REQUIREMENT_RESOLUTION_DOMAIN = "tx_trade.live.journal.reconciliation-requirement-resolution.v2"
_SCHEMA_MIGRATION_RECORD_ID = str(DATABASE_SCHEMA_VERSION)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")

_COMMAND_DOMAINS = {
    NewOrderCommand: FingerprintDomain.NEW_COMMAND_V1,
    CancelOrderCommand: FingerprintDomain.CANCEL_COMMAND_V1,
    AmendOrderCommand: FingerprintDomain.AMEND_COMMAND_V1,
    DecreaseOrderCommand: FingerprintDomain.DECREASE_COMMAND_V1,
}

_ACTIVE_STATES = tuple(
    state.value
    for state in LiveOrderState
    if state
    not in {
        LiveOrderState.FILLED,
        LiveOrderState.REJECTED,
        LiveOrderState.CANCELLED,
    }
)


def _timestamp(value: datetime) -> str:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() != timezone.utc.utcoffset(value)
    ):
        raise ValueError("timestamp must use UTC")
    return value.isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise LiveJournalIntegrityError("live journal integrity check failed")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError:
        raise LiveJournalIntegrityError("live journal integrity check failed") from None
    if _timestamp(parsed) != value:
        raise LiveJournalIntegrityError("live journal integrity check failed")
    return parsed


def _sql_material(filename: str) -> tuple[str, str]:
    try:
        raw = Path(__file__).with_name(filename).read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise LiveJournalIntegrityError("live journal schema is unavailable") from None
    fingerprint = f"sha256:{sha256(raw.encode('utf-8')).hexdigest()}"
    return raw, fingerprint


def _schema_material() -> tuple[str, str]:
    return _sql_material("live_journal_schema.sql")


def _v1_schema_material() -> tuple[str, str]:
    return _sql_material("live_journal_schema_v1.sql")


def _migration_material() -> str:
    return _sql_material("live_journal_migration_v1_to_v2.sql")[0]


def _expected_schema_signature(
    version: int = DATABASE_SCHEMA_VERSION,
) -> tuple[tuple[str, str, str], ...]:
    script, _ = _schema_material() if version == 2 else _v1_schema_material()
    # Schema material is checked in an isolated in-memory database.  Retaining
    # the stdlib constructor keeps source-connection auditing scoped to the
    # actual journal connection.
    connection = _SQLITE_CONNECT(":memory:", isolation_level=None)
    try:
        connection.executescript(script)
        return tuple(
            cast(tuple[str, str, str], tuple(row))
            for row in connection.execute(
                """SELECT type, name, sql FROM sqlite_master
                   WHERE name NOT LIKE 'sqlite_%'
                     AND type IN ('table', 'index', 'view', 'trigger')
                   ORDER BY type, name"""
            )
        )
    except sqlite3.Error:
        raise LiveJournalIntegrityError("live journal schema is invalid") from None
    finally:
        connection.close()


def _execute_script_in_transaction(connection: sqlite3.Connection, script: str) -> None:
    """Execute a trusted SQL artifact without sqlite3.executescript's implicit commit."""

    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            sql = statement.strip()
            statement = ""
            if sql:
                connection.execute(sql)
    if statement.strip():
        raise LiveJournalIntegrityError("live journal schema is invalid")


def _command_fingerprint(command: LiveCommand) -> str:
    domain = _COMMAND_DOMAINS.get(type(command))
    if domain is None:
        raise TypeError("command must be an exact live command")
    return payload_fingerprint(command, domain)


def _command_order_id(command: LiveCommand) -> str:
    if type(command) is NewOrderCommand:
        return command.intent.client_order_id
    if type(command) is CancelOrderCommand:
        return command.client_order_id
    if type(command) is AmendOrderCommand:
        return command.client_order_id
    if type(command) is DecreaseOrderCommand:
        return command.client_order_id
    raise TypeError("command must be an exact live command")


def _encode(value: object, domain: str) -> tuple[bytes, str]:
    try:
        payload = encode_journal_value(value)
        return payload, journal_digest(domain, payload)
    except LiveJournalCodecError:
        raise LiveJournalIntegrityError("live journal payload is invalid") from None


def _decode(
    payload: object,
    digest: object,
    expected_type: type[object] | tuple[type[object], ...],
    domain: str,
) -> object:
    if type(payload) is not bytes or type(digest) is not str:
        raise LiveJournalIntegrityError("live journal integrity check failed")
    try:
        return decode_journal_value(
            payload,
            expected_type,
            domain=domain,
            expected_digest=digest,
        )
    except LiveJournalCodecError:
        raise LiveJournalIntegrityError("live journal integrity check failed") from None


def _sqlite_failure(
    exc: sqlite3.Error,
) -> LiveJournalIntegrityError | LiveJournalCapacityError:
    code = getattr(exc, "sqlite_errorcode", None)
    if type(code) is int and code & 0xFF == sqlite3.SQLITE_FULL:
        return LiveJournalCapacityError("live journal capacity exceeded")
    return LiveJournalIntegrityError("live journal operation failed")


def _scalar_digest(domain: str, values: dict[str, str | int | None]) -> str:
    payload = json.dumps(
        values,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return f"sha256:{sha256(domain.encode('ascii') + bytes((0,)) + payload).hexdigest()}"


def _application_digest(
    *,
    event_payload_digest: str,
    raw_observation_id: str,
    client_order_id: str | None,
    disposition: str,
    failure_code: str | None,
    applied_at: str,
) -> str:
    return _scalar_digest(
        _APPLICATION_FACT_DOMAIN,
        {
            "applied_at": applied_at,
            "client_order_id": client_order_id,
            "disposition": disposition,
            "event_payload_digest": event_payload_digest,
            "failure_code": failure_code,
            "raw_observation_id": raw_observation_id,
        },
    )


def _resolution_digest(observation_id: str, resolution_status: str, resolved_at: str) -> str:
    return _scalar_digest(
        _RESOLUTION_FACT_DOMAIN,
        {
            "observation_id": observation_id,
            "resolution_status": resolution_status,
            "resolved_at": resolved_at,
        },
    )


def _recovery_blocker(kind: str, durable_id: str) -> str:
    digest = sha256(
        _RECOVERY_BLOCKER_DOMAIN
        + b"\x00"
        + kind.encode("ascii")
        + b"\x00"
        + durable_id.encode("ascii")
    ).hexdigest()
    return f"recovery:{kind}:{digest}"


class _CasMismatch(Exception):
    pass


class SqliteLiveOrderJournal:
    """One-connection, transactionally fenced live-order journal.

    Local path checks narrow symlink, reparse-point, hard-link, and sidecar
    attacks.  They cannot provide a cross-host filesystem lease; callers must
    place the journal in a private, trusted local directory.
    """

    def __init__(
        self,
        path: str | Path,
        mode: JournalOpenMode,
        *,
        clock: Callable[[], datetime],
        claim_token_factory: Callable[[], str],
        journal_id: str | None = None,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
        max_main_database_bytes: int = DEFAULT_MAX_MAIN_DATABASE_BYTES,
    ) -> None:
        if str(path) == ":memory:":
            raise ValueError("in-memory live journals are not supported")
        if type(mode) is not JournalOpenMode:
            raise TypeError("mode must be JournalOpenMode")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a non-negative integer")
        if type(max_main_database_bytes) is not int or max_main_database_bytes < 1:
            raise ValueError("max_main_database_bytes must be positive")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if not callable(claim_token_factory):
            raise TypeError("claim_token_factory must be callable")
        if mode is JournalOpenMode.CREATE_NEW and journal_id is None:
            raise ValueError("journal_id is required for CREATE_NEW")
        if mode is JournalOpenMode.RESUME and journal_id is not None:
            raise ValueError("journal_id must be omitted for RESUME")

        self._path = Path(path)
        self._lock = RLock()
        self._closed = False
        self._poisoned = False
        self._clock = clock
        self._claim_token_factory = claim_token_factory
        self._connection: sqlite3.Connection | None = None
        descriptor: int | None = None
        created = mode is JournalOpenMode.CREATE_NEW
        try:
            descriptor = self._reserve_path(created)
            if not created:
                self._prevalidate_resume()
            connection = sqlite3.connect(
                self._path,
                isolation_level=None,
                check_same_thread=False,
            )
            self._connection = connection
            connection.row_factory = sqlite3.Row
            self._verify_path_identity(descriptor)
            self._verify_database_path()
            self._configure(busy_timeout_ms, max_main_database_bytes)
            if created:
                assert journal_id is not None
                self._initialize(journal_id)
            else:
                self._resume_or_migrate()
            self._identity = self._load_identity()
        except (LiveJournalIntegrityError, LiveJournalCapacityError):
            self._close_after_failure()
            raise
        except sqlite3.Error as exc:
            self._close_after_failure()
            raise _sqlite_failure(exc) from None
        except (OSError, ValueError):
            self._close_after_failure()
            raise LiveJournalIntegrityError("live journal could not be opened") from None
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    @property
    def identity(self) -> LiveJournalIdentity:
        with self._lock, self._operation():
            return self._identity

    def _reserve_path(self, create: bool) -> int:
        binary = getattr(os, "O_BINARY", 0)
        noinherit = getattr(os, "O_NOINHERIT", 0)
        self._verify_trusted_parent()
        self._verify_sidecars()
        if create:
            try:
                return os.open(
                    self._path,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | binary | noinherit,
                    0o600,
                )
            except FileExistsError:
                raise LiveJournalConflictError("live journal already exists") from None
            except OSError:
                raise LiveJournalIntegrityError("live journal could not be created") from None
        try:
            current = os.lstat(self._path)
            if self._unsafe_file(current):
                raise LiveJournalIntegrityError("live journal path is unsafe")
            return os.open(self._path, os.O_RDONLY | binary | noinherit)
        except FileNotFoundError:
            raise LiveJournalIntegrityError("live journal does not exist") from None
        except LiveJournalIntegrityError:
            raise
        except OSError:
            raise LiveJournalIntegrityError("live journal could not be opened") from None

    def _verify_path_identity(self, descriptor: int) -> None:
        try:
            reserved = os.fstat(descriptor)
            current = os.lstat(self._path)
        except OSError:
            raise LiveJournalIntegrityError("live journal path changed") from None
        if (
            self._unsafe_file(current)
            or self._unsafe_file(reserved)
            or (reserved.st_dev, reserved.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise LiveJournalIntegrityError("live journal path changed")

    @staticmethod
    def _unsafe_file(value: os.stat_result) -> bool:
        reparse = bool(
            getattr(value, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        )
        return (
            stat.S_ISLNK(value.st_mode)
            or not stat.S_ISREG(value.st_mode)
            or value.st_nlink != 1
            or reparse
        )

    def _verify_trusted_parent(self) -> None:
        try:
            parent = self._path.parent
            parent_stat = os.lstat(parent)
            if (
                stat.S_ISLNK(parent_stat.st_mode)
                or not stat.S_ISDIR(parent_stat.st_mode)
                or bool(
                    getattr(parent_stat, "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
                )
                or parent.resolve(strict=True) != parent.absolute()
            ):
                raise LiveJournalIntegrityError("live journal requires a trusted local directory")
        except OSError:
            raise LiveJournalIntegrityError(
                "live journal requires a trusted local directory"
            ) from None

    def _verify_sidecars(self) -> None:
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(f"{self._path}{suffix}")
            try:
                value = os.lstat(sidecar)
            except FileNotFoundError:
                continue
            except OSError:
                raise LiveJournalIntegrityError("live journal sidecar is unsafe") from None
            if self._unsafe_file(value):
                raise LiveJournalIntegrityError("live journal sidecar is unsafe")

    def _prevalidate_resume(self) -> None:
        # Read-only validation must still observe committed WAL frames after an
        # abrupt exit.  ``immutable=1`` incorrectly ignores those frames and
        # can make a valid durable journal look like an empty main database.
        uri = f"{self._path.resolve(strict=True).as_uri()}?mode=ro"
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(uri, uri=True, isolation_level=None)
            connection.row_factory = sqlite3.Row
            self._connection = connection
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {1, DATABASE_SCHEMA_VERSION}:
                raise LiveJournalIntegrityError("live journal schema mismatch")
            self._validate_schema(version)
            self._identity = self._load_identity()
            self._verify_durable_payloads()
        except (sqlite3.Error, OSError):
            raise LiveJournalIntegrityError("live journal failed read-only validation") from None
        finally:
            self._connection = None
            if connection is not None:
                connection.close()

    def _verify_database_path(self) -> None:
        row = (
            self._require_connection()
            .execute("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetchone()
        )
        if row is None or Path(row[0]).resolve(strict=True) != self._path.resolve(strict=True):
            raise LiveJournalIntegrityError("live journal database path changed")

    def _configure(self, busy_timeout_ms: int, max_bytes: int) -> None:
        connection = self._require_connection()
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
        if mode is None or str(mode[0]).lower() != "wal":
            raise LiveJournalIntegrityError("live journal durability configuration failed")
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        pages = max_bytes // page_size
        if pages < 1:
            raise LiveJournalCapacityError("live journal capacity exceeded")
        applied = int(connection.execute(f"PRAGMA max_page_count = {pages}").fetchone()[0])
        checks = (
            int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) == 1,
            int(connection.execute("PRAGMA synchronous").fetchone()[0]) == 2,
            int(connection.execute("PRAGMA busy_timeout").fetchone()[0]) == busy_timeout_ms,
            applied <= pages,
        )
        if not all(checks):
            raise LiveJournalIntegrityError("live journal durability configuration failed")

    def _initialize(self, journal_id: str) -> None:
        script, fingerprint = _schema_material()
        _, v1_fingerprint = _v1_schema_material()
        connection = self._require_connection()
        created_at = self._now()
        identity = LiveJournalIdentity(journal_id, DATABASE_SCHEMA_VERSION, fingerprint, created_at)
        with self._transaction():
            _execute_script_in_transaction(connection, script)
            connection.execute(
                "INSERT INTO live_journal_migrations(version, schema_fingerprint) VALUES (?, ?)",
                (1, v1_fingerprint),
            )
            connection.execute(
                "INSERT INTO live_journal_migrations(version, schema_fingerprint) VALUES (?, ?)",
                (DATABASE_SCHEMA_VERSION, fingerprint),
            )
            connection.execute(
                """INSERT INTO live_journal_identity(
                       singleton, journal_id, schema_version, schema_fingerprint, created_at
                   ) VALUES (1, ?, ?, ?, ?)""",
                (journal_id, DATABASE_SCHEMA_VERSION, fingerprint, _timestamp(created_at)),
            )
            identity_payload, identity_digest = _encode(identity, _IDENTITY_DOMAIN)
            del identity_payload
            self._append_record("identity", journal_id, identity_digest, created_at)
            migration_digest = _scalar_digest(
                _SCHEMA_MIGRATION_FACT_DOMAIN,
                {
                    "version": DATABASE_SCHEMA_VERSION,
                    "from_fingerprint": v1_fingerprint,
                    "to_fingerprint": fingerprint,
                    "recorded_at": _timestamp(created_at),
                },
            )
            self._append_record(
                "schema-migration", _SCHEMA_MIGRATION_RECORD_ID, migration_digest, created_at
            )
        self._validate_schema()

    def _resume_or_migrate(self) -> None:
        connection = self._require_connection()
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version == DATABASE_SCHEMA_VERSION:
            self._validate_schema()
            return
        if version != 1:
            raise LiveJournalIntegrityError("live journal schema mismatch")
        with self._transaction():
            self._validate_schema(1)
            self._identity = self._load_identity()
            self._verify_durable_payloads()
            _execute_script_in_transaction(connection, _migration_material())
            migrated_at = self._now()
            _, v1_fingerprint = _v1_schema_material()
            _, fingerprint = _schema_material()
            migration_digest = _scalar_digest(
                _SCHEMA_MIGRATION_FACT_DOMAIN,
                {
                    "version": DATABASE_SCHEMA_VERSION,
                    "from_fingerprint": v1_fingerprint,
                    "to_fingerprint": fingerprint,
                    "recorded_at": _timestamp(migrated_at),
                },
            )
            self._append_record(
                "schema-migration",
                _SCHEMA_MIGRATION_RECORD_ID,
                migration_digest,
                migrated_at,
            )
            self._validate_schema()
            self._verify_durable_payloads()

    def _now(self) -> datetime:
        value = self._clock()
        if (
            type(value) is not datetime
            or value.tzinfo is None
            or value.utcoffset() != timezone.utc.utcoffset(value)
        ):
            raise ValueError("clock must return a timezone-aware UTC datetime")
        return value

    def _new_claim_token(self) -> str:
        value = self._claim_token_factory()
        if type(value) is not str or not _IDENTIFIER.fullmatch(value):
            raise ValueError("claim_token_factory must return a bounded ASCII identifier")
        existing = (
            self._require_connection()
            .execute(
                "SELECT 1 FROM live_dispatch_claims WHERE claim_token = ?",
                (value,),
            )
            .fetchone()
        )
        if existing is not None:
            raise LiveJournalConflictError("claim token is not unique")
        return value

    def _validate_schema(self, version: int = DATABASE_SCHEMA_VERSION) -> None:
        connection = self._require_connection()
        _, fingerprint = _schema_material()
        _, v1_fingerprint = _v1_schema_material()
        if int(connection.execute("PRAGMA application_id").fetchone()[0]) != APPLICATION_ID:
            raise LiveJournalIntegrityError("live journal schema mismatch")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != version:
            raise LiveJournalIntegrityError("live journal schema mismatch")
        quick = connection.execute("PRAGMA quick_check").fetchone()
        if quick is None or quick[0] != "ok":
            raise LiveJournalIntegrityError("live journal corruption detected")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise LiveJournalIntegrityError("live journal corruption detected")
        rows = connection.execute(
            "SELECT version, schema_fingerprint FROM live_journal_migrations ORDER BY version"
        ).fetchall()
        expected_history = (
            [(1, v1_fingerprint)]
            if version == 1
            else [(1, v1_fingerprint), (DATABASE_SCHEMA_VERSION, fingerprint)]
        )
        if [tuple(row) for row in rows] != expected_history:
            raise LiveJournalIntegrityError("live journal schema mismatch")
        signature = tuple(
            cast(tuple[str, str, str], tuple(row))
            for row in connection.execute(
                """SELECT type, name, sql FROM sqlite_master
                   WHERE name NOT LIKE 'sqlite_%'
                     AND type IN ('table', 'index', 'view', 'trigger')
                   ORDER BY type, name"""
            )
        )
        if signature != _expected_schema_signature(version):
            raise LiveJournalIntegrityError("live journal schema mismatch")

    def _load_identity(self) -> LiveJournalIdentity:
        row = (
            self._require_connection()
            .execute(
                """SELECT journal_id, schema_version, schema_fingerprint, created_at
               FROM live_journal_identity WHERE singleton = 1"""
            )
            .fetchone()
        )
        if row is None:
            raise LiveJournalIntegrityError("live journal identity is missing")
        try:
            identity = LiveJournalIdentity(row[0], row[1], row[2], _parse_timestamp(row[3]))
        except (TypeError, ValueError):
            raise LiveJournalIntegrityError("live journal identity is invalid") from None
        _, fingerprint = _schema_material()
        _, v1_fingerprint = _v1_schema_material()
        valid_creation_identity = (identity.schema_version, identity.schema_fingerprint) in {
            (1, v1_fingerprint),
            (DATABASE_SCHEMA_VERSION, fingerprint),
        }
        if not valid_creation_identity:
            raise LiveJournalIntegrityError("live journal identity is invalid")
        return identity

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise LiveJournalClosedError("live journal is closed")
        return self._connection

    def _ensure_open(self) -> None:
        if self._closed or self._poisoned or self._connection is None:
            raise LiveJournalClosedError("live journal is closed")

    @contextmanager
    def _operation(self) -> Iterator[None]:
        self._ensure_open()
        try:
            yield
        except (LiveJournalIntegrityError, LiveJournalCapacityError):
            self._poison()
            raise

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                self._poison()
            raise
        else:
            try:
                connection.execute("COMMIT")
            except sqlite3.Error as exc:
                self._poison()
                raise _sqlite_failure(exc) from None

    def _poison(self) -> None:
        self._poisoned = True
        self._close_after_failure()

    def _close_after_failure(self) -> None:
        connection = self._connection
        self._connection = None
        self._closed = True
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            connection = self._connection
            self._connection = None
            self._closed = True
            if connection is not None:
                try:
                    connection.close()
                except sqlite3.Error:
                    raise LiveJournalIntegrityError("live journal close failed") from None

    def __enter__(self) -> SqliteLiveOrderJournal:
        with self._lock, self._operation():
            pass
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _load_order_row(self, client_order_id: str) -> LiveOrder | None:
        row = (
            self._require_connection()
            .execute(
                """SELECT client_order_id, account_id, state, version, payload,
                          payload_digest, updated_at
                   FROM live_orders WHERE client_order_id = ?""",
                (client_order_id,),
            )
            .fetchone()
        )
        if row is None:
            return None
        order = cast(LiveOrder, _decode(row[4], row[5], LiveOrder, _ORDER_DOMAIN))
        if (
            row[0] != order.intent.client_order_id
            or row[1] != order.intent.account_id
            or row[2] != order.state.value
            or row[3] != order.version
            or _parse_timestamp(row[6]) != order.updated_at
        ):
            raise LiveJournalIntegrityError("live journal order projection is invalid")
        return order

    def _load_history_order(self, client_order_id: str, version: int) -> LiveOrder | None:
        row = (
            self._require_connection()
            .execute(
                """SELECT payload, payload_digest FROM live_order_history
               WHERE client_order_id = ? AND order_version = ?""",
                (client_order_id, version),
            )
            .fetchone()
        )
        if row is None:
            return None
        order = cast(LiveOrder, _decode(row[0], row[1], LiveOrder, _ORDER_DOMAIN))
        if order.intent.client_order_id != client_order_id or order.version != version:
            raise LiveJournalIntegrityError("live journal order history is invalid")
        return order

    def _load_command(self, client_command_id: str) -> LiveCommand:
        row = (
            self._require_connection()
            .execute(
                """SELECT client_command_id, client_order_id, command_kind,
                      payload_fingerprint, payload, payload_digest
               FROM live_commands WHERE client_command_id = ?""",
                (client_command_id,),
            )
            .fetchone()
        )
        if row is None:
            raise LiveJournalIntegrityError("live journal command is missing")
        command = cast(
            LiveCommand,
            _decode(
                row[4],
                row[5],
                tuple(_COMMAND_DOMAINS),
                _COMMAND_DOMAIN,
            ),
        )
        if (
            row[0] != command.client_command_id
            or row[1] != _command_order_id(command)
            or row[2] != command.kind.value
            or row[3] != _command_fingerprint(command)
        ):
            raise LiveJournalIntegrityError("live journal command is invalid")
        return command

    def _write_order(self, order: LiveOrder, expected_version: int | None) -> bool:
        payload, digest = _encode(order, _ORDER_DOMAIN)
        timestamp = _timestamp(order.updated_at)
        connection = self._require_connection()
        if expected_version is None:
            connection.execute(
                """INSERT INTO live_orders(
                       client_order_id, account_id, state, version,
                       payload, payload_digest, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    order.intent.client_order_id,
                    order.intent.account_id,
                    order.state.value,
                    order.version,
                    payload,
                    digest,
                    timestamp,
                ),
            )
        else:
            cursor = connection.execute(
                """UPDATE live_orders SET account_id = ?, state = ?, version = ?,
                       payload = ?, payload_digest = ?, updated_at = ?
                   WHERE client_order_id = ? AND version = ?""",
                (
                    order.intent.account_id,
                    order.state.value,
                    order.version,
                    payload,
                    digest,
                    timestamp,
                    order.intent.client_order_id,
                    expected_version,
                ),
            )
            if cursor.rowcount != 1:
                return False
        connection.execute(
            """INSERT INTO live_order_history(
                   client_order_id, order_version, payload, payload_digest, recorded_at
               ) VALUES (?, ?, ?, ?, ?)""",
            (
                order.intent.client_order_id,
                order.version,
                payload,
                digest,
                timestamp,
            ),
        )
        return True

    def _append_record(
        self,
        kind: str,
        record_id: str,
        payload_digest: str,
        recorded_at: datetime,
    ) -> int:
        cursor = self._require_connection().execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES (?, ?, ?, ?)""",
            (kind, record_id, payload_digest, _timestamp(recorded_at)),
        )
        if cursor.lastrowid is None:
            raise LiveJournalIntegrityError("live journal sequence allocation failed")
        return cursor.lastrowid

    def register_new_order(
        self,
        command: LiveCommand,
        order: LiveOrder,
        *,
        intent_fingerprint: str,
    ) -> CommandRegistrationResult:
        if type(command) is not NewOrderCommand:
            raise TypeError("register_new_order requires NewOrderCommand")
        if type(order) is not LiveOrder or order.intent != command.intent:
            raise ValueError("order must match the NEW command intent")
        from .live_journal_contracts import intent_fingerprint as calculate_intent_fingerprint

        if intent_fingerprint != calculate_intent_fingerprint(order.intent):
            raise ValueError("intent_fingerprint does not match order intent")
        command_fingerprint = _command_fingerprint(command)
        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    reservation = connection.execute(
                        """SELECT intent_fingerprint FROM live_order_id_reservations
                           WHERE client_order_id = ?""",
                        (order.intent.client_order_id,),
                    ).fetchone()
                    existing_command = connection.execute(
                        "SELECT payload_fingerprint FROM live_commands WHERE client_command_id = ?",
                        (command.client_command_id,),
                    ).fetchone()
                    if reservation is not None:
                        existing_order = self._load_history_order(
                            order.intent.client_order_id, order.version
                        )
                        disposition = (
                            RegistrationDisposition.EXACT_RETRY
                            if reservation[0] == intent_fingerprint
                            and existing_command is not None
                            and existing_command[0] == command_fingerprint
                            and existing_order == order
                            else RegistrationDisposition.ID_CONFLICT
                        )
                        return CommandRegistrationResult(
                            command.client_command_id, disposition, None
                        )
                    if existing_command is not None:
                        return CommandRegistrationResult(
                            command.client_command_id,
                            RegistrationDisposition.ID_CONFLICT,
                            None,
                        )
                    connection.execute(
                        """INSERT INTO live_order_id_reservations(
                               client_order_id, intent_fingerprint, reserved_at
                           ) VALUES (?, ?, ?)""",
                        (
                            order.intent.client_order_id,
                            intent_fingerprint,
                            _timestamp(command.requested_at),
                        ),
                    )
                    self._write_order(order, None)
                    order_payload, order_digest = _encode(order, _ORDER_DOMAIN)
                    del order_payload
                    self._append_record(
                        "order",
                        order.intent.client_order_id,
                        order_digest,
                        order.updated_at,
                    )
                    self._insert_command(command, command_fingerprint)
                    command_payload, command_digest = _encode(command, _COMMAND_DOMAIN)
                    del command_payload
                    self._append_record(
                        "command",
                        command.client_command_id,
                        command_digest,
                        command.requested_at,
                    )
                return CommandRegistrationResult(
                    command.client_command_id,
                    RegistrationDisposition.REGISTERED,
                    order,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def _insert_command(self, command: LiveCommand, fingerprint: str) -> None:
        payload, digest = _encode(command, _COMMAND_DOMAIN)
        self._require_connection().execute(
            """INSERT INTO live_commands(
                   client_command_id, client_order_id, command_kind,
                   payload_fingerprint, payload, payload_digest, registered_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                command.client_command_id,
                _command_order_id(command),
                command.kind.value,
                fingerprint,
                payload,
                digest,
                _timestamp(command.requested_at),
            ),
        )

    def register_command(
        self,
        command: LiveCommand,
        order: LiveOrder,
        *,
        expected_order_version: int,
    ) -> CommandRegistrationResult:
        if type(command) not in _COMMAND_DOMAINS:
            raise TypeError("command must be an exact live command")
        if type(order) is not LiveOrder:
            raise TypeError("order must be LiveOrder")
        if _command_order_id(command) != order.intent.client_order_id:
            raise ValueError("command and order must have the same client_order_id")
        fingerprint = _command_fingerprint(command)
        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    existing = connection.execute(
                        """SELECT payload_fingerprint FROM live_commands
                           WHERE client_command_id = ?""",
                        (command.client_command_id,),
                    ).fetchone()
                    if existing is not None:
                        current = self._load_history_order(
                            order.intent.client_order_id, order.version
                        )
                        disposition = (
                            RegistrationDisposition.EXACT_RETRY
                            if existing[0] == fingerprint and current == order
                            else RegistrationDisposition.ID_CONFLICT
                        )
                        return CommandRegistrationResult(
                            command.client_command_id, disposition, None
                        )
                    current = self._load_order_row(order.intent.client_order_id)
                    if current is None or current.version != expected_order_version:
                        return CommandRegistrationResult(
                            command.client_command_id,
                            RegistrationDisposition.VERSION_MISMATCH,
                            None,
                        )
                    reservation = connection.execute(
                        """SELECT intent_fingerprint FROM live_order_id_reservations
                           WHERE client_order_id = ?""",
                        (order.intent.client_order_id,),
                    ).fetchone()
                    from .live_journal_contracts import (
                        intent_fingerprint as calculate_intent_fingerprint,
                    )

                    if (
                        order.intent != current.intent
                        or reservation is None
                        or reservation[0] != calculate_intent_fingerprint(current.intent)
                    ):
                        return CommandRegistrationResult(
                            command.client_command_id,
                            RegistrationDisposition.ID_CONFLICT,
                            None,
                        )
                    if order.version != expected_order_version + 1:
                        raise ValueError(
                            "registered command order must advance version exactly once"
                        )
                    if (
                        order.pending_command is None
                        or order.pending_command.command != command
                        or order.pending_command.payload_fingerprint != fingerprint
                    ):
                        raise ValueError("order must bind the exact registered command")
                    if not self._write_order(order, expected_order_version):
                        raise _CasMismatch
                    self._insert_command(command, fingerprint)
                    command_payload, command_digest = _encode(command, _COMMAND_DOMAIN)
                    del command_payload
                    self._append_record(
                        "command",
                        command.client_command_id,
                        command_digest,
                        command.requested_at,
                    )
                return CommandRegistrationResult(
                    command.client_command_id,
                    RegistrationDisposition.REGISTERED,
                    order,
                )
            except _CasMismatch:
                return CommandRegistrationResult(
                    command.client_command_id,
                    RegistrationDisposition.VERSION_MISMATCH,
                    None,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def claim_dispatch(
        self,
        client_command_id: str,
        payload_fingerprint: str,
        *,
        expected_order_version: int,
        claimant_id: str,
    ) -> DispatchClaim:
        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    row = connection.execute(
                        """SELECT c.payload_fingerprint, c.client_order_id, o.version
                           FROM live_commands c JOIN live_orders o
                             ON o.client_order_id = c.client_order_id
                           WHERE c.client_command_id = ?""",
                        (client_command_id,),
                    ).fetchone()
                    if row is None or row[0] != payload_fingerprint:
                        return DispatchClaim(
                            client_command_id,
                            payload_fingerprint,
                            DispatchClaimDisposition.PAYLOAD_CONFLICT,
                            max(expected_order_version, 1),
                        )
                    version = int(row[2])
                    if version != expected_order_version:
                        return DispatchClaim(
                            client_command_id,
                            payload_fingerprint,
                            DispatchClaimDisposition.VERSION_MISMATCH,
                            version,
                        )
                    existing = connection.execute(
                        """SELECT claim_version FROM live_dispatch_claims
                           WHERE client_command_id = ?""",
                        (client_command_id,),
                    ).fetchone()
                    if existing is not None:
                        return DispatchClaim(
                            client_command_id,
                            payload_fingerprint,
                            DispatchClaimDisposition.ALREADY_CLAIMED,
                            int(existing[0]),
                        )
                    token = self._new_claim_token()
                    claimed_at = self._now()
                    connection.execute(
                        """INSERT INTO live_dispatch_claims(
                               client_command_id, claim_token, claimant_id,
                               expected_order_version, claim_version, claimed_at
                           ) VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            client_command_id,
                            token,
                            claimant_id,
                            expected_order_version,
                            expected_order_version,
                            _timestamp(claimed_at),
                        ),
                    )
                    outstanding = OutstandingDispatchClaim(
                        command=self._load_command(client_command_id),
                        claim_token=token,
                        claimant_id=claimant_id,
                        expected_order_version=expected_order_version,
                        claimed_at=claimed_at,
                    )
                    claim_payload, claim_digest = _encode(outstanding, _CLAIM_DOMAIN)
                    del claim_payload
                    self._append_record(
                        "dispatch-claim", client_command_id, claim_digest, claimed_at
                    )
                return DispatchClaim(
                    client_command_id,
                    payload_fingerprint,
                    DispatchClaimDisposition.ACQUIRED,
                    expected_order_version,
                    token,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def record_dispatch_receipt(
        self,
        receipt: DispatchReceipt,
        *,
        claim_token: str,
        expected_order_version: int,
    ) -> DispatchReceiptRecordResult:
        if type(receipt) is not DispatchReceipt:
            raise TypeError("receipt must be DispatchReceipt")
        payload, digest = _encode(receipt, _RECEIPT_DOMAIN)
        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    claim = connection.execute(
                        """SELECT dc.claim_token, dc.expected_order_version,
                                  c.payload_fingerprint, c.client_order_id
                           FROM live_dispatch_claims dc JOIN live_commands c
                             ON c.client_command_id = dc.client_command_id
                           WHERE dc.client_command_id = ?""",
                        (receipt.client_command_id,),
                    ).fetchone()
                    if claim is None or claim[0] != claim_token:
                        return DispatchReceiptRecordResult(
                            receipt.client_command_id,
                            ReceiptRecordDisposition.TOKEN_MISMATCH,
                            None,
                        )
                    resolved_claim = connection.execute(
                        """SELECT 1 FROM live_dispatch_claim_resolutions
                           WHERE client_command_id = ?""",
                        (receipt.client_command_id,),
                    ).fetchone()
                    if resolved_claim is not None:
                        return DispatchReceiptRecordResult(
                            receipt.client_command_id,
                            ReceiptRecordDisposition.ID_CONFLICT,
                            None,
                        )
                    prior = connection.execute(
                        """SELECT payload_digest FROM live_dispatch_receipts
                           WHERE client_command_id = ?""",
                        (receipt.client_command_id,),
                    ).fetchone()
                    if prior is not None:
                        disposition = (
                            ReceiptRecordDisposition.EXACT_RETRY
                            if prior[0] == digest
                            else ReceiptRecordDisposition.ID_CONFLICT
                        )
                        return DispatchReceiptRecordResult(
                            receipt.client_command_id, disposition, None
                        )
                    if claim[2] != receipt.payload_fingerprint:
                        return DispatchReceiptRecordResult(
                            receipt.client_command_id,
                            ReceiptRecordDisposition.ID_CONFLICT,
                            None,
                        )
                    order = self._load_order_row(claim[3])
                    if (
                        order is None
                        or order.version != expected_order_version
                        or claim[1] != expected_order_version
                    ):
                        return DispatchReceiptRecordResult(
                            receipt.client_command_id,
                            ReceiptRecordDisposition.VERSION_MISMATCH,
                            None,
                        )
                    ledger = self._load_applied_event_ledger()
                    reduced = reduce_dispatch(order, receipt, ledger)
                    if reduced.disposition is ReductionDisposition.EVENT_CONFLICT:
                        return DispatchReceiptRecordResult(
                            receipt.client_command_id,
                            ReceiptRecordDisposition.ID_CONFLICT,
                            None,
                        )
                    connection.execute(
                        """INSERT INTO live_dispatch_receipts(
                               client_command_id, payload_fingerprint, payload,
                               payload_digest, recorded_at
                           ) VALUES (?, ?, ?, ?, ?)""",
                        (
                            receipt.client_command_id,
                            receipt.payload_fingerprint,
                            payload,
                            digest,
                            _timestamp(receipt.completed_at or receipt.attempted_at),
                        ),
                    )
                    self._append_record(
                        "dispatch-receipt",
                        receipt.client_command_id,
                        digest,
                        receipt.completed_at or receipt.attempted_at,
                    )
                    if reduced.order.version != order.version and not self._write_order(
                        reduced.order, order.version
                    ):
                        raise _CasMismatch
                return DispatchReceiptRecordResult(
                    receipt.client_command_id,
                    ReceiptRecordDisposition.RECORDED,
                    reduced.order,
                )
            except _CasMismatch:
                return DispatchReceiptRecordResult(
                    receipt.client_command_id,
                    ReceiptRecordDisposition.VERSION_MISMATCH,
                    None,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def append_raw_observation(self, observation: RawBrokerObservation) -> JournalAppendResult:
        if type(observation) is not RawBrokerObservation:
            raise TypeError("observation must be RawBrokerObservation")
        if len(observation.payload) > MAX_CODEC_PAYLOAD_BYTES:
            raise LiveJournalCapacityError("raw observation exceeds the durable payload limit")
        try:
            payload = encode_journal_value(observation)
            digest = journal_digest(_RAW_DOMAIN, payload)
        except LiveJournalCodecError:
            raise LiveJournalCapacityError(
                "raw observation exceeds the durable payload limit"
            ) from None
        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    existing = connection.execute(
                        """SELECT payload_digest FROM live_raw_observations
                           WHERE observation_id = ?""",
                        (observation.observation_id,),
                    ).fetchone()
                    if existing is not None:
                        disposition = (
                            JournalAppendDisposition.EXACT_DUPLICATE
                            if existing[0] == digest
                            else JournalAppendDisposition.ID_CONFLICT
                        )
                        return JournalAppendResult(observation.observation_id, disposition)
                    try:
                        connection.execute(
                            """INSERT INTO live_raw_observations(
                                   observation_id, source, broker_session_generation,
                                   adapter_received_sequence, received_at, payload,
                                   payload_digest, resolution_status
                               ) VALUES (?, ?, ?, ?, ?, ?, ?, 'unresolved')""",
                            (
                                observation.observation_id,
                                observation.source,
                                observation.broker_session_generation,
                                observation.adapter_received_sequence,
                                _timestamp(observation.received_at),
                                payload,
                                digest,
                            ),
                        )
                        self._append_record(
                            "raw-observation",
                            observation.observation_id,
                            digest,
                            observation.received_at,
                        )
                    except sqlite3.IntegrityError:
                        return JournalAppendResult(
                            observation.observation_id,
                            JournalAppendDisposition.ID_CONFLICT,
                        )
                return JournalAppendResult(
                    observation.observation_id, JournalAppendDisposition.APPENDED
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def apply_normalized_event(
        self,
        event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
        *,
        raw_observation_id: str,
        expected_order_version: int | None,
    ) -> EventApplicationResult:
        if type(event) not in {NormalizedBrokerOrderEvent, NormalizedBrokerFillEvent}:
            raise TypeError("event must be a normalized broker event")
        semantic = broker_semantic_fingerprint(event)
        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    raw = connection.execute(
                        """SELECT source, resolution_status, broker_session_generation,
                                  adapter_received_sequence, received_at, payload, payload_digest
                           FROM live_raw_observations
                           WHERE observation_id = ?""",
                        (raw_observation_id,),
                    ).fetchone()
                    if raw is None:
                        raise LiveJournalConflictError(
                            "normalized event requires a durable raw observation"
                        )
                    raw_value = cast(
                        RawBrokerObservation,
                        _decode(raw[5], raw[6], RawBrokerObservation, _RAW_DOMAIN),
                    )
                    source = "broker-event"
                    provenance_matches = (
                        raw_value.observation_id == raw_observation_id
                        and raw[2] == event.broker_session_generation
                        and raw[3] == event.adapter_received_sequence
                        and _parse_timestamp(raw[4]) == event.received_at
                        and raw_value.broker_session_generation == event.broker_session_generation
                        and raw_value.adapter_received_sequence == event.adapter_received_sequence
                        and raw_value.received_at == event.received_at
                    )
                    existing = connection.execute(
                        """SELECT n.semantic_fingerprint, n.raw_observation_id,
                                  a.disposition
                           FROM live_normalized_events n
                           JOIN live_event_applications a
                             ON a.source = n.source AND a.event_id = n.event_id
                           WHERE n.source = ? AND n.event_id = ?""",
                        (source, event.event_id),
                    ).fetchone()
                    if (
                        existing is not None
                        and existing[0] == semantic
                        and existing[1] == raw_observation_id
                        and existing[2] == EventApplicationDisposition.APPLIED.value
                        and raw[1] == "resolved"
                    ):
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.EXACT_DUPLICATE,
                            None,
                        )
                    client_order_id = self._existing_order_id(event.correlation.client_order_id)
                    if not provenance_matches:
                        if raw[1] == "unresolved":
                            if existing is None:
                                self._insert_normalized_application(
                                    event,
                                    raw_observation_id,
                                    EventApplicationDisposition.EVENT_CONFLICT,
                                    client_order_id,
                                    LiveFailureCode.CORRELATION_CONFLICT,
                                )
                            self._record_reconciliation(
                                raw_observation_id,
                                client_order_id,
                                "observation_provenance_conflict",
                            )
                            self._resolve_observation(
                                raw_observation_id, "conflict", event.received_at
                            )
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.EVENT_CONFLICT,
                            None,
                            LiveFailureCode.CORRELATION_CONFLICT,
                        )
                    if raw[1] != "unresolved":
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.EVENT_CONFLICT,
                            None,
                            LiveFailureCode.CORRELATION_CONFLICT,
                        )
                    if existing is not None:
                        exact_applied = (
                            existing[0] == semantic
                            and existing[2] == EventApplicationDisposition.APPLIED.value
                        )
                        status = "resolved" if exact_applied else "conflict"
                        if not exact_applied:
                            reason = (
                                "event_conflict"
                                if existing[0] != semantic
                                else "prior_application_not_applied"
                            )
                            self._record_reconciliation(
                                raw_observation_id,
                                client_order_id,
                                reason,
                            )
                        self._resolve_observation(raw_observation_id, status, event.received_at)
                        if status == "resolved":
                            return EventApplicationResult(
                                event.event_id,
                                EventApplicationDisposition.EXACT_DUPLICATE,
                                None,
                            )
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.EVENT_CONFLICT,
                            None,
                            LiveFailureCode.CORRELATION_CONFLICT,
                        )
                    client_order_id = event.correlation.client_order_id
                    if client_order_id is None:
                        self._insert_normalized_application(
                            event,
                            raw_observation_id,
                            EventApplicationDisposition.UNRESOLVED,
                            None,
                            None,
                        )
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.UNRESOLVED,
                            None,
                        )
                    order = self._load_order_row(client_order_id)
                    if order is None:
                        self._insert_normalized_application(
                            event,
                            raw_observation_id,
                            EventApplicationDisposition.UNRESOLVED,
                            None,
                            None,
                        )
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.UNRESOLVED,
                            None,
                        )
                    if (
                        expected_order_version is not None
                        and order.version != expected_order_version
                    ):
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.VERSION_MISMATCH,
                            None,
                        )
                    ledger = self._load_applied_event_ledger()
                    if type(event) is NormalizedBrokerOrderEvent:
                        reduced = reduce_broker_order_event(order, event, ledger)
                    else:
                        reduced = reduce_broker_fill_event(
                            order, cast(NormalizedBrokerFillEvent, event), ledger
                        )
                    if reduced.reconciliation_required:
                        self._insert_normalized_application(
                            event,
                            raw_observation_id,
                            EventApplicationDisposition.UNRESOLVED,
                            client_order_id,
                            reduced.failure_code,
                        )
                        self._record_reconciliation(
                            raw_observation_id, client_order_id, "reducer_reconciliation"
                        )
                        self._resolve_observation(raw_observation_id, "conflict", event.received_at)
                        return EventApplicationResult(
                            event.event_id,
                            EventApplicationDisposition.UNRESOLVED,
                            None,
                        )
                    if reduced.fill is not None:
                        prior_fill = connection.execute(
                            "SELECT 1 FROM live_fills WHERE fill_id = ?",
                            (reduced.fill.fill_id,),
                        ).fetchone()
                        if prior_fill is not None:
                            self._insert_normalized_application(
                                event,
                                raw_observation_id,
                                EventApplicationDisposition.EVENT_CONFLICT,
                                client_order_id,
                                LiveFailureCode.CORRELATION_CONFLICT,
                            )
                            self._record_reconciliation(
                                raw_observation_id, client_order_id, "fill_id_conflict"
                            )
                            self._resolve_observation(
                                raw_observation_id, "conflict", event.received_at
                            )
                            return EventApplicationResult(
                                event.event_id,
                                EventApplicationDisposition.EVENT_CONFLICT,
                                None,
                                LiveFailureCode.CORRELATION_CONFLICT,
                            )
                    self._insert_normalized_application(
                        event,
                        raw_observation_id,
                        EventApplicationDisposition.APPLIED,
                        client_order_id,
                        reduced.failure_code,
                    )
                    if reduced.order.version != order.version and not self._write_order(
                        reduced.order, order.version
                    ):
                        raise _CasMismatch
                    if reduced.fill is not None:
                        fill_payload, fill_digest = _encode(reduced.fill, _FILL_DOMAIN)
                        connection.execute(
                            """INSERT INTO live_fills(
                                   fill_id, client_order_id, source, event_id,
                                   payload, payload_digest
                               ) VALUES (?, ?, ?, ?, ?, ?)""",
                            (
                                reduced.fill.fill_id,
                                client_order_id,
                                source,
                                event.event_id,
                                fill_payload,
                                fill_digest,
                            ),
                        )
                    self._resolve_observation(raw_observation_id, "resolved", event.received_at)
                return EventApplicationResult(
                    event.event_id,
                    EventApplicationDisposition.APPLIED,
                    reduced.order,
                )
            except _CasMismatch:
                return EventApplicationResult(
                    event.event_id,
                    EventApplicationDisposition.VERSION_MISMATCH,
                    None,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def _insert_normalized_application(
        self,
        event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
        raw_observation_id: str,
        disposition: EventApplicationDisposition,
        client_order_id: str | None,
        failure_code: LiveFailureCode | None,
    ) -> None:
        payload, digest = _encode(event, _EVENT_DOMAIN)
        source = "broker-event"
        connection = self._require_connection()
        applied_at = _timestamp(event.received_at)
        failure_value = failure_code.value if failure_code is not None else None
        connection.execute(
            """INSERT INTO live_normalized_events(
                   source, event_id, raw_observation_id, semantic_fingerprint,
                   payload, payload_digest, received_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                source,
                event.event_id,
                raw_observation_id,
                broker_semantic_fingerprint(event),
                payload,
                digest,
                _timestamp(event.received_at),
            ),
        )
        connection.execute(
            """INSERT INTO live_event_applications(
                   source, event_id, client_order_id, disposition,
                   failure_code, applied_at
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                source,
                event.event_id,
                client_order_id,
                disposition.value,
                failure_value,
                applied_at,
            ),
        )
        application_digest = _application_digest(
            event_payload_digest=digest,
            raw_observation_id=raw_observation_id,
            client_order_id=client_order_id,
            disposition=disposition.value,
            failure_code=failure_value,
            applied_at=applied_at,
        )
        self._append_record(
            "normalized-application",
            event.event_id,
            application_digest,
            event.received_at,
        )

    def _resolve_observation(self, observation_id: str, status: str, resolved_at: datetime) -> None:
        if status not in {"resolved", "conflict", "ambiguous"}:
            raise ValueError("observation resolution status is invalid")
        connection = self._require_connection()
        cursor = connection.execute(
            """UPDATE live_raw_observations SET resolution_status = ?
               WHERE observation_id = ? AND resolution_status = 'unresolved'""",
            (status, observation_id),
        )
        if cursor.rowcount != 1:
            raise _CasMismatch
        timestamp = _timestamp(resolved_at)
        self._append_record(
            "observation-resolution",
            observation_id,
            _resolution_digest(observation_id, status, timestamp),
            resolved_at,
        )

    def _record_reconciliation(
        self, observation_id: str | None, client_order_id: str | None, reason: str
    ) -> DurableReconciliationRequirement:
        created_at = self._now()
        cursor = self._require_connection().execute(
            """INSERT INTO live_reconciliation_requirements(
                   client_order_id, observation_id, reason_code, created_at
               ) VALUES (?, ?, ?, ?)""",
            (client_order_id, observation_id, reason, _timestamp(created_at)),
        )
        requirement_id = cursor.lastrowid
        if type(requirement_id) is not int:
            raise LiveJournalIntegrityError("live journal reconciliation identifier is invalid")
        requirement = DurableReconciliationRequirement(
            requirement_id,
            reason,
            created_at,
            client_order_id,
            observation_id,
        )
        payload, digest = _encode(requirement, _RECONCILIATION_DOMAIN)
        del payload
        self._append_record(
            "reconciliation",
            str(requirement_id),
            digest,
            created_at,
        )
        return requirement

    def _existing_order_id(self, client_order_id: str | None) -> str | None:
        if client_order_id is None:
            return None
        row = (
            self._require_connection()
            .execute(
                "SELECT 1 FROM live_orders WHERE client_order_id = ?",
                (client_order_id,),
            )
            .fetchone()
        )
        return client_order_id if row is not None else None

    def get_order(self, client_order_id: str) -> LiveOrder | None:
        with self._lock, self._operation():
            try:
                return self._load_order_row(client_order_id)
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def list_active_orders(self, account_id: str | None = None) -> tuple[LiveOrder, ...]:
        placeholders = ",".join("?" for _ in _ACTIVE_STATES)
        sql = f"SELECT client_order_id FROM live_orders WHERE state IN ({placeholders})"
        values: list[object] = list(_ACTIVE_STATES)
        if account_id is not None:
            sql += " AND account_id = ?"
            values.append(account_id)
        sql += " ORDER BY client_order_id"
        with self._lock, self._operation():
            try:
                rows = self._require_connection().execute(sql, values).fetchall()
                orders = tuple(self._load_order_row(row[0]) for row in rows)
                if any(order is None for order in orders):
                    raise LiveJournalIntegrityError("live journal order projection is invalid")
                return cast(tuple[LiveOrder, ...], orders)
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def _load_recovery_blocker_attributions(
        self,
    ) -> tuple[tuple[str, str, frozenset[str]], ...]:
        connection = self._require_connection()
        attributions: list[tuple[str, str, frozenset[str]]] = []

        def add(kind: str, durable_id: str, accounts: set[str]) -> None:
            attributions.append((kind, durable_id, frozenset(accounts)))

        for row in connection.execute(
            """SELECT dc.client_command_id, o.account_id
               FROM live_dispatch_claims dc
               JOIN live_commands c ON c.client_command_id = dc.client_command_id
               JOIN live_orders o ON o.client_order_id = c.client_order_id
               LEFT JOIN live_dispatch_receipts dr
                 ON dr.client_command_id = dc.client_command_id
               LEFT JOIN live_dispatch_claim_resolutions dcr
                 ON dcr.client_command_id = dc.client_command_id
               WHERE dr.client_command_id IS NULL
                 AND dcr.client_command_id IS NULL"""
        ):
            add("claim", row[0], {row[1]})

        observation_accounts: dict[str, set[str]] = {}
        for row in connection.execute(
            """SELECT raw_observation_id, payload, payload_digest
               FROM live_normalized_events"""
        ):
            event = cast(
                NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
                _decode(
                    row[1],
                    row[2],
                    (NormalizedBrokerOrderEvent, NormalizedBrokerFillEvent),
                    _EVENT_DOMAIN,
                ),
            )
            observation_accounts.setdefault(row[0], set()).add(event.account_id)
        for row in connection.execute(
            """SELECT a.observation_id, o.account_id
               FROM live_observation_ambiguity a
               JOIN live_orders o
                 ON o.client_order_id = a.candidate_client_order_id"""
        ):
            observation_accounts.setdefault(row[0], set()).add(row[1])

        for row in connection.execute(
            """SELECT r.observation_id, r.resolution_status
               FROM live_raw_observations r
               LEFT JOIN live_observation_reconciliation_resolutions rr
                 ON rr.observation_id = r.observation_id
               WHERE r.resolution_status IN ('unresolved', 'conflict', 'ambiguous')
                 AND rr.observation_id IS NULL"""
        ):
            add(
                f"observation-{row[1]}",
                row[0],
                observation_accounts.get(row[0], set()),
            )

        order_accounts = {
            row[0]: row[1]
            for row in connection.execute("SELECT client_order_id, account_id FROM live_orders")
        }
        for row in connection.execute(
            """SELECT r.requirement_id, r.client_order_id, r.observation_id
               FROM live_reconciliation_requirements r
               LEFT JOIN live_reconciliation_requirement_resolutions rr
                 ON rr.requirement_id = r.requirement_id
               WHERE r.resolved_at IS NULL AND rr.requirement_id IS NULL"""
        ):
            accounts: set[str] = set()
            if row[1] is not None:
                order_account = order_accounts.get(row[1])
                if order_account is None:
                    raise LiveJournalIntegrityError(
                        "live journal reconciliation requirement is invalid"
                    )
                accounts.add(order_account)
            if row[2] is not None:
                accounts.update(observation_accounts.get(row[2], set()))
            add("requirement", str(row[0]), accounts)

        return tuple(attributions)

    def _load_recovery_blockers(self, account_id: str) -> tuple[str, ...]:
        blockers = {
            _recovery_blocker(kind, durable_id)
            for kind, durable_id, accounts in self._load_recovery_blocker_attributions()
            if len(accounts) != 1 or account_id in accounts
        }
        return tuple(sorted(blockers))

    def _load_inspection_recovery_issue_codes(self, account_id: str) -> tuple[str, ...]:
        issue_by_kind = {
            "observation-unresolved": "unresolved_observation",
            "observation-conflict": "conflict_observation",
            "observation-ambiguous": "ambiguous_observation",
            "requirement": "durable_reconciliation_requirement",
        }
        issues: set[str] = set()
        for kind, _, accounts in self._load_recovery_blocker_attributions():
            if kind == "claim":
                continue
            if len(accounts) != 1:
                issues.add("global_recovery_blocker")
            elif account_id in accounts:
                issues.add(issue_by_kind[kind])
        return tuple(sorted(issues))

    def load_account_snapshot(self, account_id: str) -> LocalReconciliationSnapshot:
        if type(account_id) is not str:
            raise TypeError("account_id must be a string")
        if not _IDENTIFIER.fullmatch(account_id):
            raise ValueError("account_id must be a bounded ASCII identifier")
        with self._lock, self._operation(), localcontext(Context(prec=34)):
            try:
                connection = self._require_connection()
                connection.execute("BEGIN")
                try:
                    self._verify_durable_payloads()
                    order_rows = connection.execute(
                        """SELECT client_order_id FROM live_orders
                           WHERE account_id = ? ORDER BY client_order_id""",
                        (account_id,),
                    ).fetchall()
                    orders = tuple(self._load_order_row(row[0]) for row in order_rows)
                    if any(order is None for order in orders):
                        raise LiveJournalIntegrityError("live journal order projection is invalid")
                    typed_orders = cast(tuple[LiveOrder, ...], orders)

                    fills: list[LiveFill] = []
                    for row in connection.execute(
                        """SELECT f.fill_id, f.client_order_id, f.payload,
                                  f.payload_digest, o.account_id
                           FROM live_fills f JOIN live_orders o
                             ON o.client_order_id = f.client_order_id
                           WHERE o.account_id = ?
                           ORDER BY f.fill_id""",
                        (account_id,),
                    ):
                        fill = cast(
                            LiveFill,
                            _decode(row[2], row[3], LiveFill, _FILL_DOMAIN),
                        )
                        if (
                            row[0] != fill.fill_id
                            or row[1] != fill.client_order_id
                            or row[4] != fill.account_id
                            or fill.account_id != account_id
                        ):
                            raise LiveJournalIntegrityError("live journal fill is invalid")
                        fills.append(fill)
                    fills.sort(key=lambda fill: (fill.occurred_at, fill.fill_id))

                    as_of = self._now()
                    latest_item = max(
                        (
                            *(order.updated_at for order in typed_orders),
                            *(fill.occurred_at for fill in fills),
                        ),
                        default=None,
                    )
                    if latest_item is not None and as_of < latest_item:
                        raise ValueError("clock predates durable account data")

                    quantities: dict[tuple[str, str], list[Decimal]] = {}
                    for fill in fills:
                        key = (fill.strategy_id, fill.instrument_id)
                        signed = (
                            fill.quantity
                            if fill.side is LiveSide.BUY
                            else fill.quantity.copy_negate()
                        )
                        quantities.setdefault(key, []).append(signed)
                    exact_quantities = {
                        key: exact_decimal_sum(values) for key, values in quantities.items()
                    }
                    blockers = self._load_recovery_blockers(account_id)
                    sequence = int(
                        connection.execute(
                            "SELECT coalesce(max(journal_sequence), 0) FROM live_journal_records"
                        ).fetchone()[0]
                    )
                    attributions = tuple(
                        StrategyPositionAttribution(
                            strategy_id,
                            account_id,
                            instrument_id,
                            quantity,
                            as_of,
                        )
                        for (strategy_id, instrument_id), quantity in sorted(
                            exact_quantities.items()
                        )
                        if quantity != 0
                    )
                    snapshot = LocalReconciliationSnapshot(
                        account_id,
                        typed_orders,
                        tuple(fills),
                        attributions,
                        as_of,
                        blockers,
                        sequence,
                    )
                except BaseException:
                    connection.execute("ROLLBACK")
                    raise
                connection.execute("COMMIT")
                return snapshot
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def commit_reconciliation(
        self, request: DurableReconciliationCommitRequest
    ) -> DurableReconciliationCommitResult:
        """Atomically apply conservative, evidence-backed blocker overlays."""

        if type(request) is not DurableReconciliationCommitRequest:
            raise TypeError("request must be DurableReconciliationCommitRequest")
        request_payload, request_digest = _encode(request, _COMMIT_REQUEST_DOMAIN)

        def rejected(
            disposition: ReconciliationCommitDisposition,
        ) -> DurableReconciliationCommitResult:
            return DurableReconciliationCommitResult(
                request.commit_id, request.account_id, disposition
            )

        with self._lock, self._operation():
            try:
                with self._transaction():
                    connection = self._require_connection()
                    self._verify_durable_payloads()
                    existing = connection.execute(
                        """SELECT account_id, request_payload, request_digest, committed_at,
                                  resulting_journal_sequence
                           FROM live_reconciliation_commits WHERE commit_id = ?""",
                        (request.commit_id,),
                    ).fetchone()
                    if existing is not None:
                        if (
                            existing[0] == request.account_id
                            and existing[1] == request_payload
                            and existing[2] == request_digest
                        ):
                            resolved_claims = tuple(
                                row[0]
                                for row in connection.execute(
                                    """SELECT client_command_id FROM live_dispatch_claim_resolutions
                                       WHERE commit_id = ? ORDER BY client_command_id""",
                                    (request.commit_id,),
                                )
                            )
                            resolved_observations = tuple(
                                row[0]
                                for row in connection.execute(
                                    """SELECT observation_id
                                       FROM live_observation_reconciliation_resolutions
                                       WHERE commit_id = ? ORDER BY observation_id""",
                                    (request.commit_id,),
                                )
                            )
                            resolved_requirements = tuple(
                                int(row[0])
                                for row in connection.execute(
                                    """SELECT requirement_id
                                       FROM live_reconciliation_requirement_resolutions
                                       WHERE commit_id = ? ORDER BY requirement_id""",
                                    (request.commit_id,),
                                )
                            )
                            return DurableReconciliationCommitResult(
                                request.commit_id,
                                request.account_id,
                                ReconciliationCommitDisposition.EXACT_RETRY,
                                _parse_timestamp(existing[3]),
                                int(existing[4]),
                                resolved_claims,
                                resolved_observations,
                                resolved_requirements,
                                request.order_projections,
                            )
                        return rejected(ReconciliationCommitDisposition.ID_CONFLICT)
                    reused = connection.execute(
                        """SELECT 1 FROM live_reconciliation_commits
                           WHERE account_id = ? AND (snapshot_id = ? OR request_digest = ?)""",
                        (
                            request.account_id,
                            request.assessment.broker_snapshot.snapshot_id,
                            request_digest,
                        ),
                    ).fetchone()
                    if reused is not None:
                        return rejected(ReconciliationCommitDisposition.ID_CONFLICT)

                    assessment = request.assessment
                    recomputed_assessment = assess_reconciliation(
                        assessment.local_snapshot,
                        assessment.broker_snapshot,
                        assessment.result.reconciled_at,
                    )
                    if recomputed_assessment != assessment:
                        return rejected(ReconciliationCommitDisposition.NOT_AUTHORITATIVE)
                    if (
                        assessment.local_snapshot.account_id != request.account_id
                        or assessment.broker_snapshot.account_id != request.account_id
                        or assessment.result.account_id != request.account_id
                        or assessment.local_snapshot.journal_sequence
                        != request.expected_journal_sequence
                    ):
                        return rejected(ReconciliationCommitDisposition.NOT_AUTHORITATIVE)
                    projection_plan = project_authoritative_orders(assessment)
                    if projection_plan.disposition is OrderProjectionDisposition.READY:
                        if (
                            request.expected_order_versions
                            != projection_plan.expected_order_versions
                            or request.order_projections != projection_plan.projected_orders
                        ):
                            return rejected(ReconciliationCommitDisposition.NOT_AUTHORITATIVE)
                    elif request.order_projections:
                        return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                    if (
                        not request.order_projections
                        and projection_plan.disposition is OrderProjectionDisposition.READY
                    ):
                        return rejected(ReconciliationCommitDisposition.NOT_AUTHORITATIVE)
                    if not request.order_projections and (
                        not assessment.result.is_authoritative or assessment.result.discrepancies
                    ):
                        return rejected(ReconciliationCommitDisposition.NOT_AUTHORITATIVE)

                    durable_orders = tuple(
                        self._load_order_row(row[0])
                        for row in connection.execute(
                            """SELECT client_order_id FROM live_orders
                               WHERE account_id = ? ORDER BY client_order_id""",
                            (request.account_id,),
                        )
                    )
                    durable_fills = tuple(
                        cast(LiveFill, _decode(row[0], row[1], LiveFill, _FILL_DOMAIN))
                        for row in connection.execute(
                            """SELECT f.payload, f.payload_digest
                               FROM live_fills f JOIN live_orders o
                                 ON o.client_order_id = f.client_order_id
                               WHERE o.account_id = ?
                               ORDER BY f.payload, f.fill_id""",
                            (request.account_id,),
                        )
                    )
                    local_orders = tuple(
                        sorted(
                            assessment.local_snapshot.orders,
                            key=lambda item: item.intent.client_order_id,
                        )
                    )
                    local_fills = tuple(
                        sorted(
                            assessment.local_snapshot.fills,
                            key=lambda item: (item.occurred_at, item.fill_id),
                        )
                    )
                    if (
                        durable_orders != local_orders
                        or tuple(
                            sorted(
                                durable_fills,
                                key=lambda item: (item.occurred_at, item.fill_id),
                            )
                        )
                        != local_fills
                        or self._load_recovery_blockers(request.account_id)
                        != assessment.local_snapshot.recovery_blockers
                    ):
                        return rejected(ReconciliationCommitDisposition.STALE_SNAPSHOT)

                    sequence = int(
                        connection.execute(
                            "SELECT coalesce(max(journal_sequence), 0) FROM live_journal_records"
                        ).fetchone()[0]
                    )
                    if sequence != request.expected_journal_sequence:
                        return rejected(ReconciliationCommitDisposition.STALE_SNAPSHOT)

                    if not (
                        request.claim_resolutions
                        or request.observation_resolutions
                        or request.requirement_resolutions
                        or request.order_projections
                    ):
                        return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                    expected_versions = {
                        item.client_order_id: item.version
                        for item in request.expected_order_versions
                    }
                    for order_id, expected_version in expected_versions.items():
                        row = connection.execute(
                            "SELECT account_id, version FROM live_orders WHERE client_order_id = ?",
                            (order_id,),
                        ).fetchone()
                        if (
                            row is None
                            or row[0] != request.account_id
                            or int(row[1]) != expected_version
                        ):
                            return rejected(ReconciliationCommitDisposition.VERSION_MISMATCH)

                    claim_rows: dict[str, sqlite3.Row] = {}
                    for row in connection.execute(
                        """SELECT dc.client_command_id, dc.claim_token, dc.claim_version,
                                  dc.expected_order_version, c.command_kind, c.client_order_id,
                                  o.account_id, o.version
                           FROM live_dispatch_claims dc
                           JOIN live_commands c ON c.client_command_id = dc.client_command_id
                           JOIN live_orders o ON o.client_order_id = c.client_order_id
                           LEFT JOIN live_dispatch_receipts dr
                             ON dr.client_command_id = dc.client_command_id
                           LEFT JOIN live_dispatch_claim_resolutions rr
                             ON rr.client_command_id = dc.client_command_id
                           WHERE dr.client_command_id IS NULL AND rr.client_command_id IS NULL"""
                    ):
                        if row[6] == request.account_id:
                            claim_rows[row[0]] = row
                    if set(claim_rows) != {
                        item.client_command_id for item in request.claim_resolutions
                    }:
                        return rejected(ReconciliationCommitDisposition.VERSION_MISMATCH)
                    claim_directives = {
                        item.client_command_id: item for item in request.claim_resolutions
                    }
                    for projected_order in request.order_projections:
                        projection_current_order = self._load_order_row(
                            projected_order.intent.client_order_id
                        )
                        projected_command_id = (
                            projection_current_order.pending_command.client_command_id
                            if projection_current_order is not None
                            and projection_current_order.pending_command is not None
                            else None
                        )
                        if (
                            projected_command_id is None
                            or projected_command_id not in claim_rows
                            or projected_command_id not in claim_directives
                        ):
                            return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                    for command_id, row in claim_rows.items():
                        directive = claim_directives[command_id]
                        if (
                            row[1] != directive.claim_token
                            or int(row[2]) != int(row[3])
                            or expected_versions.get(row[5]) != int(row[7])
                        ):
                            return rejected(ReconciliationCommitDisposition.VERSION_MISMATCH)

                    observation_accounts: dict[str, set[str]] = {}
                    normalized_by_observation: dict[
                        str,
                        list[
                            tuple[
                                NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
                                str,
                            ]
                        ],
                    ] = {}
                    for row in connection.execute(
                        """SELECT raw_observation_id, event_id, payload, payload_digest
                           FROM live_normalized_events"""
                    ):
                        event = cast(
                            NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
                            _decode(
                                row[2],
                                row[3],
                                (NormalizedBrokerOrderEvent, NormalizedBrokerFillEvent),
                                _EVENT_DOMAIN,
                            ),
                        )
                        observation_accounts.setdefault(row[0], set()).add(event.account_id)
                        normalized_by_observation.setdefault(row[0], []).append((event, row[1]))
                    for row in connection.execute(
                        """SELECT a.observation_id, o.account_id
                           FROM live_observation_ambiguity a JOIN live_orders o
                             ON o.client_order_id = a.candidate_client_order_id"""
                    ):
                        observation_accounts.setdefault(row[0], set()).add(row[1])

                    open_observations: dict[str, str] = {}
                    global_observation_blocker = False
                    for row in connection.execute(
                        """SELECT r.observation_id, r.resolution_status
                           FROM live_raw_observations r
                           LEFT JOIN live_observation_reconciliation_resolutions rr
                             ON rr.observation_id = r.observation_id
                           WHERE r.resolution_status IN ('unresolved','conflict','ambiguous')
                             AND rr.observation_id IS NULL"""
                    ):
                        attributed_accounts = observation_accounts.get(row[0], set())
                        if len(attributed_accounts) != 1:
                            global_observation_blocker = True
                        elif request.account_id in attributed_accounts:
                            open_observations[row[0]] = row[1]
                    if global_observation_blocker:
                        return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                    if set(open_observations) != {
                        item.observation_id for item in request.observation_resolutions
                    }:
                        return rejected(ReconciliationCommitDisposition.VERSION_MISMATCH)
                    for observation_directive in request.observation_resolutions:
                        if (
                            open_observations[observation_directive.observation_id]
                            != observation_directive.expected_status.value
                        ):
                            return rejected(ReconciliationCommitDisposition.VERSION_MISMATCH)

                    open_requirements: dict[int, tuple[str | None, str | None, str, str]] = {}
                    global_requirement_blocker = False
                    order_accounts = {
                        row[0]: row[1]
                        for row in connection.execute(
                            "SELECT client_order_id, account_id FROM live_orders"
                        )
                    }
                    for row in connection.execute(
                        """SELECT r.requirement_id, r.client_order_id, r.observation_id,
                                  r.reason_code, r.created_at
                           FROM live_reconciliation_requirements r
                           LEFT JOIN live_reconciliation_requirement_resolutions rr
                             ON rr.requirement_id = r.requirement_id
                           WHERE r.resolved_at IS NULL AND rr.requirement_id IS NULL"""
                    ):
                        requirement_accounts: set[str] = set()
                        if row[1] is not None and row[1] in order_accounts:
                            requirement_accounts.add(order_accounts[row[1]])
                        if row[2] is not None:
                            requirement_accounts.update(observation_accounts.get(row[2], set()))
                        if len(requirement_accounts) != 1:
                            global_requirement_blocker = True
                        elif request.account_id in requirement_accounts:
                            open_requirements[int(row[0])] = (row[1], row[2], row[3], row[4])
                    if global_requirement_blocker:
                        return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                    if set(open_requirements) != {
                        item.requirement_id for item in request.requirement_resolutions
                    }:
                        return rejected(ReconciliationCommitDisposition.VERSION_MISMATCH)

                    projection_by_order = {
                        order.intent.client_order_id: order for order in request.order_projections
                    }
                    durable_account_orders = {
                        row[0]: self._load_order_row(row[0])
                        for row in connection.execute(
                            "SELECT client_order_id FROM live_orders WHERE account_id = ?",
                            (request.account_id,),
                        )
                    }
                    final_account_orders = {
                        order_id: projection_by_order.get(order_id, order)
                        for order_id, order in durable_account_orders.items()
                    }
                    if any(
                        order is not None and order.pending_command is not None
                        for order in final_account_orders.values()
                    ):
                        return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)

                    def matching_broker_evidence(
                        order: LiveOrder, resolution: ClaimResolution | ObservationResolution
                    ) -> bool:
                        values = (
                            assessment.broker_snapshot.open_orders.orders
                            if resolution.value == "broker_order_confirmed"
                            else assessment.broker_snapshot.fills.fills
                        )
                        matches = [
                            item
                            for item in values
                            if item.account_id == request.account_id
                            and item.instrument_id == order.intent.instrument_id
                            and item.side is order.intent.side
                            and item.correlation.status is CorrelationStatus.CONFIRMED
                            and item.correlation.client_order_id == order.intent.client_order_id
                        ]
                        return bool(matches)

                    for command_id, row in claim_rows.items():
                        command = self._load_command(command_id)
                        claim_current_order = self._load_order_row(row[5])
                        claim_projected_order = projection_by_order.get(row[5])
                        evidence_order = claim_projected_order or claim_current_order
                        if (
                            type(command) is not NewOrderCommand
                            or claim_current_order is None
                            or evidence_order is None
                            or (
                                claim_projected_order is None
                                and claim_current_order.pending_command is not None
                            )
                            or (
                                claim_projected_order is not None
                                and (
                                    claim_current_order.pending_command is None
                                    or claim_current_order.pending_command.command != command
                                    or claim_projected_order.intent != claim_current_order.intent
                                    or claim_projected_order.version
                                    != claim_current_order.version + 1
                                    or claim_projected_order.state is not LiveOrderState.ACCEPTED
                                    or claim_projected_order.pending_command is not None
                                    or claim_projected_order.filled_quantity != 0
                                    or claim_projected_order.remaining_quantity
                                    != claim_current_order.total_quantity
                                    or claim_projected_order.average_fill_price is not None
                                )
                            )
                            or not matching_broker_evidence(
                                evidence_order, claim_directives[command_id].resolution
                            )
                        ):
                            return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)

                    observation_orders: dict[str, LiveOrder] = {}
                    for observation_directive in request.observation_resolutions:
                        candidates = [
                            event
                            for event, event_id in normalized_by_observation.get(
                                observation_directive.observation_id, []
                            )
                            if event_id == observation_directive.normalized_event_id
                        ]
                        if len(candidates) != 1:
                            return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                        resolution_event = candidates[0]
                        expected_event_type = (
                            NormalizedBrokerOrderEvent
                            if observation_directive.resolution
                            is ObservationResolution.BROKER_ORDER_CONFIRMED
                            else NormalizedBrokerFillEvent
                        )
                        client_order_id = resolution_event.correlation.client_order_id
                        order = (
                            self._load_order_row(client_order_id)
                            if client_order_id is not None
                            else None
                        )
                        if (
                            type(resolution_event) is not expected_event_type
                            or resolution_event.account_id != request.account_id
                            or resolution_event.correlation.status
                            is not CorrelationStatus.CONFIRMED
                            or order is None
                            or (
                                type(resolution_event) is NormalizedBrokerFillEvent
                                and resolution_event.side is not order.intent.side
                            )
                            or not matching_broker_evidence(order, observation_directive.resolution)
                        ):
                            return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)
                        observation_orders[observation_directive.observation_id] = order

                    resolved_claim_orders = {row[5] for row in claim_rows.values()}
                    resolved_observation_ids = set(open_observations)
                    for cause_order_id, cause_observation_id, _, _ in open_requirements.values():
                        if cause_order_id is not None:
                            outstanding_for_order = {
                                row[5] for row in claim_rows.values() if row[5] == cause_order_id
                            }
                            if (
                                outstanding_for_order
                                and cause_order_id not in resolved_claim_orders
                            ):
                                return rejected(
                                    ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION
                                )
                        if (
                            cause_observation_id is not None
                            and cause_observation_id not in resolved_observation_ids
                        ):
                            return rejected(ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION)

                    committed_at = self._now()
                    if committed_at < assessment.result.reconciled_at:
                        return rejected(ReconciliationCommitDisposition.NOT_AUTHORITATIVE)
                    target_count = (
                        len(request.claim_resolutions)
                        + len(request.observation_resolutions)
                        + len(request.requirement_resolutions)
                    )
                    resulting_sequence = sequence + target_count + 1

                    for projected_order in request.order_projections:
                        expected_version = expected_versions[projected_order.intent.client_order_id]
                        if not self._write_order(projected_order, expected_version):
                            raise LiveJournalIntegrityError(
                                "live journal reconciliation projection write failed"
                            )
                    connection.execute(
                        """INSERT INTO live_reconciliation_commits(
                               commit_id, account_id, expected_journal_sequence,
                               base_journal_sequence, snapshot_id, request_payload,
                               request_digest, committed_at, resulting_journal_sequence
                           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            request.commit_id,
                            request.account_id,
                            request.expected_journal_sequence,
                            sequence,
                            assessment.broker_snapshot.snapshot_id,
                            request_payload,
                            request_digest,
                            _timestamp(committed_at),
                            resulting_sequence,
                        ),
                    )

                    for directive in request.claim_resolutions:
                        row = claim_rows[directive.client_command_id]
                        precondition = _scalar_digest(
                            _CLAIM_RESOLUTION_DOMAIN,
                            {
                                "client_command_id": directive.client_command_id,
                                "claim_token": row[1],
                                "claim_version": int(row[2]),
                                "order_version": int(row[7]),
                            },
                        )
                        resolution_digest = _scalar_digest(
                            _CLAIM_RESOLUTION_DOMAIN,
                            {
                                "commit_id": request.commit_id,
                                "client_command_id": directive.client_command_id,
                                "precondition": precondition,
                                "resolution": directive.resolution.value,
                                "resolved_at": _timestamp(committed_at),
                            },
                        )
                        connection.execute(
                            """INSERT INTO live_dispatch_claim_resolutions VALUES
                               (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                directive.client_command_id,
                                request.commit_id,
                                row[1],
                                int(row[2]),
                                int(row[7]),
                                precondition,
                                directive.resolution.value,
                                _timestamp(committed_at),
                                resolution_digest,
                            ),
                        )
                        self._append_record(
                            "dispatch-claim-resolution",
                            directive.client_command_id,
                            resolution_digest,
                            committed_at,
                        )

                    for observation_directive in request.observation_resolutions:
                        precondition = _scalar_digest(
                            _OBSERVATION_RECONCILIATION_DOMAIN,
                            {
                                "observation_id": observation_directive.observation_id,
                                "status": observation_directive.expected_status.value,
                                "normalized_event_id": observation_directive.normalized_event_id,
                            },
                        )
                        resolution_digest = _scalar_digest(
                            _OBSERVATION_RECONCILIATION_DOMAIN,
                            {
                                "commit_id": request.commit_id,
                                "observation_id": observation_directive.observation_id,
                                "precondition": precondition,
                                "resolution": observation_directive.resolution.value,
                                "resolved_at": _timestamp(committed_at),
                            },
                        )
                        connection.execute(
                            """INSERT INTO live_observation_reconciliation_resolutions VALUES
                               (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                observation_directive.observation_id,
                                request.commit_id,
                                observation_directive.expected_status.value,
                                precondition,
                                observation_directive.normalized_event_id,
                                observation_directive.resolution.value,
                                _timestamp(committed_at),
                                resolution_digest,
                            ),
                        )
                        self._append_record(
                            "observation-reconciliation-resolution",
                            observation_directive.observation_id,
                            resolution_digest,
                            committed_at,
                        )

                    for requirement_directive in request.requirement_resolutions:
                        row = open_requirements[requirement_directive.requirement_id]
                        precondition = _scalar_digest(
                            _REQUIREMENT_RESOLUTION_DOMAIN,
                            {
                                "requirement_id": requirement_directive.requirement_id,
                                "client_order_id": row[0],
                                "observation_id": row[1],
                                "reason_code": row[2],
                                "created_at": row[3],
                            },
                        )
                        resolution_digest = _scalar_digest(
                            _REQUIREMENT_RESOLUTION_DOMAIN,
                            {
                                "commit_id": request.commit_id,
                                "requirement_id": requirement_directive.requirement_id,
                                "precondition": precondition,
                                "resolution": requirement_directive.resolution.value,
                                "resolved_at": _timestamp(committed_at),
                            },
                        )
                        connection.execute(
                            """INSERT INTO live_reconciliation_requirement_resolutions VALUES
                               (?, ?, ?, ?, ?, ?)""",
                            (
                                requirement_directive.requirement_id,
                                request.commit_id,
                                precondition,
                                requirement_directive.resolution.value,
                                _timestamp(committed_at),
                                resolution_digest,
                            ),
                        )
                        self._append_record(
                            "reconciliation-requirement-resolution",
                            str(requirement_directive.requirement_id),
                            resolution_digest,
                            committed_at,
                        )

                    commit_digest = _scalar_digest(
                        _COMMIT_FACT_DOMAIN,
                        {
                            "commit_id": request.commit_id,
                            "account_id": request.account_id,
                            "request_digest": request_digest,
                            "base_sequence": sequence,
                            "resulting_sequence": resulting_sequence,
                            "committed_at": _timestamp(committed_at),
                        },
                    )
                    final_sequence = self._append_record(
                        "reconciliation-commit",
                        request.commit_id,
                        commit_digest,
                        committed_at,
                    )
                    if final_sequence != resulting_sequence:
                        raise LiveJournalIntegrityError(
                            "live journal reconciliation sequence is invalid"
                        )
                return DurableReconciliationCommitResult(
                    request.commit_id,
                    request.account_id,
                    ReconciliationCommitDisposition.COMMITTED,
                    committed_at,
                    resulting_sequence,
                    tuple(sorted(claim_directives)),
                    tuple(sorted(open_observations)),
                    tuple(sorted(open_requirements)),
                    request.order_projections,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None

    def _load_applied_event_ledger(self) -> AppliedEventLedger:
        connection = self._require_connection()
        dispatch_events: list[AppliedEvent] = []
        for row in connection.execute(
            """SELECT client_command_id, payload_fingerprint, payload, payload_digest
               FROM live_dispatch_receipts ORDER BY rowid"""
        ):
            receipt = cast(
                DispatchReceipt,
                _decode(row[2], row[3], DispatchReceipt, _RECEIPT_DOMAIN),
            )
            if row[0] != receipt.client_command_id or row[1] != receipt.payload_fingerprint:
                raise LiveJournalIntegrityError("live journal receipt is invalid")
            fingerprint = f"sha256:{sha256(canonical_bytes(receipt)).hexdigest()}"
            dispatch_events.append(AppliedEvent("dispatch", row[0], fingerprint))
        broker_events: list[AppliedEvent] = []
        for row in connection.execute(
            """SELECT n.source, n.event_id, n.semantic_fingerprint,
                          n.payload, n.payload_digest
                   FROM live_normalized_events n
                   JOIN live_event_applications a
                     ON a.source = n.source AND a.event_id = n.event_id
                   WHERE a.disposition = 'applied'
                   ORDER BY n.rowid"""
        ):
            event = cast(
                NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
                _decode(
                    row[3],
                    row[4],
                    (NormalizedBrokerOrderEvent, NormalizedBrokerFillEvent),
                    _EVENT_DOMAIN,
                ),
            )
            if row[1] != event.event_id or row[2] != broker_semantic_fingerprint(event):
                raise LiveJournalIntegrityError("live journal event is invalid")
            broker_events.append(AppliedEvent(row[0], row[1], row[2]))
        return AppliedEventLedger(tuple((*dispatch_events, *broker_events)))

    def _verify_durable_payloads(self) -> None:
        connection = self._require_connection()
        expected_records: dict[tuple[str, str], tuple[str, str]] = {}
        identity_payload, identity_digest = _encode(self._identity, _IDENTITY_DOMAIN)
        del identity_payload
        expected_records[("identity", self._identity.journal_id)] = (
            identity_digest,
            _timestamp(self._identity.created_at),
        )
        database_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if database_version == DATABASE_SCHEMA_VERSION:
            migration_record = connection.execute(
                """SELECT payload_digest, recorded_at FROM live_journal_records
                   WHERE record_kind = 'schema-migration' AND record_id = ?""",
                (_SCHEMA_MIGRATION_RECORD_ID,),
            ).fetchone()
            if migration_record is None:
                raise LiveJournalIntegrityError("live journal migration record is missing")
            _, v1_fingerprint = _v1_schema_material()
            _, fingerprint = _schema_material()
            migration_digest = _scalar_digest(
                _SCHEMA_MIGRATION_FACT_DOMAIN,
                {
                    "version": DATABASE_SCHEMA_VERSION,
                    "from_fingerprint": v1_fingerprint,
                    "to_fingerprint": fingerprint,
                    "recorded_at": migration_record[1],
                },
            )
            if migration_record[0] != migration_digest:
                raise LiveJournalIntegrityError("live journal migration record is invalid")
            expected_records[("schema-migration", _SCHEMA_MIGRATION_RECORD_ID)] = (
                migration_digest,
                migration_record[1],
            )
        for reservation in connection.execute(
            """SELECT client_order_id, intent_fingerprint
               FROM live_order_id_reservations"""
        ):
            history = connection.execute(
                """SELECT order_version, payload, payload_digest, recorded_at
                   FROM live_order_history WHERE client_order_id = ?
                   ORDER BY order_version""",
                (reservation[0],),
            ).fetchall()
            current = self._load_order_row(reservation[0])
            if not history or current is None:
                raise LiveJournalIntegrityError("live journal order history is invalid")
            versions: list[int] = []
            first_digest = ""
            first_recorded_at = ""
            for index, row in enumerate(history):
                order = cast(LiveOrder, _decode(row[1], row[2], LiveOrder, _ORDER_DOMAIN))
                if (
                    order.intent.client_order_id != reservation[0]
                    or row[0] != order.version
                    or order.intent != current.intent
                    or row[3] != _timestamp(order.updated_at)
                ):
                    raise LiveJournalIntegrityError("live journal order history is invalid")
                versions.append(row[0])
                if index == 0:
                    first_digest = row[2]
                    first_recorded_at = row[3]
            if (
                any(right != left + 1 for left, right in zip(versions, versions[1:]))
                or versions[-1] != current.version
            ):
                raise LiveJournalIntegrityError("live journal order history is invalid")
            from .live_journal_contracts import (
                intent_fingerprint as calculate_intent_fingerprint,
            )

            if reservation[1] != calculate_intent_fingerprint(current.intent):
                raise LiveJournalIntegrityError("live journal reservation is invalid")
            expected_records[("order", reservation[0])] = (
                first_digest,
                first_recorded_at,
            )
        for row in connection.execute(
            """SELECT client_command_id, client_order_id, command_kind,
                      payload_fingerprint, payload, payload_digest
               FROM live_commands"""
        ):
            command = cast(
                LiveCommand,
                _decode(
                    row[4],
                    row[5],
                    tuple(_COMMAND_DOMAINS),
                    _COMMAND_DOMAIN,
                ),
            )
            if (
                row[0] != command.client_command_id
                or row[1] != _command_order_id(command)
                or row[2] != command.kind.value
                or row[3] != _command_fingerprint(command)
            ):
                raise LiveJournalIntegrityError("live journal command is invalid")
            expected_records[("command", row[0])] = (
                row[5],
                _timestamp(command.requested_at),
            )
        for row in connection.execute(
            """SELECT dc.client_command_id, dc.claim_token, dc.claimant_id,
                      dc.expected_order_version, dc.claim_version, dc.claimed_at
               FROM live_dispatch_claims dc"""
        ):
            command = self._load_command(row[0])
            claimed_at = _parse_timestamp(row[5])
            if row[3] != row[4]:
                raise LiveJournalIntegrityError("live journal dispatch claim is invalid")
            claim = OutstandingDispatchClaim(command, row[1], row[2], row[3], claimed_at)
            claim_payload, claim_digest = _encode(claim, _CLAIM_DOMAIN)
            del claim_payload
            expected_records[("dispatch-claim", row[0])] = (
                claim_digest,
                row[5],
            )
        for row in connection.execute(
            """SELECT client_command_id, payload_fingerprint, payload,
                      payload_digest, recorded_at FROM live_dispatch_receipts"""
        ):
            receipt = cast(
                DispatchReceipt,
                _decode(row[2], row[3], DispatchReceipt, _RECEIPT_DOMAIN),
            )
            if (
                row[0] != receipt.client_command_id
                or row[1] != receipt.payload_fingerprint
                or _parse_timestamp(row[4]) != (receipt.completed_at or receipt.attempted_at)
            ):
                raise LiveJournalIntegrityError("live journal receipt is invalid")
            expected_records[("dispatch-receipt", row[0])] = (row[3], row[4])
        for row in connection.execute(
            """SELECT observation_id, source, broker_session_generation,
                      adapter_received_sequence, received_at, payload, payload_digest,
                      resolution_status
               FROM live_raw_observations"""
        ):
            observation = cast(
                RawBrokerObservation,
                _decode(row[5], row[6], RawBrokerObservation, _RAW_DOMAIN),
            )
            if (
                row[0] != observation.observation_id
                or row[1] != observation.source
                or row[2] != observation.broker_session_generation
                or row[3] != observation.adapter_received_sequence
                or _parse_timestamp(row[4]) != observation.received_at
            ):
                raise LiveJournalIntegrityError("live journal observation is invalid")
            expected_records[("raw-observation", row[0])] = (row[6], row[4])
            resolution = connection.execute(
                """SELECT payload_digest, recorded_at FROM live_journal_records
                   WHERE record_kind = 'observation-resolution' AND record_id = ?""",
                (row[0],),
            ).fetchone()
            if row[7] == "unresolved":
                if resolution is not None:
                    raise LiveJournalIntegrityError(
                        "live journal observation resolution is invalid"
                    )
            else:
                if resolution is None:
                    raise LiveJournalIntegrityError(
                        "live journal observation resolution is invalid"
                    )
                expected_resolution_digest = _resolution_digest(row[0], row[7], resolution[1])
                if resolution[0] != expected_resolution_digest:
                    raise LiveJournalIntegrityError(
                        "live journal observation resolution is invalid"
                    )
                if row[7] == "conflict":
                    requirement = connection.execute(
                        """SELECT 1 FROM live_reconciliation_requirements
                           WHERE observation_id = ? AND resolved_at IS NULL""",
                        (row[0],),
                    ).fetchone()
                    if requirement is None:
                        raise LiveJournalIntegrityError(
                            "live journal observation resolution is invalid"
                        )
                expected_records[("observation-resolution", row[0])] = (
                    resolution[0],
                    resolution[1],
                )
        for row in connection.execute(
            "SELECT candidate_client_order_id FROM live_observation_ambiguity"
        ):
            if type(row[0]) is not str or _IDENTIFIER.fullmatch(row[0]) is None:
                raise LiveJournalIntegrityError("live journal observation ambiguity is invalid")
        for row in connection.execute(
            """SELECT r.observation_id, r.resolution_status,
                      count(a.candidate_client_order_id)
               FROM live_raw_observations r
               LEFT JOIN live_observation_ambiguity a
                 ON a.observation_id = r.observation_id
               GROUP BY r.observation_id, r.resolution_status"""
        ):
            candidate_count = int(row[2])
            if (row[1] == "ambiguous" and candidate_count < 2) or (
                row[1] != "ambiguous" and candidate_count != 0
            ):
                raise LiveJournalIntegrityError("live journal observation ambiguity is invalid")
        for row in connection.execute(
            """SELECT n.source, n.event_id, n.raw_observation_id,
                      n.semantic_fingerprint, n.payload, n.payload_digest,
                      n.received_at, a.client_order_id, a.disposition,
                      a.failure_code, a.applied_at
               FROM live_normalized_events n
               LEFT JOIN live_event_applications a
                 ON a.source = n.source AND a.event_id = n.event_id"""
        ):
            event = cast(
                NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
                _decode(
                    row[4],
                    row[5],
                    (NormalizedBrokerOrderEvent, NormalizedBrokerFillEvent),
                    _EVENT_DOMAIN,
                ),
            )
            raw = connection.execute(
                """SELECT broker_session_generation, adapter_received_sequence,
                          received_at, resolution_status FROM live_raw_observations
                   WHERE observation_id = ?""",
                (row[2],),
            ).fetchone()
            valid_dispositions = {
                EventApplicationDisposition.APPLIED.value,
                EventApplicationDisposition.UNRESOLVED.value,
                EventApplicationDisposition.EVENT_CONFLICT.value,
            }
            provenance_ok = (
                raw is not None
                and raw[0] == event.broker_session_generation
                and raw[1] == event.adapter_received_sequence
                and _parse_timestamp(raw[2]) == event.received_at
            )
            conflict_requirement = connection.execute(
                """SELECT 1 FROM live_reconciliation_requirements
                   WHERE observation_id = ? AND resolved_at IS NULL""",
                (row[2],),
            ).fetchone()
            if (
                row[0] != "broker-event"
                or row[1] != event.event_id
                or row[3] != broker_semantic_fingerprint(event)
                or _parse_timestamp(row[6]) != event.received_at
                or row[8] not in valid_dispositions
                or _parse_timestamp(row[10]) != event.received_at
                or (
                    not provenance_ok
                    and (
                        row[8] != EventApplicationDisposition.EVENT_CONFLICT.value
                        or conflict_requirement is None
                    )
                )
                or (row[7] is not None and row[7] != event.correlation.client_order_id)
                or (
                    row[8] == EventApplicationDisposition.APPLIED.value
                    and (raw is None or raw[3] != "resolved")
                )
                or (
                    row[8] == EventApplicationDisposition.EVENT_CONFLICT.value
                    and (raw is None or raw[3] != "conflict")
                )
                or (
                    row[8] == EventApplicationDisposition.UNRESOLVED.value
                    and (
                        raw is None
                        or raw[3] not in {"unresolved", "conflict"}
                        or (raw[3] == "conflict" and conflict_requirement is None)
                    )
                )
            ):
                raise LiveJournalIntegrityError("live journal event application is invalid")
            if row[8] == EventApplicationDisposition.EVENT_CONFLICT.value:
                if row[9] != LiveFailureCode.CORRELATION_CONFLICT.value:
                    raise LiveJournalIntegrityError("live journal event application is invalid")
            application_digest = _application_digest(
                event_payload_digest=row[5],
                raw_observation_id=row[2],
                client_order_id=row[7],
                disposition=row[8],
                failure_code=row[9],
                applied_at=row[10],
            )
            expected_records[("normalized-application", row[1])] = (
                application_digest,
                row[10],
            )
        for row in connection.execute(
            """SELECT fill_id, client_order_id, payload, payload_digest
               FROM live_fills"""
        ):
            fill = cast(LiveFill, _decode(row[2], row[3], LiveFill, _FILL_DOMAIN))
            fill_order = self._load_order_row(row[1])
            if (
                row[0] != fill.fill_id
                or row[1] != fill.client_order_id
                or fill_order is None
                or fill.account_id != fill_order.intent.account_id
                or fill.strategy_id != fill_order.intent.strategy_id
                or fill.instrument_id != fill_order.intent.instrument_id
                or fill.side is not fill_order.intent.side
            ):
                raise LiveJournalIntegrityError("live journal fill is invalid")
        for row in connection.execute(
            """SELECT requirement_id, client_order_id, observation_id,
                      reason_code, created_at, resolved_at
               FROM live_reconciliation_requirements"""
        ):
            requirement = DurableReconciliationRequirement(
                row[0], row[3], _parse_timestamp(row[4]), row[1], row[2]
            )
            if row[5] is not None:
                raise LiveJournalIntegrityError(
                    "resolved reconciliation requirements are unsupported"
                )
            payload, digest = _encode(requirement, _RECONCILIATION_DOMAIN)
            del payload
            expected_records[("reconciliation", str(row[0]))] = (digest, row[4])
        if database_version == DATABASE_SCHEMA_VERSION:
            commit_requests: dict[str, DurableReconciliationCommitRequest] = {}
            for row in connection.execute(
                """SELECT commit_id, account_id, expected_journal_sequence,
                          base_journal_sequence, snapshot_id, request_payload,
                          request_digest, committed_at, resulting_journal_sequence
                   FROM live_reconciliation_commits"""
            ):
                request = cast(
                    DurableReconciliationCommitRequest,
                    _decode(
                        row[5],
                        row[6],
                        DurableReconciliationCommitRequest,
                        _COMMIT_REQUEST_DOMAIN,
                    ),
                )
                record = connection.execute(
                    """SELECT journal_sequence, payload_digest, recorded_at
                       FROM live_journal_records
                       WHERE record_kind = 'reconciliation-commit' AND record_id = ?""",
                    (row[0],),
                ).fetchone()
                if (
                    record is None
                    or row[0] != request.commit_id
                    or row[1] != request.account_id
                    or int(row[2]) != request.expected_journal_sequence
                    or int(row[3]) != request.expected_journal_sequence
                    or row[4] != request.assessment.broker_snapshot.snapshot_id
                    or int(row[8]) != int(record[0])
                    or row[7] != record[2]
                ):
                    raise LiveJournalIntegrityError("live journal reconciliation commit is invalid")
                commit_digest = _scalar_digest(
                    _COMMIT_FACT_DOMAIN,
                    {
                        "commit_id": row[0],
                        "account_id": row[1],
                        "request_digest": row[6],
                        "base_sequence": int(row[3]),
                        "resulting_sequence": int(row[8]),
                        "committed_at": row[7],
                    },
                )
                if record[1] != commit_digest:
                    raise LiveJournalIntegrityError("live journal reconciliation commit is invalid")
                expected_records[("reconciliation-commit", row[0])] = (
                    commit_digest,
                    row[7],
                )
                recomputed_assessment = assess_reconciliation(
                    request.assessment.local_snapshot,
                    request.assessment.broker_snapshot,
                    request.assessment.result.reconciled_at,
                )
                if recomputed_assessment != request.assessment:
                    raise LiveJournalIntegrityError(
                        "live journal reconciliation projection is invalid"
                    )
                projection_plan = project_authoritative_orders(request.assessment)
                if request.order_projections:
                    if (
                        projection_plan.disposition is not OrderProjectionDisposition.READY
                        or request.expected_order_versions
                        != projection_plan.expected_order_versions
                        or request.order_projections != projection_plan.projected_orders
                    ):
                        raise LiveJournalIntegrityError(
                            "live journal reconciliation projection is invalid"
                        )
                    local_by_order = {
                        order.intent.client_order_id: order
                        for order in request.assessment.local_snapshot.orders
                    }
                    expected_by_order = {
                        item.client_order_id: item.version
                        for item in request.expected_order_versions
                    }
                    requested_claim_ids = {
                        item.client_command_id for item in request.claim_resolutions
                    }
                    claim_directives_by_id = {
                        item.client_command_id: item for item in request.claim_resolutions
                    }
                    for projected_order in request.order_projections:
                        order_id = projected_order.intent.client_order_id
                        expected_version = expected_by_order[order_id]
                        prior_order = self._load_history_order(order_id, expected_version)
                        projection_row = connection.execute(
                            """SELECT payload, payload_digest FROM live_order_history
                               WHERE client_order_id = ? AND order_version = ?""",
                            (order_id, projected_order.version),
                        ).fetchone()
                        projection_payload, projection_digest = _encode(
                            projected_order, _ORDER_DOMAIN
                        )
                        pending_command_id = (
                            prior_order.pending_command.client_command_id
                            if prior_order is not None and prior_order.pending_command is not None
                            else None
                        )
                        claim_directive = (
                            claim_directives_by_id.get(pending_command_id)
                            if pending_command_id is not None
                            else None
                        )
                        version_audit = (
                            connection.execute(
                                """SELECT r.expected_order_version,
                                          dc.expected_order_version, dc.claim_version
                                   FROM live_dispatch_claim_resolutions r
                                   JOIN live_dispatch_claims dc
                                     ON dc.client_command_id = r.client_command_id
                                   WHERE r.client_command_id = ? AND r.commit_id = ?""",
                                (pending_command_id, request.commit_id),
                            ).fetchone()
                            if pending_command_id is not None
                            else None
                        )
                        if (
                            prior_order is None
                            or prior_order.pending_command is None
                            or type(prior_order.pending_command.command) is not NewOrderCommand
                            or pending_command_id not in requested_claim_ids
                            or claim_directive is None
                            or claim_directive.resolution
                            is not ClaimResolution.BROKER_ORDER_CONFIRMED
                            or version_audit is None
                            or any(int(value) != expected_version for value in version_audit)
                            or prior_order != local_by_order.get(order_id)
                            or prior_order.intent != projected_order.intent
                            or projected_order.version != expected_version + 1
                            or projection_row is None
                            or projection_row[0] != projection_payload
                            or projection_row[1] != projection_digest
                        ):
                            raise LiveJournalIntegrityError(
                                "live journal reconciliation projection history is invalid"
                            )
                elif projection_plan.disposition is OrderProjectionDisposition.READY:
                    raise LiveJournalIntegrityError(
                        "live journal reconciliation projection is missing"
                    )
                elif (
                    not request.assessment.result.is_authoritative
                    or request.assessment.result.discrepancies
                ):
                    raise LiveJournalIntegrityError(
                        "live journal reconciliation assessment is invalid"
                    )
                commit_requests[row[0]] = request

            for commit_id, verified_request in commit_requests.items():
                requested_claims = {
                    item.client_command_id for item in verified_request.claim_resolutions
                }
                requested_observations = {
                    item.observation_id for item in verified_request.observation_resolutions
                }
                requested_requirements = {
                    item.requirement_id for item in verified_request.requirement_resolutions
                }
                target_count = (
                    len(requested_claims)
                    + len(requested_observations)
                    + len(requested_requirements)
                )
                observed_claims = {
                    row[0]
                    for row in connection.execute(
                        """SELECT client_command_id
                           FROM live_dispatch_claim_resolutions
                           WHERE commit_id = ?""",
                        (commit_id,),
                    )
                }
                observed_observations = {
                    row[0]
                    for row in connection.execute(
                        """SELECT observation_id
                           FROM live_observation_reconciliation_resolutions
                           WHERE commit_id = ?""",
                        (commit_id,),
                    )
                }
                observed_requirements = {
                    int(row[0])
                    for row in connection.execute(
                        """SELECT requirement_id
                           FROM live_reconciliation_requirement_resolutions
                           WHERE commit_id = ?""",
                        (commit_id,),
                    )
                }
                commit_row = connection.execute(
                    """SELECT base_journal_sequence, resulting_journal_sequence
                       FROM live_reconciliation_commits WHERE commit_id = ?""",
                    (commit_id,),
                ).fetchone()
                if (
                    commit_row is None
                    or (target_count == 0 and not verified_request.order_projections)
                    or requested_claims != observed_claims
                    or requested_observations != observed_observations
                    or requested_requirements != observed_requirements
                    or int(commit_row[0]) != verified_request.expected_journal_sequence
                    or int(commit_row[1])
                    != verified_request.expected_journal_sequence + target_count + 1
                ):
                    raise LiveJournalIntegrityError(
                        "live journal reconciliation commit mapping is invalid"
                    )

            for row in connection.execute(
                """SELECT r.client_command_id, r.commit_id,
                          r.expected_claim_token, r.expected_claim_version,
                          r.expected_order_version, r.expected_precondition_digest,
                          r.resolution_kind, r.resolved_at, r.resolution_digest,
                          dc.claim_token, dc.claim_version, c.client_order_id,
                          o.account_id
                   FROM live_dispatch_claim_resolutions r
                   JOIN live_dispatch_claims dc
                     ON dc.client_command_id = r.client_command_id
                   JOIN live_commands c ON c.client_command_id = r.client_command_id
                   JOIN live_orders o ON o.client_order_id = c.client_order_id"""
            ):
                stored_request = commit_requests.get(row[1])
                claim_directive = (
                    next(
                        (
                            item
                            for item in stored_request.claim_resolutions
                            if item.client_command_id == row[0]
                        ),
                        None,
                    )
                    if stored_request is not None
                    else None
                )
                precondition = _scalar_digest(
                    _CLAIM_RESOLUTION_DOMAIN,
                    {
                        "client_command_id": row[0],
                        "claim_token": row[2],
                        "claim_version": int(row[3]),
                        "order_version": int(row[4]),
                    },
                )
                resolution_digest = _scalar_digest(
                    _CLAIM_RESOLUTION_DOMAIN,
                    {
                        "commit_id": row[1],
                        "client_command_id": row[0],
                        "precondition": precondition,
                        "resolution": row[6],
                        "resolved_at": row[7],
                    },
                )
                if (
                    stored_request is None
                    or claim_directive is None
                    or stored_request.account_id != row[12]
                    or claim_directive.claim_token != row[2]
                    or claim_directive.resolution.value != row[6]
                    or row[2] != row[9]
                    or int(row[3]) != int(row[10])
                    or row[5] != precondition
                    or row[8] != resolution_digest
                    or connection.execute(
                        "SELECT 1 FROM live_dispatch_receipts WHERE client_command_id = ?",
                        (row[0],),
                    ).fetchone()
                    is not None
                ):
                    raise LiveJournalIntegrityError("live journal claim resolution is invalid")
                expected_records[("dispatch-claim-resolution", row[0])] = (
                    resolution_digest,
                    row[7],
                )

            for row in connection.execute(
                """SELECT r.observation_id, r.commit_id,
                          r.expected_resolution_status,
                          r.expected_precondition_digest, r.normalized_event_id,
                          r.resolution_kind, r.resolved_at, r.resolution_digest,
                          raw.resolution_status
                   FROM live_observation_reconciliation_resolutions r
                   JOIN live_raw_observations raw
                     ON raw.observation_id = r.observation_id"""
            ):
                stored_request = commit_requests.get(row[1])
                observation_directive = (
                    next(
                        (
                            item
                            for item in stored_request.observation_resolutions
                            if item.observation_id == row[0]
                        ),
                        None,
                    )
                    if stored_request is not None
                    else None
                )
                provenance = connection.execute(
                    """SELECT 1 FROM live_normalized_events
                       WHERE raw_observation_id = ? AND event_id = ?""",
                    (row[0], row[4]),
                ).fetchall()
                precondition = _scalar_digest(
                    _OBSERVATION_RECONCILIATION_DOMAIN,
                    {
                        "observation_id": row[0],
                        "status": row[2],
                        "normalized_event_id": row[4],
                    },
                )
                resolution_digest = _scalar_digest(
                    _OBSERVATION_RECONCILIATION_DOMAIN,
                    {
                        "commit_id": row[1],
                        "observation_id": row[0],
                        "precondition": precondition,
                        "resolution": row[5],
                        "resolved_at": row[6],
                    },
                )
                if (
                    observation_directive is None
                    or observation_directive.expected_status.value != row[2]
                    or observation_directive.normalized_event_id != row[4]
                    or observation_directive.resolution.value != row[5]
                    or row[8] != row[2]
                    or len(provenance) != 1
                    or row[3] != precondition
                    or row[7] != resolution_digest
                ):
                    raise LiveJournalIntegrityError(
                        "live journal observation reconciliation resolution is invalid"
                    )
                expected_records[("observation-reconciliation-resolution", row[0])] = (
                    resolution_digest,
                    row[6],
                )

            for row in connection.execute(
                """SELECT rr.requirement_id, rr.commit_id,
                          rr.expected_precondition_digest, rr.resolution_kind,
                          rr.resolved_at, rr.resolution_digest,
                          r.client_order_id, r.observation_id, r.reason_code, r.created_at
                   FROM live_reconciliation_requirement_resolutions rr
                   JOIN live_reconciliation_requirements r
                     ON r.requirement_id = rr.requirement_id"""
            ):
                stored_request = commit_requests.get(row[1])
                requirement_directive = (
                    next(
                        (
                            item
                            for item in stored_request.requirement_resolutions
                            if item.requirement_id == int(row[0])
                        ),
                        None,
                    )
                    if stored_request is not None
                    else None
                )
                precondition = _scalar_digest(
                    _REQUIREMENT_RESOLUTION_DOMAIN,
                    {
                        "requirement_id": int(row[0]),
                        "client_order_id": row[6],
                        "observation_id": row[7],
                        "reason_code": row[8],
                        "created_at": row[9],
                    },
                )
                resolution_digest = _scalar_digest(
                    _REQUIREMENT_RESOLUTION_DOMAIN,
                    {
                        "commit_id": row[1],
                        "requirement_id": int(row[0]),
                        "precondition": precondition,
                        "resolution": row[3],
                        "resolved_at": row[4],
                    },
                )
                if (
                    requirement_directive is None
                    or requirement_directive.resolution.value != row[3]
                    or row[2] != precondition
                    or row[5] != resolution_digest
                ):
                    raise LiveJournalIntegrityError(
                        "live journal requirement resolution is invalid"
                    )
                expected_records[("reconciliation-requirement-resolution", str(row[0]))] = (
                    resolution_digest,
                    row[4],
                )
        actual_records = {
            (row[0], row[1]): (row[2], row[3])
            for row in connection.execute(
                """SELECT record_kind, record_id, payload_digest, recorded_at
                   FROM live_journal_records"""
            )
        }
        if actual_records != expected_records:
            raise LiveJournalIntegrityError("live journal record mapping is invalid")
        sequence_row = connection.execute(
            "SELECT count(*), min(journal_sequence), max(journal_sequence) FROM live_journal_records"
        ).fetchone()
        count = int(sequence_row[0])
        minimum = int(sequence_row[1] or 0)
        maximum = int(sequence_row[2] or 0)
        sqlite_sequence = connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'live_journal_records'"
        ).fetchone()
        if (
            count != len(expected_records)
            or minimum != 1
            or maximum != count
            or sqlite_sequence is None
            or int(sqlite_sequence[0]) != maximum
        ):
            raise LiveJournalIntegrityError("live journal sequence is not contiguous")

    def load_recovery_snapshot(self) -> LiveJournalRecoverySnapshot:
        with self._lock, self._operation():
            try:
                connection = self._require_connection()
                self._verify_durable_payloads()
                orders = tuple(
                    self._load_order_row(row[0])
                    for row in connection.execute(
                        "SELECT client_order_id FROM live_orders ORDER BY client_order_id"
                    )
                )
                if any(order is None for order in orders):
                    raise LiveJournalIntegrityError("live journal order projection is invalid")
                typed_orders = cast(tuple[LiveOrder, ...], orders)
                claims: list[OutstandingDispatchClaim] = []
                for row in connection.execute(
                    """SELECT c.client_command_id, c.client_order_id, c.command_kind,
                              c.payload_fingerprint, c.payload, c.payload_digest,
                              dc.claim_token, dc.claimant_id,
                              dc.expected_order_version, dc.claimed_at
                       FROM live_dispatch_claims dc JOIN live_commands c
                         ON c.client_command_id = dc.client_command_id
                       LEFT JOIN live_dispatch_receipts dr
                         ON dr.client_command_id = dc.client_command_id
                       LEFT JOIN live_dispatch_claim_resolutions dcr
                         ON dcr.client_command_id = dc.client_command_id
                       WHERE dr.client_command_id IS NULL
                         AND dcr.client_command_id IS NULL
                       ORDER BY dc.claimed_at, dc.client_command_id"""
                ):
                    command = cast(
                        LiveCommand,
                        _decode(
                            row[4],
                            row[5],
                            tuple(_COMMAND_DOMAINS),
                            _COMMAND_DOMAIN,
                        ),
                    )
                    if (
                        row[0] != command.client_command_id
                        or row[1] != _command_order_id(command)
                        or row[2] != command.kind.value
                        or row[3] != _command_fingerprint(command)
                    ):
                        raise LiveJournalIntegrityError("live journal command is invalid")
                    claims.append(
                        OutstandingDispatchClaim(
                            command,
                            row[6],
                            row[7],
                            row[8],
                            _parse_timestamp(row[9]),
                        )
                    )
                unresolved = tuple(
                    cast(
                        RawBrokerObservation,
                        _decode(row[0], row[1], RawBrokerObservation, _RAW_DOMAIN),
                    )
                    for row in connection.execute(
                        """SELECT r.payload, r.payload_digest FROM live_raw_observations r
                           LEFT JOIN live_observation_reconciliation_resolutions rr
                             ON rr.observation_id = r.observation_id
                           WHERE r.resolution_status = 'unresolved'
                             AND rr.observation_id IS NULL
                           ORDER BY r.received_at, r.observation_id"""
                    )
                )
                conflicts = tuple(
                    cast(
                        RawBrokerObservation,
                        _decode(row[0], row[1], RawBrokerObservation, _RAW_DOMAIN),
                    )
                    for row in connection.execute(
                        """SELECT r.payload, r.payload_digest FROM live_raw_observations r
                           LEFT JOIN live_observation_reconciliation_resolutions rr
                             ON rr.observation_id = r.observation_id
                           WHERE r.resolution_status = 'conflict'
                             AND rr.observation_id IS NULL
                           ORDER BY r.received_at, r.observation_id"""
                    )
                )
                ambiguous_by_id: dict[str, list[str]] = {}
                for row in connection.execute(
                    """SELECT a.observation_id, a.candidate_client_order_id
                       FROM live_observation_ambiguity a
                       LEFT JOIN live_observation_reconciliation_resolutions rr
                         ON rr.observation_id = a.observation_id
                       WHERE rr.observation_id IS NULL
                       ORDER BY a.observation_id, a.candidate_client_order_id"""
                ):
                    ambiguous_by_id.setdefault(row[0], []).append(row[1])
                ambiguous: list[AmbiguousObservation] = []
                for observation_id, candidates in ambiguous_by_id.items():
                    row = connection.execute(
                        """SELECT r.payload, r.payload_digest FROM live_raw_observations r
                           LEFT JOIN live_observation_reconciliation_resolutions rr
                             ON rr.observation_id = r.observation_id
                           WHERE r.observation_id = ? AND r.resolution_status = 'ambiguous'
                             AND rr.observation_id IS NULL""",
                        (observation_id,),
                    ).fetchone()
                    if row is None:
                        raise LiveJournalIntegrityError("live journal ambiguity is invalid")
                    observation = cast(
                        RawBrokerObservation,
                        _decode(row[0], row[1], RawBrokerObservation, _RAW_DOMAIN),
                    )
                    ambiguous.append(AmbiguousObservation(observation, tuple(candidates)))
                requirements = tuple(
                    DurableReconciliationRequirement(
                        row[0],
                        row[3],
                        _parse_timestamp(row[4]),
                        row[1],
                        row[2],
                    )
                    for row in connection.execute(
                        """SELECT r.requirement_id, r.client_order_id, r.observation_id,
                                  r.reason_code, r.created_at
                           FROM live_reconciliation_requirements r
                           LEFT JOIN live_reconciliation_requirement_resolutions rr
                             ON rr.requirement_id = r.requirement_id
                           WHERE r.resolved_at IS NULL AND rr.requirement_id IS NULL
                           ORDER BY r.requirement_id"""
                    )
                )
                sequence_row = connection.execute(
                    """SELECT count(*), min(journal_sequence), max(journal_sequence)
                       FROM live_journal_records"""
                ).fetchone()
                count = int(sequence_row[0])
                minimum = int(sequence_row[1] or 0)
                sequence = int(sequence_row[2] or 0)
                sqlite_sequence_row = connection.execute(
                    """SELECT seq FROM sqlite_sequence
                       WHERE name = 'live_journal_records'"""
                ).fetchone()
                expected_count = 2 + sum(
                    int(connection.execute(query).fetchone()[0])
                    for query in (
                        "SELECT count(*) FROM live_order_id_reservations",
                        "SELECT count(*) FROM live_commands",
                        "SELECT count(*) FROM live_dispatch_claims",
                        "SELECT count(*) FROM live_dispatch_receipts",
                        "SELECT count(*) FROM live_raw_observations",
                        """SELECT count(*) FROM live_raw_observations
                           WHERE resolution_status != 'unresolved'""",
                        "SELECT count(*) FROM live_normalized_events",
                        "SELECT count(*) FROM live_reconciliation_requirements",
                        "SELECT count(*) FROM live_reconciliation_commits",
                        "SELECT count(*) FROM live_dispatch_claim_resolutions",
                        "SELECT count(*) FROM live_observation_reconciliation_resolutions",
                        "SELECT count(*) FROM live_reconciliation_requirement_resolutions",
                    )
                )
                kinds = {
                    row[0]
                    for row in connection.execute(
                        "SELECT DISTINCT record_kind FROM live_journal_records"
                    )
                }
                expected_kinds = {
                    "identity",
                    "schema-migration",
                    "order",
                    "command",
                    "dispatch-claim",
                    "dispatch-receipt",
                    "raw-observation",
                    "observation-resolution",
                    "normalized-application",
                    "reconciliation",
                    "reconciliation-commit",
                    "dispatch-claim-resolution",
                    "observation-reconciliation-resolution",
                    "reconciliation-requirement-resolution",
                }
                if (
                    count < 1
                    or minimum != 1
                    or sequence != count
                    or sqlite_sequence_row is None
                    or int(sqlite_sequence_row[0]) != sequence
                    or count != expected_count
                    or not kinds.issubset(expected_kinds)
                ):
                    raise LiveJournalIntegrityError("live journal sequence is not contiguous")
                return LiveJournalRecoverySnapshot(
                    self._identity,
                    typed_orders,
                    tuple(claims),
                    unresolved,
                    conflicts,
                    tuple(ambiguous),
                    requirements,
                    self._load_applied_event_ledger(),
                    sequence,
                )
            except sqlite3.Error as exc:
                raise _sqlite_failure(exc) from None


def _load_read_only_inspection_payload(
    connection: sqlite3.Connection,
    account_id: str,
    database_schema_version: int,
) -> tuple[
    LiveJournalIdentity,
    int,
    LiveJournalRecoverySnapshot | None,
    tuple[str, ...],
]:
    """Reuse the journal verifier/hydrator without exposing a journal instance."""

    journal = object.__new__(SqliteLiveOrderJournal)
    journal._path = Path(":memory:")
    journal._lock = RLock()
    journal._closed = False
    journal._poisoned = False
    journal._connection = connection
    journal._validate_schema(database_schema_version)
    identity = journal._load_identity()
    journal._identity = identity
    if database_schema_version == 1:
        journal._verify_durable_payloads()
        sequence_row = connection.execute(
            "SELECT max(journal_sequence) FROM live_journal_records"
        ).fetchone()
        sequence = int(sequence_row[0] or 0)
        return identity, sequence, None, ()
    snapshot = journal.load_recovery_snapshot()
    issue_codes = journal._load_inspection_recovery_issue_codes(account_id)
    return identity, snapshot.journal_sequence, snapshot, issue_codes


__all__ = [
    "APPLICATION_ID",
    "DEFAULT_BUSY_TIMEOUT_MS",
    "DEFAULT_MAX_MAIN_DATABASE_BYTES",
    "SqliteLiveOrderJournal",
]

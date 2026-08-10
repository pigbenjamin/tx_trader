"""One-shot, fail-closed inspection of a SQLite live-order journal."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import os
from pathlib import Path
import re
import sqlite3

from .live_journal_contracts import (
    LiveJournalCapacityError as _LiveJournalCapacityError,
    LiveJournalIntegrityError as _LiveJournalIntegrityError,
)
from .live_journal_inspection import (
    build_live_journal_inspection_report as _build_live_journal_inspection_report,
    build_schema_upgrade_required_inspection_report as _build_schema_upgrade_report,
)
from .live_journal_inspection_contracts import (
    LiveJournalInspectionError as _LiveJournalInspectionError,
    LiveJournalInspectionFailureCode as _FailureCode,
    LiveJournalInspectionIssueCode as _IssueCode,
    LiveJournalInspectionReport as _InspectionReport,
)
from .sqlite_live_order_journal import (
    _ConnectionBoundLiveJournalReader,
    DATABASE_SCHEMA_VERSION as _DATABASE_SCHEMA_VERSION,
    _unsafe_regular_file,
    _verify_reserved_path_identity,
    _verify_trusted_local_parent,
)

MAX_INSPECTION_MAIN_DATABASE_BYTES = 64 * 1024 * 1024
MAX_INSPECTION_SERIALIZED_DATABASE_BYTES = 64 * 1024 * 1024
MAX_INSPECTION_SIDECAR_BYTES = 64 * 1024 * 1024
MAX_INSPECTION_TOTAL_ROWS = 25_000
INSPECTION_PROGRESS_OPCODE_INTERVAL = 1_000
MAX_INSPECTION_PROGRESS_CALLBACKS = 100_000

_SQLITE_CONNECT = sqlite3.connect
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_V1_DURABLE_TABLES = (
    "live_journal_migrations",
    "live_journal_identity",
    "live_journal_records",
    "live_order_id_reservations",
    "live_orders",
    "live_order_history",
    "live_commands",
    "live_dispatch_claims",
    "live_dispatch_receipts",
    "live_raw_observations",
    "live_normalized_events",
    "live_event_applications",
    "live_fills",
    "live_observation_ambiguity",
    "live_reconciliation_requirements",
)
_V2_DURABLE_TABLES = _V1_DURABLE_TABLES + (
    "live_reconciliation_commits",
    "live_dispatch_claim_resolutions",
    "live_observation_reconciliation_resolutions",
    "live_reconciliation_requirement_resolutions",
)


@dataclass(frozen=True, slots=True)
class _FileSnapshot:
    device: int
    inode: int
    size: int
    modified_ns: int
    content_digest: str


def _failure(code: _FailureCode) -> _LiveJournalInspectionError:
    return _LiveJournalInspectionError(code)


def _digest_descriptor(descriptor: int, *, maximum_bytes: int) -> tuple[int, str]:
    digest = sha256()
    total = 0
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise _failure(_FailureCode.CAPACITY_EXCEEDED)
            digest.update(chunk)
    except _LiveJournalInspectionError:
        raise
    except OSError:
        raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
    return total, f"sha256:{digest.hexdigest()}"


def _snapshot_file(
    path: Path,
    *,
    maximum_bytes: int,
    descriptor: int | None = None,
) -> _FileSnapshot | None:
    try:
        value = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError:
        raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
    if _unsafe_regular_file(value):
        raise _failure(_FailureCode.ACTIVE_OR_UNCLEAN_SOURCE)
    if value.st_size > maximum_bytes:
        raise _failure(_FailureCode.CAPACITY_EXCEEDED)
    owned_descriptor: int | None = None
    if descriptor is None:
        try:
            binary = getattr(os, "O_BINARY", 0)
            noinherit = getattr(os, "O_NOINHERIT", 0)
            owned_descriptor = os.open(path, os.O_RDONLY | binary | noinherit)
            descriptor = owned_descriptor
        except OSError:
            raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
    try:
        descriptor_stat = os.fstat(descriptor)
        if _unsafe_regular_file(descriptor_stat) or (
            descriptor_stat.st_dev,
            descriptor_stat.st_ino,
        ) != (value.st_dev, value.st_ino):
            raise _failure(_FailureCode.SOURCE_CHANGED)
        content_size, content_digest = _digest_descriptor(
            descriptor,
            maximum_bytes=maximum_bytes,
        )
        descriptor_after = os.fstat(descriptor)
        if (
            content_size != value.st_size
            or descriptor_after.st_size != value.st_size
            or descriptor_after.st_mtime_ns != value.st_mtime_ns
        ):
            raise _failure(_FailureCode.SOURCE_CHANGED)
    except OSError:
        raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
    finally:
        if owned_descriptor is not None:
            try:
                os.close(owned_descriptor)
            except MemoryError:
                raise
            except Exception:
                pass
    return _FileSnapshot(
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        content_digest,
    )


def _snapshot_source(
    path: Path,
    main_descriptor: int,
) -> tuple[_FileSnapshot | None, ...]:
    return (
        _snapshot_file(
            path,
            maximum_bytes=MAX_INSPECTION_MAIN_DATABASE_BYTES,
            descriptor=main_descriptor,
        ),
        *(
            _snapshot_file(
                Path(f"{path}{suffix}"),
                maximum_bytes=MAX_INSPECTION_SIDECAR_BYTES,
            )
            for suffix in _SIDECAR_SUFFIXES
        ),
    )


def _install_authorizer(connection: sqlite3.Connection) -> None:
    denied_names = (
        "SQLITE_INSERT",
        "SQLITE_UPDATE",
        "SQLITE_DELETE",
        "SQLITE_CREATE_INDEX",
        "SQLITE_CREATE_TABLE",
        "SQLITE_CREATE_TEMP_INDEX",
        "SQLITE_CREATE_TEMP_TABLE",
        "SQLITE_CREATE_TEMP_TRIGGER",
        "SQLITE_CREATE_TEMP_VIEW",
        "SQLITE_CREATE_TRIGGER",
        "SQLITE_CREATE_VIEW",
        "SQLITE_CREATE_VTABLE",
        "SQLITE_DROP_INDEX",
        "SQLITE_DROP_TABLE",
        "SQLITE_DROP_TEMP_INDEX",
        "SQLITE_DROP_TEMP_TABLE",
        "SQLITE_DROP_TEMP_TRIGGER",
        "SQLITE_DROP_TEMP_VIEW",
        "SQLITE_DROP_TRIGGER",
        "SQLITE_DROP_VIEW",
        "SQLITE_DROP_VTABLE",
        "SQLITE_ALTER_TABLE",
        "SQLITE_REINDEX",
        "SQLITE_ANALYZE",
        "SQLITE_ATTACH",
        "SQLITE_DETACH",
        "SQLITE_SAVEPOINT",
    )
    denied = {value for name in denied_names if (value := getattr(sqlite3, name, None)) is not None}
    getter_pragmas = {
        "application_id",
        "foreign_key_check",
        "quick_check",
        "page_count",
        "page_size",
        "user_version",
    }

    def authorize(
        action: int,
        first: str | None,
        second: str | None,
        _database: str | None,
        _trigger: str | None,
    ) -> int:
        if action in denied:
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_TRANSACTION:
            return (
                sqlite3.SQLITE_OK
                if first is not None and first.upper() in {"BEGIN", "ROLLBACK"}
                else sqlite3.SQLITE_DENY
            )
        if action == sqlite3.SQLITE_PRAGMA:
            return (
                sqlite3.SQLITE_OK
                if first is not None and first.lower() in getter_pragmas and second is None
                else sqlite3.SQLITE_DENY
            )
        if (
            action == sqlite3.SQLITE_FUNCTION
            and second is not None
            and second.lower() == "load_extension"
        ):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorize)


def _install_progress_budget(connection: sqlite3.Connection) -> None:
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return int(callbacks >= MAX_INSPECTION_PROGRESS_CALLBACKS)

    connection.set_progress_handler(progress, INSPECTION_PROGRESS_OPCODE_INTERVAL)


def _enforce_total_row_budget(
    connection: sqlite3.Connection,
    database_schema_version: int,
) -> None:
    tables = _V1_DURABLE_TABLES if database_schema_version == 1 else _V2_DURABLE_TABLES
    total = 0
    for table in tables:
        row = connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()
        if row is None or type(row[0]) is not int or row[0] < 0:
            raise _failure(_FailureCode.INTEGRITY_FAILURE)
        total += row[0]
        if total > MAX_INSPECTION_TOTAL_ROWS:
            raise _failure(_FailureCode.CAPACITY_EXCEEDED)


def _sqlite_failure_code(exc: sqlite3.Error) -> _FailureCode:
    code = getattr(exc, "sqlite_errorcode", None)
    if type(code) is int:
        primary = code & 0xFF
        if primary in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            return _FailureCode.ACTIVE_OR_UNCLEAN_SOURCE
        if primary in {sqlite3.SQLITE_FULL, sqlite3.SQLITE_INTERRUPT}:
            return _FailureCode.CAPACITY_EXCEEDED
    return _FailureCode.INTEGRITY_FAILURE


def _bind_connection_to_reserved_source(
    connection: sqlite3.Connection,
    reserved: _FileSnapshot,
) -> bytes:
    try:
        page_count_row = connection.execute("PRAGMA page_count").fetchone()
        page_size_row = connection.execute("PRAGMA page_size").fetchone()
        if page_count_row is None or page_size_row is None:
            raise _failure(_FailureCode.INTEGRITY_FAILURE)
        page_count = page_count_row[0]
        page_size = page_size_row[0]
        if (
            type(page_count) is not int
            or type(page_size) is not int
            or page_count < 1
            or page_size < 1
        ):
            raise _failure(_FailureCode.INTEGRITY_FAILURE)
        serialized_size = page_count * page_size
        if serialized_size > MAX_INSPECTION_SERIALIZED_DATABASE_BYTES:
            raise _failure(_FailureCode.CAPACITY_EXCEEDED)
        if serialized_size != reserved.size:
            raise _failure(_FailureCode.SOURCE_CHANGED)
        serialized = connection.serialize(name="main")
    except _LiveJournalInspectionError:
        raise
    except MemoryError:
        raise _failure(_FailureCode.CAPACITY_EXCEEDED) from None
    except (AttributeError, OverflowError, sqlite3.Error):
        raise _failure(_FailureCode.INTEGRITY_FAILURE) from None
    if len(serialized) > MAX_INSPECTION_SERIALIZED_DATABASE_BYTES:
        raise _failure(_FailureCode.CAPACITY_EXCEEDED)
    if len(serialized) != reserved.size:
        raise _failure(_FailureCode.SOURCE_CHANGED)
    if f"sha256:{sha256(serialized).hexdigest()}" != reserved.content_digest:
        raise _failure(_FailureCode.SOURCE_CHANGED)
    return serialized


def _open_isolated_inspection_connection(serialized: bytes) -> sqlite3.Connection:
    connection: sqlite3.Connection | None = None

    def close_failed_connection() -> bool:
        if connection is None:
            return False
        try:
            connection.close()
        except MemoryError:
            return True
        except Exception:
            pass
        return False

    try:
        if (
            len(serialized) < 100
            or serialized[:16] != b"SQLite format 3\x00"
            or (serialized[18], serialized[19]) not in {(1, 1), (2, 2)}
        ):
            raise _failure(_FailureCode.INTEGRITY_FAILURE)
        isolated_image = bytearray(serialized)
        # A serialized WAL-mode main file retains WAL read/write header bytes,
        # which make SQLite seek disk sidecars even after deserialization.  The
        # already-authenticated private copy uses rollback-format transport
        # bytes so every subsequent read is served solely from memory.
        isolated_image[18] = 1
        isolated_image[19] = 1
        connection = _SQLITE_CONNECT(":memory:", isolation_level=None)
        connection.deserialize(isolated_image, name="main")
        connection.row_factory = sqlite3.Row
        _install_authorizer(connection)
        _install_progress_budget(connection)
        connection.execute("BEGIN")
        return connection
    except MemoryError:
        close_failed_connection()
        raise _failure(_FailureCode.CAPACITY_EXCEEDED) from None
    except sqlite3.Error as exc:
        if close_failed_connection():
            raise _failure(_FailureCode.CAPACITY_EXCEEDED) from None
        raise _failure(_sqlite_failure_code(exc)) from None
    except (AttributeError, OverflowError):
        if close_failed_connection():
            raise _failure(_FailureCode.CAPACITY_EXCEEDED) from None
        raise _failure(_FailureCode.INTEGRITY_FAILURE) from None


def _inspect_sqlite_live_order_journal(
    path: str | Path,
    *,
    account_id: str,
) -> _InspectionReport:
    """Inspect one local journal without exposing a writable journal object."""

    if (
        not isinstance(path, (str, Path))
        or str(path) == ":memory:"
        or "\x00" in str(path)
        or type(account_id) is not str
        or _IDENTIFIER.fullmatch(account_id) is None
    ):
        raise _failure(_FailureCode.INVALID_REQUEST)

    source = Path(path)
    descriptor: int | None = None
    source_connection: sqlite3.Connection | None = None
    inspection_connection: sqlite3.Connection | None = None
    before: tuple[_FileSnapshot | None, ...] | None = None
    primary_error: _LiveJournalInspectionError | None = None
    report: _InspectionReport | None = None
    try:
        try:
            _verify_trusted_local_parent(source)
        except _LiveJournalIntegrityError:
            raise _failure(_FailureCode.INVALID_REQUEST) from None
        try:
            main_stat = os.lstat(source)
        except FileNotFoundError:
            raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
        except OSError:
            raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
        if _unsafe_regular_file(main_stat):
            raise _failure(_FailureCode.INVALID_REQUEST)
        if main_stat.st_size > MAX_INSPECTION_MAIN_DATABASE_BYTES:
            raise _failure(_FailureCode.CAPACITY_EXCEEDED)
        try:
            binary = getattr(os, "O_BINARY", 0)
            noinherit = getattr(os, "O_NOINHERIT", 0)
            descriptor = os.open(source, os.O_RDONLY | binary | noinherit)
        except OSError:
            raise _failure(_FailureCode.SOURCE_UNAVAILABLE) from None
        try:
            _verify_reserved_path_identity(source, descriptor)
            resolved = source.resolve(strict=True)
        except (OSError, _LiveJournalIntegrityError):
            raise _failure(_FailureCode.INVALID_REQUEST) from None

        for suffix in _SIDECAR_SUFFIXES:
            try:
                os.lstat(Path(f"{source}{suffix}"))
            except FileNotFoundError:
                continue
            except OSError:
                raise _failure(_FailureCode.ACTIVE_OR_UNCLEAN_SOURCE) from None
            raise _failure(_FailureCode.ACTIVE_OR_UNCLEAN_SOURCE)

        before = _snapshot_source(source, descriptor)
        if before[0] is None:
            raise _failure(_FailureCode.SOURCE_UNAVAILABLE)
        if any(item is not None for item in before[1:]):
            raise _failure(_FailureCode.ACTIVE_OR_UNCLEAN_SOURCE)

        uri = f"{resolved.as_uri()}?mode=ro&immutable=1&cache=private"
        source_connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        source_connection.row_factory = sqlite3.Row
        _install_authorizer(source_connection)
        if _snapshot_source(source, descriptor) != before:
            raise _failure(_FailureCode.SOURCE_CHANGED)

        source_connection.execute("BEGIN")
        reserved_main = before[0]
        if reserved_main is None:
            raise _failure(_FailureCode.SOURCE_CHANGED)
        serialized = _bind_connection_to_reserved_source(source_connection, reserved_main)
        source_connection.execute("ROLLBACK")
        source_connection.close()
        source_connection = None

        inspection_connection = _open_isolated_inspection_connection(serialized)
        version = int(inspection_connection.execute("PRAGMA user_version").fetchone()[0])
        if version not in {1, _DATABASE_SCHEMA_VERSION}:
            raise _failure(_FailureCode.INTEGRITY_FAILURE)
        reader = _ConnectionBoundLiveJournalReader(inspection_connection)
        reader.validate_schema(version)
        _enforce_total_row_budget(inspection_connection, version)
        payload = reader.load(account_id, version)
        if version == 1:
            report = _build_schema_upgrade_report(
                payload.identity,
                account_id,
                database_schema_version=version,
                journal_sequence=payload.journal_sequence,
            )
        else:
            if payload.snapshot is None:
                raise _failure(_FailureCode.INTEGRITY_FAILURE)
            issue_codes = tuple(_IssueCode(code) for code in payload.issue_codes)
            report = _build_live_journal_inspection_report(
                payload.snapshot,
                account_id,
                database_schema_version=version,
                scoped_issue_codes=issue_codes,
            )
    except _LiveJournalInspectionError as exc:
        primary_error = exc
    except MemoryError:
        primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
    except _LiveJournalCapacityError:
        primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
    except (_LiveJournalIntegrityError, TypeError, ValueError):
        primary_error = _failure(_FailureCode.INTEGRITY_FAILURE)
    except sqlite3.Error as exc:
        primary_error = _failure(_sqlite_failure_code(exc))
    except OSError:
        primary_error = _failure(_FailureCode.SOURCE_UNAVAILABLE)
    finally:
        if inspection_connection is not None:
            try:
                inspection_connection.execute("ROLLBACK")
            except Exception as exc:
                if isinstance(exc, MemoryError):
                    primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
                elif primary_error is None:
                    primary_error = _failure(_FailureCode.INTEGRITY_FAILURE)
            try:
                inspection_connection.close()
            except Exception as exc:
                if isinstance(exc, MemoryError):
                    primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
                elif primary_error is None:
                    primary_error = _failure(_FailureCode.INTEGRITY_FAILURE)
        if source_connection is not None:
            try:
                source_connection.execute("ROLLBACK")
            except Exception as exc:
                if isinstance(exc, MemoryError):
                    primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
                elif primary_error is None:
                    primary_error = _failure(_FailureCode.INTEGRITY_FAILURE)
            try:
                source_connection.close()
            except Exception as exc:
                if isinstance(exc, MemoryError):
                    primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
                elif primary_error is None:
                    primary_error = _failure(_FailureCode.INTEGRITY_FAILURE)
        changed = False
        if before is not None:
            try:
                assert descriptor is not None
                changed = _snapshot_source(source, descriptor) != before
            except _LiveJournalInspectionError:
                changed = True
            except MemoryError:
                primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
            except Exception:
                changed = True
        if descriptor is not None:
            try:
                os.close(descriptor)
            except Exception as exc:
                if isinstance(exc, MemoryError):
                    primary_error = _failure(_FailureCode.CAPACITY_EXCEEDED)
                elif primary_error is None:
                    primary_error = _failure(_FailureCode.INTEGRITY_FAILURE)
        if changed:
            primary_error = _failure(_FailureCode.SOURCE_CHANGED)

    if primary_error is not None:
        raise primary_error
    if report is None:
        raise _failure(_FailureCode.INTEGRITY_FAILURE)
    return report


def inspect_sqlite_live_order_journal(
    path: str | Path,
    *,
    account_id: str,
) -> _InspectionReport:
    try:
        return _inspect_sqlite_live_order_journal(path, account_id=account_id)
    except MemoryError:
        raise _failure(_FailureCode.CAPACITY_EXCEEDED) from None


__all__ = [
    "INSPECTION_PROGRESS_OPCODE_INTERVAL",
    "MAX_INSPECTION_MAIN_DATABASE_BYTES",
    "MAX_INSPECTION_PROGRESS_CALLBACKS",
    "MAX_INSPECTION_SERIALIZED_DATABASE_BYTES",
    "MAX_INSPECTION_SIDECAR_BYTES",
    "MAX_INSPECTION_TOTAL_ROWS",
    "inspect_sqlite_live_order_journal",
]

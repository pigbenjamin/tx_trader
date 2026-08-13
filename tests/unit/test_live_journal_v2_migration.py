from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

import pytest

from tx_trade.orders.live_contracts import (
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_codec import encode_journal_value, journal_digest
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    LiveJournalIdentity,
    LiveJournalIntegrityError,
    OutstandingDispatchClaim,
    intent_fingerprint,
)
from tx_trade.orders.live_ports import DispatchClaimDisposition, RawBrokerObservation
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 8, 3, tzinfo=timezone.utc)
ORDERS_DIR = Path(__file__).parents[2] / "tx_trade" / "orders"
V1_SCHEMA = ORDERS_DIR / "live_journal_schema_v1.sql"
CURRENT_SCHEMA = ORDERS_DIR / "live_journal_schema.sql"
V1_SHA256 = "2e2a378b3babf61c7458f7354e875a733eba803ffc9d7bf460a9644db5c724c1"


def _digest(domain: str, value: object) -> tuple[bytes, str]:
    payload = encode_journal_value(value)
    return payload, journal_digest(domain, payload)


def _scalar_digest(domain: str, values: dict[str, str | int]) -> str:
    payload = json.dumps(
        values,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return f"sha256:{sha256(domain.encode('ascii') + bytes((0,)) + payload).hexdigest()}"


def _submitting() -> tuple[NewOrderCommand, object]:
    intent = LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id="order-1",
        account_id="account-1",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )
    command = NewOrderCommand("command-1", intent, NOW + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = advance_local(
        advance_local(create_live_order(intent), LiveOrderState.VALIDATED, NOW),
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(command, fingerprint),
    )
    return command, order


def _create_frozen_v1(path: Path, *, with_claim: bool = False) -> dict[str, object]:
    assert sha256(V1_SCHEMA.read_bytes()).hexdigest() == V1_SHA256
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(V1_SCHEMA.read_text(encoding="utf-8"))
        fingerprint = f"sha256:{V1_SHA256}"
        identity = LiveJournalIdentity("journal-v1", 1, fingerprint, NOW)
        _, identity_digest = _digest("tx_trade.live.journal.identity.v1", identity)
        connection.execute("INSERT INTO live_journal_migrations VALUES (1, ?)", (fingerprint,))
        connection.execute(
            "INSERT INTO live_journal_identity VALUES (1, ?, 1, ?, ?)",
            (identity.journal_id, fingerprint, NOW.isoformat().replace("+00:00", "Z")),
        )
        connection.execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES ('identity', ?, ?, ?)""",
            (identity.journal_id, identity_digest, NOW.isoformat().replace("+00:00", "Z")),
        )
        seeded: dict[str, object] = {"identity_digest": identity_digest}
        if with_claim:
            command, order = _submitting()
            order_payload, order_digest = _digest("tx_trade.live.journal.order.v1", order)
            command_payload, command_digest = _digest("tx_trade.live.journal.command.v1", command)
            claimed_at = NOW + timedelta(seconds=2)
            claim = OutstandingDispatchClaim(
                command, "claim-token-v1", "dispatcher-v1", order.version, claimed_at
            )
            _, claim_digest = _digest("tx_trade.live.journal.dispatch-claim.v1", claim)
            timestamp = order.updated_at.isoformat().replace("+00:00", "Z")
            connection.execute(
                "INSERT INTO live_order_id_reservations VALUES (?, ?, ?)",
                (order.intent.client_order_id, intent_fingerprint(order.intent), timestamp),
            )
            connection.execute(
                "INSERT INTO live_orders VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    order.intent.client_order_id,
                    order.intent.account_id,
                    order.state.value,
                    order.version,
                    order_payload,
                    order_digest,
                    timestamp,
                ),
            )
            connection.execute(
                "INSERT INTO live_order_history VALUES (NULL, ?, ?, ?, ?, ?)",
                (
                    order.intent.client_order_id,
                    order.version,
                    order_payload,
                    order_digest,
                    timestamp,
                ),
            )
            connection.execute(
                "INSERT INTO live_commands VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    command.client_command_id,
                    order.intent.client_order_id,
                    command.kind.value,
                    payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
                    command_payload,
                    command_digest,
                    command.requested_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.execute(
                "INSERT INTO live_dispatch_claims VALUES (?, ?, ?, ?, ?, ?)",
                (
                    command.client_command_id,
                    claim.claim_token,
                    claim.claimant_id,
                    order.version,
                    order.version,
                    claimed_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            for kind, record_id, digest, recorded_at in (
                ("order", order.intent.client_order_id, order_digest, timestamp),
                (
                    "command",
                    command.client_command_id,
                    command_digest,
                    command.requested_at.isoformat().replace("+00:00", "Z"),
                ),
                (
                    "dispatch-claim",
                    command.client_command_id,
                    claim_digest,
                    claimed_at.isoformat().replace("+00:00", "Z"),
                ),
            ):
                connection.execute(
                    """INSERT INTO live_journal_records(
                           record_kind, record_id, payload_digest, recorded_at
                       ) VALUES (?, ?, ?, ?)""",
                    (kind, record_id, digest, recorded_at),
                )
            seeded.update(
                command=command,
                order=order,
                order_payload=order_payload,
                order_digest=order_digest,
                command_payload=command_payload,
                command_digest=command_digest,
            )
        connection.commit()
        return seeded
    finally:
        connection.close()


def _open(path: Path, mode: JournalOpenMode, journal_id: str | None = None):
    return SqliteLiveOrderJournal(
        path,
        mode,
        clock=lambda: NOW + timedelta(hours=1),
        claim_token_factory=lambda: "unused-token",
        journal_id=journal_id,
    )


def _schema_signature(path: Path) -> tuple[tuple[str, str, str], ...]:
    connection = sqlite3.connect(path)
    try:
        return tuple(
            connection.execute(
                """SELECT type, name, sql FROM sqlite_master
                   WHERE name NOT LIKE 'sqlite_%'
                     AND type IN ('table', 'index', 'view', 'trigger')
                   ORDER BY type, name"""
            )
        )
    finally:
        connection.close()


def _add_ambiguity_rows(
    path: Path,
    *,
    status: str,
    candidates: tuple[str, ...],
) -> None:
    observation = RawBrokerObservation(
        "raw-ambiguous-v1",
        "capital-primary",
        1,
        1,
        NOW + timedelta(minutes=1),
        b"ambiguous",
    )
    payload, digest = _digest("tx_trade.live.journal.raw-observation.v1", observation)
    recorded_at = observation.received_at.isoformat().replace("+00:00", "Z")
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """INSERT INTO live_raw_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                observation.observation_id,
                observation.source,
                observation.broker_session_generation,
                observation.adapter_received_sequence,
                recorded_at,
                payload,
                digest,
                status,
            ),
        )
        connection.execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES ('raw-observation', ?, ?, ?)""",
            (observation.observation_id, digest, recorded_at),
        )
        if status != "unresolved":
            resolution_digest = _scalar_digest(
                "tx_trade.live.journal.observation-resolution.v1",
                {
                    "observation_id": observation.observation_id,
                    "resolution_status": status,
                    "resolved_at": recorded_at,
                },
            )
            connection.execute(
                """INSERT INTO live_journal_records(
                       record_kind, record_id, payload_digest, recorded_at
                   ) VALUES ('observation-resolution', ?, ?, ?)""",
                (observation.observation_id, resolution_digest, recorded_at),
            )
        connection.executemany(
            "INSERT INTO live_observation_ambiguity VALUES (?, ?, 1)",
            ((observation.observation_id, candidate) for candidate in candidates),
        )
        connection.commit()
    finally:
        connection.close()


def test_create_new_is_v3_and_empty_frozen_v1_resumes_via_atomic_migration(
    tmp_path: Path,
) -> None:
    fresh_path = tmp_path / "fresh.sqlite3"
    fresh = _open(fresh_path, JournalOpenMode.CREATE_NEW, "journal-v2")
    assert fresh.identity.schema_version == 3
    fresh.close()

    migrated_path = tmp_path / "empty-v1.sqlite3"
    seeded = _create_frozen_v1(migrated_path)
    migrated = _open(migrated_path, JournalOpenMode.RESUME)
    assert migrated.identity.schema_version == 1
    assert migrated.identity.schema_fingerprint == f"sha256:{V1_SHA256}"
    assert migrated.load_recovery_snapshot().journal_sequence == 3
    migrated.close()

    assert _schema_signature(migrated_path) == _schema_signature(fresh_path)
    connection = sqlite3.connect(migrated_path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
        assert connection.execute(
            """SELECT record_kind, record_id FROM live_journal_records
               ORDER BY journal_sequence"""
        ).fetchall() == [
            ("identity", "journal-v1"),
            ("schema-migration", "2"),
            ("schema-migration", "3"),
        ]
        assert connection.execute(
            """SELECT payload_digest FROM live_journal_records
               WHERE record_kind = 'identity'"""
        ).fetchone() == (seeded["identity_digest"],)
    finally:
        connection.close()


def test_nonempty_v1_preserves_canonical_rows_and_claim_outcome_unknown(
    tmp_path: Path,
) -> None:
    path = tmp_path / "claimed-v1.sqlite3"
    seeded = _create_frozen_v1(path, with_claim=True)
    journal = _open(path, JournalOpenMode.RESUME)
    snapshot = journal.load_recovery_snapshot()
    command = seeded["command"]
    order = seeded["order"]
    assert snapshot.orders == (order,)
    assert len(snapshot.outstanding_claims) == 1
    retry = journal.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=order.version,
        claimant_id="dispatcher-after-restart",
    )
    assert retry.disposition is DispatchClaimDisposition.ALREADY_CLAIMED
    assert journal.load_account_snapshot("account-1").recovery_blockers
    journal.close()

    connection = sqlite3.connect(path)
    try:
        assert connection.execute(
            "SELECT payload, payload_digest FROM live_orders WHERE client_order_id = 'order-1'"
        ).fetchone() == (seeded["order_payload"], seeded["order_digest"])
        assert connection.execute(
            "SELECT payload, payload_digest FROM live_commands WHERE client_command_id = 'command-1'"
        ).fetchone() == (seeded["command_payload"], seeded["command_digest"])
        assert connection.execute("SELECT count(*) FROM live_dispatch_receipts").fetchone() == (0,)
    finally:
        connection.close()


@pytest.mark.parametrize("damage", ("future", "history", "signature", "partial-v2"))
def test_resume_rejects_noncanonical_or_partial_schema(tmp_path: Path, damage: str) -> None:
    path = tmp_path / f"bad-{damage}.sqlite3"
    _create_frozen_v1(path)
    connection = sqlite3.connect(path)
    try:
        if damage == "future":
            connection.execute("PRAGMA user_version = 99")
        elif damage == "history":
            connection.execute(
                "UPDATE live_journal_migrations SET schema_fingerprint = ? WHERE version = 1",
                ("sha256:" + "0" * 64,),
            )
        elif damage == "signature":
            connection.execute("CREATE TABLE injected_schema(value TEXT)")
        else:
            connection.execute(
                "CREATE TABLE live_reconciliation_commits(commit_id TEXT PRIMARY KEY)"
            )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _open(path, JournalOpenMode.RESUME)


def test_failed_migration_transaction_never_leaves_a_mixed_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "rollback-v1.sqlite3"
    _create_frozen_v1(path)
    original_read_text = Path.read_text

    def broken_migration(self: Path, *args: object, **kwargs: object) -> str:
        value = original_read_text(self, *args, **kwargs)
        if self.name == "live_journal_migration_v1_to_v2.sql":
            return value + "\nSELECT * FROM deliberately_missing_table;\n"
        return value

    monkeypatch.setattr(Path, "read_text", broken_migration)
    with pytest.raises(LiveJournalIntegrityError):
        _open(path, JournalOpenMode.RESUME)

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(1,)]
        assert connection.execute(
            """SELECT count(*) FROM sqlite_master
               WHERE type = 'table' AND name IN (
                   'live_reconciliation_commits',
                   'live_dispatch_claim_resolutions',
                   'live_observation_reconciliation_resolutions',
                   'live_reconciliation_requirement_resolutions'
               )"""
        ).fetchone() == (0,)
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("status", "candidates"),
    (
        ("unresolved", ("candidate-1", "candidate-2")),
        ("ambiguous", ("candidate-only",)),
    ),
)
def test_invalid_v1_ambiguity_fails_before_migration(
    tmp_path: Path,
    status: str,
    candidates: tuple[str, ...],
) -> None:
    path = tmp_path / f"invalid-v1-ambiguity-{status}.sqlite3"
    _create_frozen_v1(path)
    _add_ambiguity_rows(path, status=status, candidates=candidates)

    with pytest.raises(LiveJournalIntegrityError):
        _open(path, JournalOpenMode.RESUME)

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(1,)]
    finally:
        connection.close()


def test_invalid_v1_ambiguity_candidate_identifiers_fail_before_migration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalid-v1-ambiguity-identifiers.sqlite3"
    _create_frozen_v1(path)
    _add_ambiguity_rows(
        path,
        status="ambiguous",
        candidates=("invalid candidate one", "invalid candidate two"),
    )

    with pytest.raises(LiveJournalIntegrityError) as raised:
        _open(path, JournalOpenMode.RESUME)
    assert type(raised.value) is LiveJournalIntegrityError

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(1,)]
    finally:
        connection.close()


@pytest.mark.parametrize("candidate_count", (0, 1))
def test_invalid_v2_ambiguity_candidate_count_fails_closed(
    tmp_path: Path,
    candidate_count: int,
) -> None:
    path = tmp_path / f"invalid-v2-ambiguity-{candidate_count}.sqlite3"
    journal = _open(path, JournalOpenMode.CREATE_NEW, "journal-v2-ambiguity")
    observation = RawBrokerObservation(
        "raw-ambiguous-v2",
        "capital-primary",
        1,
        1,
        NOW + timedelta(minutes=1),
        b"ambiguous",
    )
    journal.append_raw_observation(observation)
    journal.close()

    recorded_at = observation.received_at.isoformat().replace("+00:00", "Z")
    resolution_digest = _scalar_digest(
        "tx_trade.live.journal.observation-resolution.v1",
        {
            "observation_id": observation.observation_id,
            "resolution_status": "ambiguous",
            "resolved_at": recorded_at,
        },
    )
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("UPDATE live_raw_observations SET resolution_status = 'ambiguous'")
        connection.execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES ('observation-resolution', ?, ?, ?)""",
            (observation.observation_id, resolution_digest, recorded_at),
        )
        if candidate_count:
            connection.execute(
                "INSERT INTO live_observation_ambiguity VALUES (?, ?, 1)",
                (observation.observation_id, "candidate-only"),
            )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LiveJournalIntegrityError):
        _open(path, JournalOpenMode.RESUME)


def test_invalid_v2_ambiguity_candidate_identifiers_are_sanitized(
    tmp_path: Path,
) -> None:
    path = tmp_path / "invalid-v2-ambiguity-identifiers.sqlite3"
    journal = _open(path, JournalOpenMode.CREATE_NEW, "journal-v2-invalid-candidates")
    observation = RawBrokerObservation(
        "raw-invalid-candidates-v2",
        "capital-primary",
        1,
        1,
        NOW + timedelta(minutes=1),
        b"ambiguous",
    )
    journal.append_raw_observation(observation)
    journal.close()

    recorded_at = observation.received_at.isoformat().replace("+00:00", "Z")
    resolution_digest = _scalar_digest(
        "tx_trade.live.journal.observation-resolution.v1",
        {
            "observation_id": observation.observation_id,
            "resolution_status": "ambiguous",
            "resolved_at": recorded_at,
        },
    )
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("UPDATE live_raw_observations SET resolution_status = 'ambiguous'")
        connection.execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES ('observation-resolution', ?, ?, ?)""",
            (observation.observation_id, resolution_digest, recorded_at),
        )
        connection.executemany(
            "INSERT INTO live_observation_ambiguity VALUES (?, ?, 1)",
            (
                (observation.observation_id, "invalid candidate one"),
                (observation.observation_id, "invalid candidate two"),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LiveJournalIntegrityError) as raised:
        _open(path, JournalOpenMode.RESUME)
    assert type(raised.value) is LiveJournalIntegrityError

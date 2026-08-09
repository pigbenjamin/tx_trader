"""Fixtures for black-box live-journal inspection tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import sqlite3

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
    OutstandingDispatchClaim,
    intent_fingerprint,
)
from tx_trade.orders.live_ports import RawBrokerObservation
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)
ORDERS_DIR = Path(__file__).parents[2] / "tx_trade" / "orders"
V1_SCHEMA = ORDERS_DIR / "live_journal_schema_v1.sql"


def submitting_order(
    *,
    account_id: str = "account-a",
    order_id: str = "order-a",
    command_id: str = "command-a",
) -> tuple[NewOrderCommand, object]:
    intent = LiveOrderIntent(
        strategy_id="strategy-test",
        client_order_id=order_id,
        account_id=account_id,
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )
    command = NewOrderCommand(command_id, intent, NOW + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, NOW)
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(command, fingerprint),
    )
    return command, order


def create_v2(path: Path, *, orders: tuple[tuple[str, str, str], ...] = ()) -> None:
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        clock=lambda: NOW + timedelta(minutes=1),
        claim_token_factory=lambda: "claim-token-secret",
        journal_id="journal-inspection-test",
    )
    try:
        for account_id, order_id, command_id in orders:
            command, order = submitting_order(
                account_id=account_id,
                order_id=order_id,
                command_id=command_id,
            )
            journal.register_new_order(
                command,
                order,
                intent_fingerprint=intent_fingerprint(command.intent),
            )
    finally:
        journal.close()


def create_v2_with_claim(
    path: Path,
    *,
    account_id: str = "account-a",
    order_id: str = "order-a",
    command_id: str = "command-a",
    claim_token: str = "claim-token-secret",
    claimant_id: str = "claimant-secret",
) -> None:
    command, order = submitting_order(
        account_id=account_id,
        order_id=order_id,
        command_id=command_id,
    )
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        clock=lambda: NOW + timedelta(minutes=1),
        claim_token_factory=lambda: claim_token,
        journal_id="journal-inspection-test",
    )
    try:
        journal.register_new_order(
            command,
            order,
            intent_fingerprint=intent_fingerprint(command.intent),
        )
        journal.claim_dispatch(
            command.client_command_id,
            payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
            expected_order_version=order.version,
            claimant_id=claimant_id,
        )
    finally:
        journal.close()


def create_multi_account_foreign_secrets(
    path: Path,
    *,
    foreign_claim_token: str,
    foreign_raw_payload: bytes,
) -> None:
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        clock=lambda: NOW + timedelta(minutes=1),
        claim_token_factory=lambda: foreign_claim_token,
        journal_id="journal-inspection-test",
    )
    try:
        for account_id, order_id, command_id in (
            ("account-a", "order-a", "command-a"),
            ("account-b", "order-b", "command-b"),
        ):
            command, order = submitting_order(
                account_id=account_id,
                order_id=order_id,
                command_id=command_id,
            )
            journal.register_new_order(
                command,
                order,
                intent_fingerprint=intent_fingerprint(command.intent),
            )
            if account_id == "account-b":
                journal.claim_dispatch(
                    command.client_command_id,
                    payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
                    expected_order_version=order.version,
                    claimant_id="foreign-claimant-secret",
                )
        journal.append_raw_observation(
            RawBrokerObservation(
                "foreign-observation",
                "capital-primary",
                1,
                1,
                NOW + timedelta(minutes=2),
                foreign_raw_payload,
            )
        )
    finally:
        journal.close()


def create_semantically_blocked_v2(path: Path) -> None:
    command, order = submitting_order()
    create_v2_with_claim(path)
    invalid_expected_version = order.version + 1
    claim = OutstandingDispatchClaim(
        command,
        "claim-token-secret",
        "claimant-secret",
        invalid_expected_version,
        NOW + timedelta(minutes=1),
    )
    claim_digest = journal_digest(
        "tx_trade.live.journal.dispatch-claim.v1",
        encode_journal_value(claim),
    )
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """UPDATE live_dispatch_claims
               SET expected_order_version = ?, claim_version = ?
               WHERE client_command_id = ?""",
            (invalid_expected_version, invalid_expected_version, command.client_command_id),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'dispatch-claim' AND record_id = ?""",
            (claim_digest, command.client_command_id),
        )
        connection.commit()
    finally:
        connection.close()


def create_frozen_v1(path: Path, *, with_claim: bool = False) -> None:
    schema_bytes = V1_SCHEMA.read_bytes()
    fingerprint = f"sha256:{sha256(schema_bytes).hexdigest()}"
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(schema_bytes.decode("utf-8"))
        identity = LiveJournalIdentity("journal-v1", 1, fingerprint, NOW)
        identity_payload = encode_journal_value(identity)
        identity_digest = journal_digest("tx_trade.live.journal.identity.v1", identity_payload)
        timestamp = NOW.isoformat().replace("+00:00", "Z")
        connection.execute("INSERT INTO live_journal_migrations VALUES (1, ?)", (fingerprint,))
        connection.execute(
            "INSERT INTO live_journal_identity VALUES (1, ?, 1, ?, ?)",
            (identity.journal_id, fingerprint, timestamp),
        )
        connection.execute(
            "INSERT INTO live_journal_records VALUES (NULL, 'identity', ?, ?, ?)",
            (identity.journal_id, identity_digest, timestamp),
        )
        if with_claim:
            command, order = submitting_order()
            order_timestamp = order.updated_at.isoformat().replace("+00:00", "Z")
            order_payload = encode_journal_value(order)
            order_digest = journal_digest("tx_trade.live.journal.order.v1", order_payload)
            command_payload = encode_journal_value(command)
            command_digest = journal_digest("tx_trade.live.journal.command.v1", command_payload)
            claim = OutstandingDispatchClaim(
                command,
                "claim-token-v1-secret",
                "claimant-v1-secret",
                order.version,
                NOW + timedelta(seconds=2),
            )
            claim_digest = journal_digest(
                "tx_trade.live.journal.dispatch-claim.v1",
                encode_journal_value(claim),
            )
            connection.execute(
                "INSERT INTO live_order_id_reservations VALUES (?, ?, ?)",
                (
                    order.intent.client_order_id,
                    intent_fingerprint(order.intent),
                    order_timestamp,
                ),
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
                    order_timestamp,
                ),
            )
            connection.execute(
                "INSERT INTO live_order_history VALUES (NULL, ?, ?, ?, ?, ?)",
                (
                    order.intent.client_order_id,
                    order.version,
                    order_payload,
                    order_digest,
                    order_timestamp,
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
                    claim.claimed_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            for kind, record_id, digest, recorded_at in (
                ("order", order.intent.client_order_id, order_digest, order_timestamp),
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
                    claim.claimed_at.isoformat().replace("+00:00", "Z"),
                ),
            ):
                connection.execute(
                    "INSERT INTO live_journal_records VALUES (NULL, ?, ?, ?, ?)",
                    (kind, record_id, digest, recorded_at),
                )
        connection.commit()
    finally:
        connection.close()


def database_rows(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        return {
            str(name): tuple(connection.execute(f'SELECT * FROM "{name}" ORDER BY rowid'))
            for (name,) in tables
        }
    finally:
        connection.close()


def schema_signature(path: Path) -> tuple[tuple[object, ...], ...]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        return tuple(
            connection.execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
        )
    finally:
        connection.close()

"""Fixtures for black-box live-journal inspection tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
import sqlite3

from tx_trade.orders.live_contracts import (
    FingerprintDomain,
    BrokerCorrelation,
    BrokerOpenOrderObservation,
    BrokerOrderEventType,
    CorrelationStatus,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_codec import encode_journal_value, journal_digest
from tx_trade.orders.live_journal_contracts import (
    DurableReconciliationRequirement,
    JournalOpenMode,
    LiveJournalIdentity,
    OutstandingDispatchClaim,
    intent_fingerprint,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    RawBrokerObservation,
    ReconciliationResult,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
    ExpectedOrderVersion,
    ObservationResolution,
    ObservationResolutionDirective,
    ObservationStatus,
    RequirementResolution,
    RequirementResolutionDirective,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    ReconciliationAssessment,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order
import tx_trade.orders.sqlite_live_order_journal as journal_module
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)
ORDERS_DIR = Path(__file__).parents[2] / "tx_trade" / "orders"
V1_SCHEMA = ORDERS_DIR / "live_journal_schema_v1.sql"


class AttributionBlocker(StrEnum):
    UNRESOLVED = "unresolved"
    CONFLICT = "conflict"
    AMBIGUOUS = "ambiguous"
    REQUIREMENT = "requirement"


class AttributionState(StrEnum):
    SELECTED = "selected"
    FOREIGN = "foreign"
    GLOBAL = "global"
    RESOLVED = "resolved"


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


def _accepted_order(
    journal: SqliteLiveOrderJournal,
    *,
    account_id: str,
    order_id: str,
    command_id: str,
    sequence: int,
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
    observed_at = NOW + timedelta(seconds=sequence + 10)
    raw = RawBrokerObservation(
        f"accepted-raw-{sequence}",
        "attribution-fixture",
        1,
        sequence,
        observed_at,
        b"accepted-baseline",
    )
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        event_id=f"accepted-event-{sequence}",
        account_id=account_id,
        instrument_id=command.intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=observed_at,
        broker_session_generation=1,
        adapter_received_sequence=sequence,
        correlation=BrokerCorrelation(
            1,
            sequence,
            CorrelationStatus.CONFIRMED,
            observed_at,
            broker_order_sequence=f"accepted-broker-{sequence}",
            client_order_id=order_id,
        ),
    )
    applied = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,
    )
    assert applied.order is not None
    assert applied.order.state is LiveOrderState.ACCEPTED
    return applied.order


def _append_fixture_record(
    connection: sqlite3.Connection,
    kind: str,
    record_id: str,
    digest: str,
    recorded_at: datetime,
) -> None:
    connection.execute(
        """INSERT INTO live_journal_records(
               record_kind, record_id, payload_digest, recorded_at
           ) VALUES (?, ?, ?, ?)""",
        (
            kind,
            record_id,
            digest,
            recorded_at.isoformat().replace("+00:00", "Z"),
        ),
    )


def _build_verifier_valid_ambiguity(
    path: Path,
    *,
    observation: RawBrokerObservation,
    event: NormalizedBrokerOrderEvent,
    candidate_order_ids: tuple[str, str],
) -> None:
    """Build the schema-only ambiguity shape for which no public writer exists."""

    event_payload, event_digest = journal_module._encode(
        event,
        journal_module._EVENT_DOMAIN,
    )
    applied_at = event.received_at.isoformat().replace("+00:00", "Z")
    application_digest = journal_module._application_digest(
        event_payload_digest=event_digest,
        raw_observation_id=observation.observation_id,
        client_order_id=None,
        disposition="unresolved",
        failure_code=None,
        applied_at=applied_at,
    )
    resolution_digest = journal_module._resolution_digest(
        observation.observation_id,
        "ambiguous",
        applied_at,
    )
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE live_raw_observations SET resolution_status = 'ambiguous' "
            "WHERE observation_id = ? AND resolution_status = 'unresolved'",
            (observation.observation_id,),
        )
        connection.execute(
            """INSERT INTO live_normalized_events(
                   source, event_id, raw_observation_id, semantic_fingerprint,
                   payload, payload_digest, received_at
               ) VALUES ('broker-event', ?, ?, ?, ?, ?, ?)""",
            (
                event.event_id,
                observation.observation_id,
                journal_module.broker_semantic_fingerprint(event),
                event_payload,
                event_digest,
                applied_at,
            ),
        )
        connection.execute(
            """INSERT INTO live_event_applications(
                   source, event_id, client_order_id, disposition,
                   failure_code, applied_at
               ) VALUES ('broker-event', ?, NULL, 'unresolved', NULL, ?)""",
            (event.event_id, applied_at),
        )
        connection.executemany(
            """INSERT INTO live_observation_ambiguity(
                   observation_id, candidate_client_order_id, resolution_version
               ) VALUES (?, ?, 1)""",
            (
                (observation.observation_id, candidate_order_ids[0]),
                (observation.observation_id, candidate_order_ids[1]),
            ),
        )
        _append_fixture_record(
            connection,
            "normalized-application",
            event.event_id,
            application_digest,
            event.received_at,
        )
        _append_fixture_record(
            connection,
            "observation-resolution",
            observation.observation_id,
            resolution_digest,
            observation.received_at,
        )
        connection.commit()
    finally:
        connection.close()


def _build_verifier_valid_requirement(
    path: Path,
    *,
    client_order_id: str | None,
    observation_id: str | None = None,
) -> int:
    """Build a standalone durable requirement, which has no public creation API."""

    created_at = NOW + timedelta(minutes=2)
    reason = "attribution_fixture_requirement"
    connection = sqlite3.connect(path)
    try:
        cursor = connection.execute(
            """INSERT INTO live_reconciliation_requirements(
                   client_order_id, observation_id, reason_code, created_at
               ) VALUES (?, ?, ?, ?)""",
            (
                client_order_id,
                observation_id,
                reason,
                created_at.isoformat().replace("+00:00", "Z"),
            ),
        )
        requirement_id = cursor.lastrowid
        assert type(requirement_id) is int
        requirement = DurableReconciliationRequirement(
            requirement_id,
            reason,
            created_at,
            client_order_id,
            observation_id,
        )
        _, digest = journal_module._encode(
            requirement,
            journal_module._RECONCILIATION_DOMAIN,
        )
        _append_fixture_record(
            connection,
            "reconciliation",
            str(requirement_id),
            digest,
            created_at,
        )
        connection.commit()
        return requirement_id
    finally:
        connection.close()


def _authoritative_assessment(
    journal: SqliteLiveOrderJournal,
    account_id: str,
) -> ReconciliationAssessment:
    local = journal.load_account_snapshot(account_id)
    captured_at = local.as_of
    evidence = tuple(
        CompletenessEvidence(
            kind,
            account_id,
            EvidenceCompleteness.COMPLETE,
            captured_at,
            f"snapshot-{account_id}",
        )
        for kind in (
            EvidenceQueryKind.OPEN_ORDERS,
            EvidenceQueryKind.FILLS,
            EvidenceQueryKind.POSITIONS,
        )
    )
    open_orders = tuple(
        BrokerOpenOrderObservation(
            f"broker-view-{order.intent.client_order_id}",
            account_id,
            order.intent.instrument_id,
            order.intent.side,
            order.total_quantity,
            order.remaining_quantity,
            order.intent.limit_price,
            BrokerCorrelation(
                2,
                index,
                CorrelationStatus.CONFIRMED,
                captured_at,
                broker_order_sequence=f"broker-view-{index}",
                client_order_id=order.intent.client_order_id,
            ),
            captured_at,
        )
        for index, order in enumerate(local.orders, 1)
    )
    broker = BrokerReconciliationSnapshot(
        f"snapshot-{account_id}",
        account_id,
        OpenOrdersSnapshot(open_orders, evidence[0]),
        BrokerFillsSnapshot((), evidence[1]),
        BrokerPositionsSnapshot((), evidence[2]),
        captured_at,
    )
    result = ReconciliationResult(
        account_id,
        ReconciliationStatus.COMPLETE,
        (),
        evidence,
        captured_at,
    )
    return ReconciliationAssessment(local, broker, result)


def _commit_attribution_resolution(
    journal: SqliteLiveOrderJournal,
    *,
    account_id: str,
    blocker: AttributionBlocker,
    observation_id: str | None,
    event_id: str | None,
    requirement_ids: tuple[int, ...],
) -> None:
    assessment = _authoritative_assessment(journal, account_id)
    status_by_blocker = {
        AttributionBlocker.CONFLICT: ObservationStatus.CONFLICT,
        AttributionBlocker.AMBIGUOUS: ObservationStatus.AMBIGUOUS,
    }
    observations = (
        (
            ObservationResolutionDirective(
                observation_id,
                status_by_blocker[blocker],
                event_id,
                ObservationResolution.BROKER_ORDER_CONFIRMED,
            ),
        )
        if observation_id is not None and event_id is not None
        else ()
    )
    request = DurableReconciliationCommitRequest(
        f"resolve-{blocker.value}",
        account_id,
        assessment,
        assessment.local_snapshot.journal_sequence,
        tuple(
            ExpectedOrderVersion(order.intent.client_order_id, order.version)
            for order in assessment.local_snapshot.orders
        ),
        observation_resolutions=observations,
        requirement_resolutions=tuple(
            RequirementResolutionDirective(item, RequirementResolution.SATISFIED)
            for item in requirement_ids
        ),
    )
    assert journal.commit_reconciliation(request).disposition.value == "committed"


def create_attribution_scenario(
    path: Path,
    *,
    blocker: AttributionBlocker,
    state: AttributionState,
) -> tuple[str, str]:
    """Create one validated attribution fixture and return only its account IDs."""

    if type(blocker) is not AttributionBlocker:
        raise TypeError("blocker must be AttributionBlocker")
    if type(state) is not AttributionState:
        raise TypeError("state must be AttributionState")
    selected_account = "selected-account"
    foreign_account = "foreign-account-secret"
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.CREATE_NEW,
        clock=lambda: NOW + timedelta(hours=2),
        claim_token_factory=lambda: "unused-attribution-token",
        journal_id="journal-attribution-matrix",
    )
    selected_orders = (
        _accepted_order(
            journal,
            account_id=selected_account,
            order_id="selected-order-a",
            command_id="selected-command-a",
            sequence=1,
        ),
        _accepted_order(
            journal,
            account_id=selected_account,
            order_id="selected-order-b",
            command_id="selected-command-b",
            sequence=2,
        ),
    )
    foreign_orders = (
        _accepted_order(
            journal,
            account_id=foreign_account,
            order_id="foreign-order-secret-a",
            command_id="foreign-command-secret-a",
            sequence=3,
        ),
        _accepted_order(
            journal,
            account_id=foreign_account,
            order_id="foreign-order-secret-b",
            command_id="foreign-command-secret-b",
            sequence=4,
        ),
    )
    owner_account = foreign_account if state is AttributionState.FOREIGN else selected_account
    owner_orders = foreign_orders if state is AttributionState.FOREIGN else selected_orders
    observed_at = NOW + timedelta(minutes=1)
    observation = RawBrokerObservation(
        f"{blocker.value}-observation-secret",
        "attribution-fixture",
        3,
        1,
        observed_at,
        f"{blocker.value}-payload-secret".encode("ascii"),
    )
    event = NormalizedBrokerOrderEvent(
        event_id=f"{blocker.value}-event-secret",
        account_id=owner_account,
        instrument_id=owner_orders[0].intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=observed_at,
        broker_session_generation=3,
        adapter_received_sequence=1,
        correlation=BrokerCorrelation(
            3,
            1,
            CorrelationStatus.CONFIRMED,
            observed_at,
            broker_order_sequence=f"{blocker.value}-broker-secret",
            client_order_id=owner_orders[0].intent.client_order_id,
        ),
    )
    requirement_ids: tuple[int, ...] = ()
    if blocker is not AttributionBlocker.REQUIREMENT or state is AttributionState.GLOBAL:
        journal.append_raw_observation(observation)

    if blocker is AttributionBlocker.UNRESOLVED:
        unresolved_event = NormalizedBrokerOrderEvent(
            event_id=event.event_id,
            account_id=owner_account,
            instrument_id=event.instrument_id,
            event_type=event.event_type,
            received_at=event.received_at,
            broker_session_generation=event.broker_session_generation,
            adapter_received_sequence=event.adapter_received_sequence,
            correlation=BrokerCorrelation(
                3,
                1,
                CorrelationStatus.CANDIDATE,
                observed_at,
                broker_order_sequence="unresolved-broker-secret",
            ),
        )
        if state is AttributionState.RESOLVED:
            result = journal.apply_normalized_event(
                event,
                raw_observation_id=observation.observation_id,
                expected_order_version=owner_orders[0].version,
            )
            assert result.order is not None
        elif state is not AttributionState.GLOBAL:
            journal.apply_normalized_event(
                unresolved_event,
                raw_observation_id=observation.observation_id,
                expected_order_version=None,
            )
    elif blocker is AttributionBlocker.CONFLICT and state is not AttributionState.GLOBAL:
        conflict_event = NormalizedBrokerOrderEvent(
            event_id=event.event_id,
            account_id=event.account_id,
            instrument_id="different-instrument",
            event_type=event.event_type,
            received_at=event.received_at,
            broker_session_generation=event.broker_session_generation,
            adapter_received_sequence=event.adapter_received_sequence,
            correlation=event.correlation,
        )
        journal.apply_normalized_event(
            conflict_event,
            raw_observation_id=observation.observation_id,
            expected_order_version=owner_orders[0].version,
        )
        requirement_ids = tuple(
            item.requirement_id
            for item in journal.load_recovery_snapshot().reconciliation_requirements
            if item.observation_id == observation.observation_id
        )
        if state is AttributionState.RESOLVED:
            _commit_attribution_resolution(
                journal,
                account_id=selected_account,
                blocker=blocker,
                observation_id=observation.observation_id,
                event_id=event.event_id,
                requirement_ids=requirement_ids,
            )
    elif blocker is AttributionBlocker.REQUIREMENT and state is AttributionState.GLOBAL:
        foreign_requirement_event = NormalizedBrokerOrderEvent(
            event_id=event.event_id,
            account_id=foreign_account,
            instrument_id=foreign_orders[0].intent.instrument_id,
            event_type=event.event_type,
            received_at=event.received_at,
            broker_session_generation=event.broker_session_generation,
            adapter_received_sequence=event.adapter_received_sequence,
            correlation=BrokerCorrelation(
                3,
                1,
                CorrelationStatus.CANDIDATE,
                observed_at,
                broker_order_sequence="requirement-foreign-broker-secret",
            ),
        )
        journal.apply_normalized_event(
            foreign_requirement_event,
            raw_observation_id=observation.observation_id,
            expected_order_version=None,
        )
    journal.close()

    if blocker is AttributionBlocker.AMBIGUOUS:
        candidates = (
            (selected_orders[0].intent.client_order_id, foreign_orders[0].intent.client_order_id)
            if state is AttributionState.GLOBAL
            else tuple(order.intent.client_order_id for order in owner_orders)
        )
        _build_verifier_valid_ambiguity(
            path,
            observation=observation,
            event=event,
            candidate_order_ids=candidates,
        )
    elif blocker is AttributionBlocker.REQUIREMENT:
        requirement_id = _build_verifier_valid_requirement(
            path,
            client_order_id=(
                selected_orders[0].intent.client_order_id
                if state is AttributionState.GLOBAL
                else owner_orders[0].intent.client_order_id
            ),
            observation_id=(
                observation.observation_id if state is AttributionState.GLOBAL else None
            ),
        )
        requirement_ids = (requirement_id,)
    elif blocker is AttributionBlocker.CONFLICT and state is AttributionState.GLOBAL:
        connection = sqlite3.connect(path)
        try:
            resolved_at = observation.received_at.isoformat().replace("+00:00", "Z")
            connection.execute(
                "UPDATE live_raw_observations SET resolution_status = 'conflict' "
                "WHERE observation_id = ?",
                (observation.observation_id,),
            )
            _append_fixture_record(
                connection,
                "observation-resolution",
                observation.observation_id,
                journal_module._resolution_digest(
                    observation.observation_id, "conflict", resolved_at
                ),
                observation.received_at,
            )
            connection.commit()
        finally:
            connection.close()
        _build_verifier_valid_requirement(
            path,
            client_order_id=None,
            observation_id=observation.observation_id,
        )

    if state is AttributionState.RESOLVED and blocker in {
        AttributionBlocker.AMBIGUOUS,
        AttributionBlocker.REQUIREMENT,
    }:
        resumed = SqliteLiveOrderJournal(
            path,
            JournalOpenMode.RESUME,
            clock=lambda: NOW + timedelta(hours=2),
            claim_token_factory=lambda: "unused-attribution-token",
        )
        try:
            resumed.load_recovery_snapshot()
            _commit_attribution_resolution(
                resumed,
                account_id=selected_account,
                blocker=blocker,
                observation_id=(
                    observation.observation_id if blocker is AttributionBlocker.AMBIGUOUS else None
                ),
                event_id=event.event_id if blocker is AttributionBlocker.AMBIGUOUS else None,
                requirement_ids=requirement_ids,
            )
        finally:
            resumed.close()

    validation = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: NOW + timedelta(hours=3),
        claim_token_factory=lambda: "unused-validation-token",
    )
    try:
        validation.load_recovery_snapshot()
    finally:
        validation.close()
    return selected_account, foreign_account


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

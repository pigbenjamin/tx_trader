from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tx_trade.orders.live_contracts import (
    AccountSnapshot,
    AmendOrderCommand,
    BrokerCorrelation,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
    CancelOrderCommand,
    CorrelationStatus,
    DecreaseOrderCommand,
    DispatchReceipt,
    LiveCommand,
    LiveFailure,
    LiveFailureCode,
    LiveOrder,
    LiveSide,
    NewOrderCommand,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    ReadinessSnapshot,
)
from tx_trade.orders.live_ports import (
    AccountCatalogPort,
    AmbiguousObservation,
    BrokerFillsSnapshot,
    BrokerOrderQueryPort,
    BrokerPositionsSnapshot,
    BrokerReplyBatch,
    BrokerReplySourcePort,
    ClientOrderIdReservation,
    CompletenessEvidence,
    DispatchClaim,
    DispatchClaimDisposition,
    EvidenceCompleteness,
    EvidenceQueryKind,
    EventApplicationDisposition,
    EventApplicationResult,
    LiveOrderDispatchPort,
    LiveOrderEventSink,
    OpenOrdersSnapshot,
    OrderJournalPort,
    OrderServicePort,
    RawBrokerObservation,
    ReconciliationPort,
    ReconciliationResult,
    ReconciliationStatus,
    ReservationDisposition,
)

NOW = datetime(2026, 7, 29, tzinfo=timezone.utc)
FINGERPRINT = f"sha256:{'0' * 64}"


def correlation(**changes: object) -> BrokerCorrelation:
    values: dict[str, object] = {
        "broker_session_generation": 1,
        "adapter_received_sequence": 1,
        "status": CorrelationStatus.CANDIDATE,
        "correlated_at": NOW,
        "broker_order_sequence": "broker-order-1",
    }
    values.update(changes)
    return BrokerCorrelation(**values)  # type: ignore[arg-type]


def evidence(
    query_kind: EvidenceQueryKind,
    *,
    account_id: str = "account-1",
    status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    source_cursor: str = "snapshot-1",
    reason: str | None = None,
) -> CompletenessEvidence:
    if status is not EvidenceCompleteness.COMPLETE and reason is None:
        reason = "broker result was not exhaustive"
    return CompletenessEvidence(query_kind, account_id, status, NOW, source_cursor, reason)


def open_order() -> BrokerOpenOrderObservation:
    return BrokerOpenOrderObservation(
        "open-observation-1",
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("2"),
        Decimal("1"),
        Decimal("22000"),
        correlation(),
        NOW,
    )


def broker_fill() -> BrokerFillObservation:
    return BrokerFillObservation(
        "fill-observation-1",
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22000"),
        correlation(broker_fill_id="broker-fill-1"),
        NOW,
        NOW,
    )


class FakeAccountCatalog:
    def list_accounts(self) -> tuple[AccountSnapshot, ...]:
        return ()

    def get_readiness(self, account_id: str) -> ReadinessSnapshot:
        raise NotImplementedError


class FakeDispatch:
    def dispatch(self, command: LiveCommand) -> DispatchReceipt:
        raise NotImplementedError


class FakeReplySource:
    def read_replies(
        self, *, after_cursor: str | None = None, limit: int = 100
    ) -> BrokerReplyBatch:
        raise NotImplementedError


class FakeQuery:
    def query_open_orders(self, account_id: str) -> OpenOrdersSnapshot:
        raise NotImplementedError

    def query_fills(self, account_id: str) -> BrokerFillsSnapshot:
        raise NotImplementedError

    def query_positions(self, account_id: str) -> BrokerPositionsSnapshot:
        raise NotImplementedError


class FakeJournal:
    def reserve_client_order_id(
        self, client_order_id: str, payload_fingerprint: str
    ) -> ClientOrderIdReservation:
        raise NotImplementedError

    def claim_dispatch(
        self,
        client_command_id: str,
        payload_fingerprint: str,
        *,
        expected_version: int,
        claimant_id: str,
    ) -> DispatchClaim:
        raise NotImplementedError

    def record_dispatch_receipt(
        self,
        receipt: DispatchReceipt,
        *,
        claim_token: str,
        expected_version: int,
    ) -> bool:
        raise NotImplementedError

    def append_raw_observation(self, observation: RawBrokerObservation) -> object:
        raise NotImplementedError

    def apply_normalized_event(
        self,
        event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
        *,
        expected_order_version: int | None,
    ) -> EventApplicationResult:
        raise NotImplementedError

    def load_unresolved_observations(self) -> tuple[RawBrokerObservation, ...]:
        return ()

    def load_ambiguous_observations(self) -> tuple[AmbiguousObservation, ...]:
        return ()


class FakeService:
    def submit(self, command: NewOrderCommand) -> LiveOrder | LiveFailure:
        raise NotImplementedError

    def cancel(self, command: CancelOrderCommand) -> LiveOrder | LiveFailure:
        raise NotImplementedError

    def amend(self, command: AmendOrderCommand) -> LiveOrder | LiveFailure:
        raise NotImplementedError

    def decrease(self, command: DecreaseOrderCommand) -> LiveOrder | LiveFailure:
        raise NotImplementedError

    def get_order(self, client_order_id: str) -> LiveOrder | None:
        return None


class FakeReconciliation:
    def reconcile(self, account_id: str) -> ReconciliationResult:
        raise NotImplementedError


class FakeSink:
    def publish(self, event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent) -> None:
        return None


def test_fake_implementations_structurally_satisfy_each_distinct_port() -> None:
    assert isinstance(FakeAccountCatalog(), AccountCatalogPort)
    assert isinstance(FakeDispatch(), LiveOrderDispatchPort)
    assert isinstance(FakeReplySource(), BrokerReplySourcePort)
    assert isinstance(FakeQuery(), BrokerOrderQueryPort)
    assert isinstance(FakeJournal(), OrderJournalPort)
    assert isinstance(FakeService(), OrderServicePort)
    assert isinstance(FakeReconciliation(), ReconciliationPort)
    assert isinstance(FakeSink(), LiveOrderEventSink)


def test_missing_or_malformed_implementations_fail_runtime_checks() -> None:
    class Missing:
        pass

    class NonCallableDispatch:
        dispatch = None

    assert not isinstance(Missing(), AccountCatalogPort)
    assert not isinstance(Missing(), BrokerOrderQueryPort)
    assert not isinstance(Missing(), OrderJournalPort)
    assert not isinstance(NonCallableDispatch(), LiveOrderDispatchPort)


def test_dispatch_can_only_return_transport_receipt() -> None:
    annotation = LiveOrderDispatchPort.__dict__["dispatch"].__annotations__["return"]

    assert annotation == "DispatchReceipt"
    assert "LiveOrder" not in annotation


def test_query_surfaces_are_separate_and_carry_completeness_evidence() -> None:
    method_names = {name for name in BrokerOrderQueryPort.__dict__ if not name.startswith("_")}

    assert method_names == {"query_open_orders", "query_fills", "query_positions"}
    assert OpenOrdersSnapshot.__annotations__["orders"] == "tuple[BrokerOpenOrderObservation, ...]"
    assert BrokerFillsSnapshot.__annotations__["fills"] == "tuple[BrokerFillObservation, ...]"
    assert BrokerPositionsSnapshot.__annotations__["positions"] == "tuple[BrokerPosition, ...]"


def test_journal_exposes_idempotency_cas_observations_and_ambiguity() -> None:
    methods = {name for name in OrderJournalPort.__dict__ if not name.startswith("_")}

    assert methods == {
        "reserve_client_order_id",
        "claim_dispatch",
        "record_dispatch_receipt",
        "append_raw_observation",
        "apply_normalized_event",
        "load_unresolved_observations",
        "load_ambiguous_observations",
    }


def test_import_does_not_initialize_io_sdk_configuration_or_environment() -> None:
    import tx_trade.orders.live_ports as module

    source = module.__loader__.get_source(module.__name__)  # type: ignore[union-attr]
    assert source is not None
    lowered = source.lower()
    for forbidden in (
        "sqlite",
        "skcom",
        "import os",
        "getenv",
        "config",
        "open(",
        "connect(",
    ):
        assert forbidden not in lowered


def test_raw_observation_is_immutable_and_payload_is_redacted() -> None:
    observation = RawBrokerObservation(
        "observation-1",
        "reply",
        1,
        1,
        NOW,
        b"sensitive-broker-payload",
    )

    assert "sensitive-broker-payload" not in repr(observation)
    with pytest.raises(FrozenInstanceError):
        observation.source = "changed"  # type: ignore[misc]


def test_query_snapshots_use_broker_only_observations_without_local_identity() -> None:
    order = open_order()
    fill = broker_fill()
    position = BrokerPosition("account-1", "TXF-202608", Decimal("1"), Decimal("22000"), NOW)

    orders = OpenOrdersSnapshot((order,), evidence(EvidenceQueryKind.OPEN_ORDERS))
    fills = BrokerFillsSnapshot((fill,), evidence(EvidenceQueryKind.FILLS))
    positions = BrokerPositionsSnapshot((position,), evidence(EvidenceQueryKind.POSITIONS))

    assert orders.orders == (order,)
    assert fills.fills == (fill,)
    assert positions.positions == (position,)
    assert not hasattr(order, "intent")
    assert not hasattr(order, "client_order_id")
    assert not hasattr(fill, "strategy_id")
    assert not hasattr(fill, "client_order_id")


def test_snapshot_evidence_kind_account_timestamp_and_elements_fail_closed() -> None:
    with pytest.raises(ValueError, match="OPEN_ORDERS"):
        OpenOrdersSnapshot((open_order(),), evidence(EvidenceQueryKind.FILLS))
    with pytest.raises(ValueError, match="account"):
        OpenOrdersSnapshot(
            (open_order(),),
            evidence(EvidenceQueryKind.OPEN_ORDERS, account_id="account-2"),
        )
    future = BrokerOpenOrderObservation(
        "open-observation-2",
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("1"),
        None,
        correlation(),
        NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="postdate"):
        OpenOrdersSnapshot((future,), evidence(EvidenceQueryKind.OPEN_ORDERS))
    with pytest.raises(TypeError, match="BrokerOpenOrderObservation"):
        OpenOrdersSnapshot((object(),), evidence(EvidenceQueryKind.OPEN_ORDERS))  # type: ignore[arg-type]


def test_completeness_evidence_requires_exact_scope_cursor_timestamp_and_reason() -> None:
    complete = evidence(EvidenceQueryKind.OPEN_ORDERS)

    assert complete.permits_absence_inference
    with pytest.raises(ValueError, match="source_cursor"):
        evidence(EvidenceQueryKind.OPEN_ORDERS, source_cursor="")
    with pytest.raises(ValueError, match="reason"):
        CompletenessEvidence(
            EvidenceQueryKind.OPEN_ORDERS,
            "account-1",
            EvidenceCompleteness.INCOMPLETE,
            NOW,
            "snapshot-1",
        )
    with pytest.raises(ValueError, match="UTC"):
        CompletenessEvidence(
            EvidenceQueryKind.OPEN_ORDERS,
            "account-1",
            EvidenceCompleteness.COMPLETE,
            NOW.astimezone(timezone(timedelta(hours=8))),
            "snapshot-1",
        )


def test_reply_batch_requires_reply_backfill_evidence_and_bounded_timestamp() -> None:
    raw = RawBrokerObservation("raw-1", "reply", 1, 1, NOW, b"payload")

    batch = BrokerReplyBatch((raw,), evidence(EvidenceQueryKind.REPLY_BACKFILL))
    assert batch.observations == (raw,)
    with pytest.raises(ValueError, match="REPLY_BACKFILL"):
        BrokerReplyBatch((raw,), evidence(EvidenceQueryKind.FILLS))


def test_journal_results_enforce_fingerprints_dispositions_versions_and_ambiguity() -> None:
    reserved = ClientOrderIdReservation(
        "client-order-1",
        FINGERPRINT,
        None,
        ReservationDisposition.RESERVED,
    )
    exact = ClientOrderIdReservation(
        "client-order-1",
        FINGERPRINT,
        FINGERPRINT,
        ReservationDisposition.EXACT_RETRY,
    )
    claim = DispatchClaim(
        "command-1",
        FINGERPRINT,
        DispatchClaimDisposition.ACQUIRED,
        1,
        "claim-1",
    )
    raw = RawBrokerObservation("raw-1", "reply", 1, 1, NOW, b"payload")

    assert reserved.may_continue and exact.may_continue and claim.acquired
    with pytest.raises(ValueError, match="fingerprint equality"):
        ClientOrderIdReservation(
            "client-order-1",
            FINGERPRINT,
            FINGERPRINT,
            ReservationDisposition.PAYLOAD_CONFLICT,
        )
    with pytest.raises(ValueError, match="fingerprint"):
        ClientOrderIdReservation(
            "client-order-1",
            "not-a-fingerprint",
            None,
            ReservationDisposition.RESERVED,
        )
    with pytest.raises(ValueError, match="positive"):
        DispatchClaim(
            "command-1",
            FINGERPRINT,
            DispatchClaimDisposition.ACQUIRED,
            0,
            "claim-1",
        )
    with pytest.raises(ValueError, match="claim_token"):
        DispatchClaim(
            "command-1",
            FINGERPRINT,
            DispatchClaimDisposition.ALREADY_CLAIMED,
            1,
            "claim-1",
        )
    with pytest.raises(ValueError, match="at least two"):
        AmbiguousObservation(raw, ("client-order-1",))
    with pytest.raises(ValueError, match="unique"):
        AmbiguousObservation(raw, ("client-order-1", "client-order-1"))


def test_event_application_conflict_is_explicit_and_requires_reconciliation() -> None:
    conflict = EventApplicationResult(
        "event-1",
        EventApplicationDisposition.EVENT_CONFLICT,
        None,
        LiveFailureCode.CORRELATION_CONFLICT,
    )

    assert conflict.requires_reconciliation
    assert conflict.failure_code is LiveFailureCode.CORRELATION_CONFLICT
    with pytest.raises(ValueError, match="CORRELATION_CONFLICT"):
        EventApplicationResult(
            "event-1",
            EventApplicationDisposition.EVENT_CONFLICT,
            None,
        )
    with pytest.raises(ValueError, match="CORRELATION_CONFLICT"):
        EventApplicationResult(
            "event-1",
            EventApplicationDisposition.UNRESOLVED,
            None,
            LiveFailureCode.CORRELATION_CONFLICT,
        )
    with pytest.raises(TypeError, match="failure_code"):
        EventApplicationResult(
            "event-1",
            EventApplicationDisposition.EVENT_CONFLICT,
            None,
            "correlation_conflict",  # type: ignore[arg-type]
        )


def reconciliation_evidence() -> tuple[CompletenessEvidence, ...]:
    return (
        evidence(EvidenceQueryKind.OPEN_ORDERS, source_cursor="open-1"),
        evidence(EvidenceQueryKind.FILLS, source_cursor="fills-1"),
        evidence(EvidenceQueryKind.POSITIONS, source_cursor="positions-1"),
    )


def test_reconciliation_authority_requires_complete_nonempty_unique_coverage() -> None:
    complete = ReconciliationResult(
        "account-1",
        ReconciliationStatus.COMPLETE,
        (),
        reconciliation_evidence(),
        NOW,
    )
    partial = ReconciliationResult(
        "account-1",
        ReconciliationStatus.INCOMPLETE,
        (),
        (),
        NOW,
    )

    assert complete.is_authoritative
    assert not partial.is_authoritative
    with pytest.raises(ValueError, match="all broker query evidence"):
        ReconciliationResult("account-1", ReconciliationStatus.COMPLETE, (), (), NOW)
    with pytest.raises(ValueError, match="all broker query evidence"):
        ReconciliationResult(
            "account-1",
            ReconciliationStatus.COMPLETE,
            (),
            reconciliation_evidence()[:-1],
            NOW,
        )
    duplicate = reconciliation_evidence() + (
        evidence(EvidenceQueryKind.FILLS, source_cursor="fills-2"),
    )
    with pytest.raises(ValueError, match="unique"):
        ReconciliationResult(
            "account-1",
            ReconciliationStatus.COMPLETE,
            (),
            duplicate,
            NOW,
        )


def test_reconciliation_rejects_cross_account_and_incomplete_complete_evidence() -> None:
    wrong_account = (
        evidence(EvidenceQueryKind.OPEN_ORDERS, account_id="account-2"),
        *reconciliation_evidence()[1:],
    )
    with pytest.raises(ValueError, match="account"):
        ReconciliationResult(
            "account-1",
            ReconciliationStatus.COMPLETE,
            (),
            wrong_account,
            NOW,
        )
    incomplete = (
        evidence(
            EvidenceQueryKind.OPEN_ORDERS,
            status=EvidenceCompleteness.UNKNOWN,
            reason="query timed out",
        ),
        *reconciliation_evidence()[1:],
    )
    with pytest.raises(ValueError, match="complete evidence"):
        ReconciliationResult(
            "account-1",
            ReconciliationStatus.COMPLETE,
            (),
            incomplete,
            NOW,
        )

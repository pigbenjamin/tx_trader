from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tx_trade.orders.live_contracts import (
    LiveOrderIntent,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    ReconciliationResult,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ClaimResolution,
    ClaimResolutionDirective,
    DurableReconciliationCommitRequest,
    DurableReconciliationCommitResult,
    ExpectedOrderVersion,
    ObservationResolution,
    ObservationResolutionDirective,
    ObservationStatus,
    ReconciliationCommitDisposition,
    RequirementResolution,
    RequirementResolutionDirective,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)
from tx_trade.orders.live_state_machine import create_live_order

NOW = datetime(2026, 8, 3, tzinfo=timezone.utc)


def assessment(*, account_id: str = "account-1", sequence: int = 4) -> ReconciliationAssessment:
    local = LocalReconciliationSnapshot(account_id, (), (), (), NOW, (), sequence)
    captured_at = NOW + timedelta(seconds=1)

    def evidence(kind: EvidenceQueryKind) -> CompletenessEvidence:
        return CompletenessEvidence(
            kind,
            account_id,
            EvidenceCompleteness.COMPLETE,
            captured_at,
            "snapshot-1",
        )

    open_orders = OpenOrdersSnapshot((), evidence(EvidenceQueryKind.OPEN_ORDERS))
    fills = BrokerFillsSnapshot((), evidence(EvidenceQueryKind.FILLS))
    positions = BrokerPositionsSnapshot((), evidence(EvidenceQueryKind.POSITIONS))
    broker = BrokerReconciliationSnapshot(
        "snapshot-1", account_id, open_orders, fills, positions, captured_at
    )
    result = ReconciliationResult(
        account_id,
        ReconciliationStatus.COMPLETE,
        (),
        (open_orders.evidence, fills.evidence, positions.evidence),
        captured_at + timedelta(seconds=1),
    )
    return ReconciliationAssessment(local, broker, result)


def projected_order(*, account_id: str = "account-1"):
    intent = LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id="order-1",
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
    return replace(create_live_order(intent), version=2)


def request(**changes: object) -> DurableReconciliationCommitRequest:
    values: dict[str, object] = {
        "commit_id": "commit-1",
        "account_id": "account-1",
        "assessment": assessment(),
        "expected_journal_sequence": 4,
        "expected_order_versions": (ExpectedOrderVersion("order-1", 1),),
        "claim_resolutions": (
            ClaimResolutionDirective(
                "command-1", "claim-token-1", ClaimResolution.BROKER_ORDER_CONFIRMED
            ),
        ),
        "observation_resolutions": (
            ObservationResolutionDirective(
                "observation-1",
                ObservationStatus.UNRESOLVED,
                "event-1",
                ObservationResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
        "requirement_resolutions": (
            RequirementResolutionDirective(1, RequirementResolution.SATISFIED),
        ),
        "order_projections": (projected_order(),),
    }
    values.update(changes)
    return DurableReconciliationCommitRequest(**values)  # type: ignore[arg-type]


def test_valid_request_is_frozen_slotted_and_has_no_committed_at() -> None:
    value = request()

    assert value.assessment.local_snapshot.journal_sequence == 4
    assert "committed_at" not in {item.name for item in fields(value)}
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        value.commit_id = "different"  # type: ignore[misc]


@pytest.mark.parametrize("disposition", tuple(ReconciliationCommitDisposition))
def test_every_disposition_has_a_typed_valid_result(
    disposition: ReconciliationCommitDisposition,
) -> None:
    durable = disposition in {
        ReconciliationCommitDisposition.COMMITTED,
        ReconciliationCommitDisposition.EXACT_RETRY,
    }
    result = DurableReconciliationCommitResult(
        "commit-1",
        "account-1",
        disposition,
        NOW if durable else None,
        5 if durable else None,
        ("command-1",) if durable else (),
        ("observation-1",) if durable else (),
        (1,) if durable else (),
        (projected_order(),) if durable else (),
    )
    assert result.disposition is disposition
    if disposition is ReconciliationCommitDisposition.EXACT_RETRY:
        assert result.committed_at is NOW
        assert result.resulting_journal_sequence == 5
        assert result.resolved_claim_ids == ("command-1",)
        assert result.order_projections == (projected_order(),)


@pytest.mark.parametrize(
    ("field_name", "duplicate"),
    (
        (
            "expected_order_versions",
            (ExpectedOrderVersion("order-1", 1), ExpectedOrderVersion("order-1", 1)),
        ),
        (
            "claim_resolutions",
            (
                ClaimResolutionDirective(
                    "command-1",
                    "claim-token-1",
                    ClaimResolution.BROKER_ORDER_CONFIRMED,
                ),
                ClaimResolutionDirective(
                    "command-1",
                    "claim-token-2",
                    ClaimResolution.BROKER_FILL_CONFIRMED,
                ),
            ),
        ),
        (
            "observation_resolutions",
            (
                ObservationResolutionDirective(
                    "observation-1",
                    ObservationStatus.UNRESOLVED,
                    "event-1",
                    ObservationResolution.BROKER_ORDER_CONFIRMED,
                ),
                ObservationResolutionDirective(
                    "observation-1",
                    ObservationStatus.CONFLICT,
                    "event-2",
                    ObservationResolution.BROKER_FILL_CONFIRMED,
                ),
            ),
        ),
        (
            "requirement_resolutions",
            (
                RequirementResolutionDirective(1, RequirementResolution.SATISFIED),
                RequirementResolutionDirective(1, RequirementResolution.SATISFIED),
            ),
        ),
        ("order_projections", (projected_order(), projected_order())),
    ),
)
def test_request_rejects_duplicate_targets(field_name: str, duplicate: object) -> None:
    with pytest.raises(ValueError, match="unique"):
        request(**{field_name: duplicate})


def test_request_rejects_bad_ids_sequences_versions_and_types() -> None:
    with pytest.raises(ValueError, match="commit_id"):
        request(commit_id="")
    with pytest.raises(ValueError, match="nonnegative"):
        request(expected_journal_sequence=-1)
    with pytest.raises(TypeError, match="integer"):
        request(expected_journal_sequence=True)
    with pytest.raises(ValueError, match="positive"):
        ExpectedOrderVersion("order-1", -1)
    with pytest.raises(ValueError, match="positive"):
        RequirementResolutionDirective(0, RequirementResolution.SATISFIED)
    with pytest.raises(TypeError, match="ClaimResolution"):
        ClaimResolutionDirective("command-1", "claim-token-1", "resolved")  # type: ignore[arg-type]


def test_directives_require_cas_tokens_event_provenance_and_typed_status() -> None:
    with pytest.raises(ValueError, match="claim_token"):
        ClaimResolutionDirective("command-1", "", ClaimResolution.BROKER_ORDER_CONFIRMED)
    with pytest.raises(ValueError, match="normalized_event_id"):
        ObservationResolutionDirective(
            "observation-1",
            ObservationStatus.UNRESOLVED,
            "",
            ObservationResolution.BROKER_FILL_CONFIRMED,
        )
    with pytest.raises(TypeError, match="ObservationStatus"):
        ObservationResolutionDirective(
            "observation-1",
            "unresolved",  # type: ignore[arg-type]
            "event-1",
            ObservationResolution.BROKER_FILL_CONFIRMED,
        )
    with pytest.raises(TypeError, match="ObservationResolution"):
        ObservationResolutionDirective(
            "observation-1",
            ObservationStatus.AMBIGUOUS,
            "event-1",
            "resolved",  # type: ignore[arg-type]
        )


def test_request_requires_assessment_cut_and_account_consistency() -> None:
    with pytest.raises(ValueError, match="durable cut"):
        request(expected_journal_sequence=3)
    with pytest.raises(ValueError, match="assessment"):
        request(account_id="account-2")
    with pytest.raises(ValueError, match="request account"):
        request(order_projections=(projected_order(account_id="account-2"),))
    with pytest.raises(ValueError, match="requires an expected"):
        request(expected_order_versions=())
    with pytest.raises(ValueError, match="advance"):
        request(expected_order_versions=(ExpectedOrderVersion("order-1", 2),))


def test_local_snapshot_journal_sequence_is_exact_nonnegative_integer() -> None:
    assert LocalReconciliationSnapshot("account-1", (), (), (), NOW).journal_sequence == 0
    with pytest.raises(ValueError, match="nonnegative"):
        LocalReconciliationSnapshot("account-1", (), (), (), NOW, (), -1)
    with pytest.raises(TypeError, match="integer"):
        LocalReconciliationSnapshot("account-1", (), (), (), NOW, (), True)


def test_result_rejects_incomplete_or_cross_account_durable_data() -> None:
    with pytest.raises(ValueError, match="require committed_at"):
        DurableReconciliationCommitResult(
            "commit-1", "account-1", ReconciliationCommitDisposition.EXACT_RETRY
        )
    with pytest.raises(ValueError, match="must not contain"):
        DurableReconciliationCommitResult(
            "commit-1",
            "account-1",
            ReconciliationCommitDisposition.ID_CONFLICT,
            resolved_claim_ids=("command-1",),
        )
    with pytest.raises(ValueError, match="result account"):
        DurableReconciliationCommitResult(
            "commit-1",
            "account-1",
            ReconciliationCommitDisposition.COMMITTED,
            NOW,
            5,
            order_projections=(projected_order(account_id="account-2"),),
        )

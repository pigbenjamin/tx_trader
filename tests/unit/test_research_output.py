from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from uuid import UUID

import pytest

import tx_trade.app.research_output as research_output
from tx_trade.app.research_output import (
    ResearchOutputCorrelation,
    ResearchOutputError,
    ResearchOutputLimits,
    encode_market_record,
    encode_paper_record,
    encode_summary_record,
    materialize_research_jsonl,
)
from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.orders import (
    MatchDisposition,
    MatchResult,
    OrderSide,
    OrderType,
    PaperDecisionBatchResult,
    PaperBrokerLimits,
    TimeInForce,
)
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.strategy import (
    InstrumentTriggeredOrderStrategy,
    OrderTemplate,
    PaperReplayCoordinator,
    StrategyExecutionMode,
    StrategyRegistration,
)

PAPER_RUN_ID = UUID("d74caa1f-0e57-4d4c-a6ef-a727144797dc")


def _completed_run():
    envelopes = make_offline_fixture_envelopes()
    broker = PaperBroker(
        paper_run_id=PAPER_RUN_ID,
        limits=PaperBrokerLimits(
            max_orders=10,
            max_open_orders=10,
            max_fills=10,
            max_events=30,
            max_market_data_records=20,
            max_instrument_versions=10,
            max_positions=10,
        ),
    )
    strategy = InstrumentTriggeredOrderStrategy(
        OrderTemplate(
            strategy_id="research",
            client_order_id="entry",
            account_id="paper-account",
            instrument_id="TAIFEX:0:TX00",
            side=OrderSide.BUY,
            quantity=Decimal("1.00"),
            order_type=OrderType.MARKET,
            limit_price=None,
            time_in_force=TimeInForce.DAY,
            day_trade=False,
        )
    )
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("research", strategy),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=len(envelopes),
    )
    for envelope in envelopes:
        coordinator.publish(envelope)
    snapshot = broker.snapshot()
    correlation = ResearchOutputCorrelation(
        replay_session_id=envelopes[0].session_id,
        paper_run_id=PAPER_RUN_ID,
        execution_config_fingerprint=snapshot.execution_config_fingerprint,
        terminal_cursor=envelopes[-1].ingest_sequence,
    )
    return envelopes, coordinator.decision_records(), snapshot, correlation


def _limits(**overrides: int) -> ResearchOutputLimits:
    values = {
        "max_market_records": 20,
        "max_paper_events": 30,
        "max_decision_records": 20,
        "max_output_bytes": 1_000_000,
    }
    values.update(overrides)
    return ResearchOutputLimits(**values)


def test_materializes_canonical_market_paper_and_one_terminal_summary() -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()

    first = materialize_research_jsonl(
        market_envelopes=envelopes,
        decision_records=decisions,
        broker_snapshot=snapshot,
        correlation=correlation,
        limits=_limits(),
    )
    second = materialize_research_jsonl(
        market_envelopes=envelopes,
        decision_records=decisions,
        broker_snapshot=snapshot,
        correlation=correlation,
        limits=_limits(),
    )

    assert first == second
    assert first.endswith(b"\n")
    records = tuple(json.loads(line) for line in first.decode("utf-8").splitlines())
    assert [record["record_type"] for record in records].count("market") == len(envelopes)
    assert [record["record_type"] for record in records].count("paper") == len(snapshot.events)
    assert [record["record_type"] for record in records].count("summary") == 1
    assert all(record["schema_version"] == 1 for record in records)
    assert records[0]["envelope"]["event_type"] == "connection_status"
    paper_records = tuple(record for record in records if record["record_type"] == "paper")
    assert tuple(record["event"]["paper_sequence"] for record in paper_records) == tuple(
        event.paper_sequence for event in snapshot.events
    )
    assert paper_records[0]["event"]["payload"]["intent"]["quantity"] == "1"
    summary = records[-1]
    assert summary["replay_session_id"] == str(correlation.replay_session_id)
    assert summary["paper_run_id"] == str(PAPER_RUN_ID)
    assert summary["execution_config_fingerprint"] == snapshot.execution_config_fingerprint
    assert summary["terminal_cursor"] == envelopes[-1].ingest_sequence
    assert summary["terminal_broker_sequence"] == snapshot.next_paper_sequence - 1
    assert summary["counts"] == {
        "decisions": len(decisions),
        "fills": len(snapshot.fills),
        "market_records": len(envelopes),
        "orders": len(snapshot.orders),
        "paper_events": len(snapshot.events),
        "positions": len(snapshot.positions),
    }


def test_single_record_encoders_preserve_materialized_schema_bytes() -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()
    decision_by_source = {
        (record.source_session_id, record.source_ingest_sequence): record for record in decisions
    }
    encoded = b"".join(encode_market_record(envelope) for envelope in envelopes)
    encoded += b"".join(
        encode_paper_record(
            event,
            decision_by_source[(event.source_session_id, event.source_ingest_sequence)],
        )
        for event in snapshot.events
        if event.source_session_id is not None and event.source_ingest_sequence is not None
    )
    encoded += encode_summary_record(
        market_record_count=len(envelopes),
        decision_record_count=len(decisions),
        broker_snapshot=snapshot,
        correlation=correlation,
    )

    assert encoded == materialize_research_jsonl(
        market_envelopes=envelopes,
        decision_records=decisions,
        broker_snapshot=snapshot,
        correlation=correlation,
        limits=_limits(),
    )


def test_paper_records_come_from_complete_snapshot_journal() -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()
    assert snapshot.events
    # Duplicate retry results are empty, while the authoritative journal remains complete.
    duplicate_decisions = tuple(
        replace(
            record,
            batch_result=PaperDecisionBatchResult(
                paper_run_id=PAPER_RUN_ID,
                source_session_id=record.source_session_id,
                source_ingest_sequence=record.source_ingest_sequence,
                decision_fingerprint=record.decision.decision_fingerprint,
                match_result=MatchResult(
                    paper_run_id=PAPER_RUN_ID,
                    disposition=MatchDisposition.DUPLICATE,
                    source_session_id=record.source_session_id,
                    source_ingest_sequence=record.source_ingest_sequence,
                    fills=(),
                    events=(),
                    skip_reasons=(),
                    snapshot_version=snapshot.snapshot_version,
                ),
                command_results=(),
                events=(),
            ),
        )
        for record in decisions
    )

    output = materialize_research_jsonl(
        market_envelopes=envelopes,
        decision_records=duplicate_decisions,
        broker_snapshot=snapshot,
        correlation=correlation,
        limits=_limits(),
    )
    records = tuple(json.loads(line) for line in output.decode("utf-8").splitlines())
    assert sum(record["record_type"] == "paper" for record in records) == len(snapshot.events)


@pytest.mark.parametrize(
    "mutation",
    ["session", "paper_run", "fingerprint", "cursor", "decision_digest"],
)
def test_rejects_correlation_mismatch_with_sanitized_error(mutation: str) -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()
    if mutation == "session":
        correlation = replace(
            correlation, replay_session_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        )
    elif mutation == "paper_run":
        correlation = replace(
            correlation, paper_run_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
        )
    elif mutation == "fingerprint":
        correlation = replace(correlation, execution_config_fingerprint="sha256:" + "a" * 64)
    elif mutation == "cursor":
        correlation = replace(correlation, terminal_cursor=correlation.terminal_cursor - 1)
    else:
        decisions = (replace(decisions[0], envelope_digest="a" * 64), *decisions[1:])

    with pytest.raises(ResearchOutputError) as caught:
        materialize_research_jsonl(
            market_envelopes=envelopes,
            decision_records=decisions,
            broker_snapshot=snapshot,
            correlation=correlation,
            limits=_limits(),
        )

    assert str(caught.value) == "research output materialization failed"


@pytest.mark.parametrize(
    "limits",
    [
        _limits(max_market_records=1),
        _limits(max_paper_events=1),
        _limits(max_decision_records=1),
        _limits(max_output_bytes=1),
    ],
)
def test_capacity_limits_fail_closed(limits: ResearchOutputLimits) -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()

    with pytest.raises(ResearchOutputError, match="^research output materialization failed$"):
        materialize_research_jsonl(
            market_envelopes=envelopes,
            decision_records=decisions,
            broker_snapshot=snapshot,
            correlation=correlation,
            limits=limits,
        )


def test_rejects_partial_or_unordered_inputs() -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()

    with pytest.raises(ResearchOutputError, match="^research output materialization failed$"):
        materialize_research_jsonl(
            market_envelopes=tuple(reversed(envelopes)),
            decision_records=decisions,
            broker_snapshot=snapshot,
            correlation=correlation,
            limits=_limits(),
        )


def test_rejects_empty_input_because_replay_sessions_are_non_empty() -> None:
    _, _, snapshot, correlation = _completed_run()

    with pytest.raises(ResearchOutputError, match="^research output materialization failed$"):
        materialize_research_jsonl(
            market_envelopes=(),
            decision_records=(),
            broker_snapshot=snapshot,
            correlation=correlation,
            limits=_limits(),
        )


@pytest.mark.parametrize("control_flow", [KeyboardInterrupt, SystemExit])
def test_does_not_sanitize_control_flow_exceptions(
    monkeypatch: pytest.MonkeyPatch,
    control_flow: type[BaseException],
) -> None:
    envelopes, decisions, snapshot, correlation = _completed_run()

    def interrupt(*args: object, **kwargs: object) -> object:
        raise control_flow()

    monkeypatch.setattr(research_output.json, "dumps", interrupt)

    with pytest.raises(control_flow):
        materialize_research_jsonl(
            market_envelopes=envelopes,
            decision_records=decisions,
            broker_snapshot=snapshot,
            correlation=correlation,
            limits=_limits(),
        )

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal, getcontext
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_SESSION_ID,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import Instrument, MarketDataEnvelope, Quote
from tx_trade.orders.contracts import (
    CancelIntent,
    FeePolicyKind,
    FeeRoundingMode,
    MatchDisposition,
    MatchSkipReason,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperBrokerLimits,
    PaperExecutionConfig,
    PaperEventType,
    PaperFeeRule,
    PaperFeeSchedule,
    PaperRejection,
    RejectionCode,
    SlippageConfig,
    SlippageMode,
    TimeInForce,
    canonical_json,
)
from tx_trade.orders.paper_broker import (
    PaperBroker,
    PaperBrokerCapacityError,
    PaperBrokerInputError,
)
from tx_trade.orders.execution_policies import (
    ExecutionPolicyError,
    ExecutionPolicyErrorCode,
    assess_fee,
)

RUN_ID = UUID("11111111-1111-1111-1111-111111111111")
LIMITS = PaperBrokerLimits(
    max_orders=20,
    max_open_orders=20,
    max_fills=50,
    max_events=100,
    max_market_data_records=100,
    max_instrument_versions=20,
)


def _broker(
    limits: PaperBrokerLimits = LIMITS,
    execution_config: PaperExecutionConfig | None = None,
) -> PaperBroker:
    return PaperBroker(
        paper_run_id=RUN_ID,
        expected_source_session_id=OFFLINE_FIXTURE_SESSION_ID,
        limits=limits,
        execution_config=execution_config,
    )


def _unbound_broker(limits: PaperBrokerLimits = LIMITS) -> PaperBroker:
    return PaperBroker(
        paper_run_id=RUN_ID,
        limits=limits,
    )


def _intent(
    client_order_id: str,
    *,
    side: OrderSide = OrderSide.BUY,
    quantity: str = "1",
    order_type: OrderType = OrderType.MARKET,
    limit_price: str | None = None,
    source_sequence: int | None = None,
    strategy_id: str = "strategy",
    account_id: str = "paper",
) -> OrderIntent:
    quote = make_offline_fixture_envelopes()[3].payload
    assert isinstance(quote, Quote)
    return OrderIntent(
        strategy_id=strategy_id,
        client_order_id=client_order_id,
        account_id=account_id,
        instrument_id=quote.instrument_id,
        side=side,
        quantity=Decimal(quantity),
        order_type=order_type,
        limit_price=None if limit_price is None else Decimal(limit_price),
        time_in_force=TimeInForce.DAY,
        day_trade=False,
        created_at=quote.received_at,
        source_session_id=(None if source_sequence is None else OFFLINE_FIXTURE_SESSION_ID),
        source_ingest_sequence=source_sequence,
    )


def _shift_quote(
    envelope: MarketDataEnvelope,
    sequence: int,
    *,
    bid_qty: int | None = None,
    ask_qty: int | None = None,
    seconds: int = 0,
) -> MarketDataEnvelope:
    quote = envelope.payload
    assert isinstance(quote, Quote)
    timestamp = quote.received_at + timedelta(seconds=seconds)
    changed = replace(
        quote,
        bid_qty_raw=quote.bid_qty_raw if bid_qty is None else bid_qty,
        ask_qty_raw=quote.ask_qty_raw if ask_qty is None else ask_qty,
        event_at=timestamp,
        received_at=timestamp,
    )
    return replace(
        envelope,
        payload=changed,
        ingest_sequence=sequence,
        sequence=sequence,
        dedupe_key=f"quote:{sequence}",
        event_at=timestamp,
        received_at=timestamp,
    )


def _prime(broker: PaperBroker) -> tuple[MarketDataEnvelope, MarketDataEnvelope]:
    envelopes = make_offline_fixture_envelopes()
    instrument = envelopes[2]
    quote = envelopes[3]
    broker.process_market_data(instrument)
    return instrument, quote


def _execution_config(
    *,
    slippage_mode: SlippageMode = SlippageMode.NONE,
    slippage_value: str = "0",
    fee_instrument_id: str | None = None,
    fee_currency: str = "TWD",
    fee_per_unit: str = "0.6",
    fee_quantum: str = "0.01",
) -> PaperExecutionConfig:
    schedule = (
        PaperFeeSchedule()
        if fee_instrument_id is None
        else PaperFeeSchedule(
            kind=FeePolicyKind.PER_UNIT,
            rules=(
                PaperFeeRule(
                    instrument_id=fee_instrument_id,
                    currency=fee_currency,
                    amount_per_unit=Decimal(fee_per_unit),
                    quantum=Decimal(fee_quantum),
                    rounding_mode=FeeRoundingMode.ROUND_HALF_UP,
                    policy_id="unit-test",
                    policy_version="1",
                ),
            ),
        )
    )
    return PaperExecutionConfig(
        slippage=SlippageConfig(
            mode=slippage_mode,
            value=Decimal(slippage_value),
        ),
        fee_schedule=schedule,
    )


def test_submit_idempotency_conflict_cancel_and_query_ordering() -> None:
    broker = _unbound_broker()
    first_intent = _intent("first")
    first = broker.submit(first_intent)
    retry = broker.submit(first_intent)
    conflict = broker.submit(replace(first_intent, quantity=Decimal("2")))
    second = broker.submit(_intent("second"))

    assert retry == first
    assert isinstance(conflict, PaperRejection)
    assert conflict.code is RejectionCode.IDEMPOTENCY_CONFLICT
    assert broker.list_orders() == (first, second)
    assert broker.get_order(first.paper_order_id) == first
    assert broker.list_positions() == ()

    request = CancelIntent(
        strategy_id="strategy",
        client_order_id="first",
        paper_order_id=first.paper_order_id,
        requested_at=first.updated_at,
    )
    cancelled = broker.cancel(request)
    assert cancelled.status is OrderStatus.CANCELLED
    event_count = len(broker.snapshot().events)
    assert broker.cancel(request) == cancelled
    assert len(broker.snapshot().events) == event_count


def test_exact_submit_retry_returns_current_order_without_side_effects() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    intent = _intent("lifecycle-retry", quantity="5")
    accepted = broker.submit(intent)

    broker.process_market_data(quote)
    partial = broker.get_order(accepted.paper_order_id)
    assert partial is not None
    assert partial.status is OrderStatus.PARTIALLY_FILLED
    partial_snapshot = broker.snapshot()
    assert broker.submit(intent) == partial
    assert broker.snapshot() == partial_snapshot

    broker.process_market_data(_shift_quote(quote, 4, ask_qty=2, seconds=1))
    filled = broker.get_order(accepted.paper_order_id)
    assert filled is not None
    assert filled.status is OrderStatus.FILLED
    filled_snapshot = broker.snapshot()
    assert broker.submit(intent) == filled
    assert broker.snapshot() == filled_snapshot

    cancel_intent = _intent("cancelled-retry")
    open_order = broker.submit(cancel_intent)
    cancelled = broker.cancel(
        CancelIntent(
            strategy_id=cancel_intent.strategy_id,
            client_order_id=cancel_intent.client_order_id,
            paper_order_id=open_order.paper_order_id,
            requested_at=cancel_intent.created_at + timedelta(seconds=2),
        )
    )
    cancelled_snapshot = broker.snapshot()
    assert broker.submit(cancel_intent) == cancelled
    assert broker.snapshot() == cancelled_snapshot


def test_rejected_commands_and_conflicts_are_idempotent() -> None:
    broker = _broker()
    accepted_intent = _intent("conflict")
    broker.submit(accepted_intent)
    conflicting = replace(accepted_intent, quantity=Decimal("2"))
    first_conflict = broker.submit(conflicting)
    conflict_snapshot = broker.snapshot()
    assert broker.submit(conflicting) == first_conflict
    assert broker.snapshot() == conflict_snapshot

    unknown = CancelIntent(
        strategy_id="strategy",
        client_order_id="missing",
        paper_order_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        requested_at=accepted_intent.created_at,
    )
    first_unknown = broker.cancel(unknown)
    unknown_snapshot = broker.snapshot()
    assert broker.cancel(unknown) == first_unknown
    assert broker.snapshot() == unknown_snapshot

    _, quote = _prime(broker)
    filled = broker.submit(_intent("filled"))
    broker.process_market_data(quote)
    terminal_request = CancelIntent(
        strategy_id="strategy",
        client_order_id="filled",
        paper_order_id=filled.paper_order_id,
        requested_at=quote.received_at,
    )
    first_terminal = broker.cancel(terminal_request)
    terminal_snapshot = broker.snapshot()
    assert broker.cancel(terminal_request) == first_terminal
    assert broker.snapshot() == terminal_snapshot


def test_command_outcome_indexes_are_bounded_without_unreplayable_events() -> None:
    limits = PaperBrokerLimits(
        max_orders=1,
        max_open_orders=1,
        max_fills=1,
        max_events=10,
        max_market_data_records=2,
        max_instrument_versions=1,
    )
    submit_broker = _broker(limits)
    session_b = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    invalid = replace(
        _intent("cached-invalid", source_sequence=1),
        source_session_id=session_b,
    )
    submit_broker.submit(invalid)
    before_uncached_submit = submit_broker.snapshot()
    uncached_submit = submit_broker.submit(_intent("uncached"))
    assert isinstance(uncached_submit, PaperRejection)
    assert uncached_submit.code is RejectionCode.CAPACITY_EXCEEDED
    assert submit_broker.submit(_intent("uncached")) == uncached_submit
    assert submit_broker.snapshot() == before_uncached_submit
    assert len(submit_broker._state.submit_outcomes) == 1

    cancel_broker = _broker(limits)
    first = CancelIntent(
        strategy_id="strategy",
        client_order_id="first-missing",
        paper_order_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        requested_at=_intent("time").created_at,
    )
    cancel_broker.cancel(first)
    second = replace(
        first,
        client_order_id="second-missing",
        paper_order_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
    )
    before_uncached_cancel = cancel_broker.snapshot()
    uncached_cancel = cancel_broker.cancel(second)
    assert isinstance(uncached_cancel, PaperRejection)
    assert uncached_cancel.code is RejectionCode.UNKNOWN_ORDER
    assert cancel_broker.cancel(second) == uncached_cancel
    assert cancel_broker.snapshot() == before_uncached_cancel
    assert len(cancel_broker._state.cancel_outcomes) == 1


def test_cancel_variants_and_saturated_cache_do_not_change_business_semantics() -> None:
    limits = PaperBrokerLimits(
        max_orders=1,
        max_open_orders=1,
        max_fills=1,
        max_events=10,
        max_market_data_records=2,
        max_instrument_versions=1,
    )
    broker = _broker(limits)
    intent = _intent("legal-cancel")
    order = broker.submit(intent)
    unknown = CancelIntent(
        strategy_id="strategy",
        client_order_id="missing",
        paper_order_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        requested_at=intent.created_at,
    )
    first_unknown = broker.cancel(unknown)
    assert isinstance(first_unknown, PaperRejection)
    assert first_unknown.code is RejectionCode.UNKNOWN_ORDER
    assert len(broker._state.cancel_outcomes) == 1

    request = CancelIntent(
        strategy_id=intent.strategy_id,
        client_order_id=intent.client_order_id,
        paper_order_id=order.paper_order_id,
        requested_at=intent.created_at + timedelta(seconds=1),
    )
    cancelled = broker.cancel(request)
    assert cancelled.status is OrderStatus.CANCELLED
    assert len(broker._state.cancel_outcomes) == 1
    cancelled_snapshot = broker.snapshot()
    for seconds in (2, 3, 4):
        assert (
            broker.cancel(
                replace(
                    request,
                    requested_at=intent.created_at + timedelta(seconds=seconds),
                )
            )
            == cancelled
        )
        assert broker.snapshot() == cancelled_snapshot

    variant_broker = _broker(limits)
    for seconds in (0, 1):
        result = variant_broker.cancel(
            replace(
                unknown,
                requested_at=intent.created_at + timedelta(seconds=seconds),
            )
        )
        assert isinstance(result, PaperRejection)
        assert result.code is RejectionCode.UNKNOWN_ORDER
    before_third_variant = variant_broker.snapshot()
    third = variant_broker.cancel(
        replace(unknown, requested_at=intent.created_at + timedelta(seconds=2))
    )
    assert isinstance(third, PaperRejection)
    assert third.code is RejectionCode.UNKNOWN_ORDER
    assert variant_broker.snapshot() == before_third_variant


def test_source_caused_intent_establishes_pending_session_fence() -> None:
    broker = _unbound_broker()
    intent_a = _intent("session-a", source_sequence=3)
    accepted = broker.submit(intent_a)
    session_b = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    intent_b = replace(
        _intent("session-b", source_sequence=3),
        source_session_id=session_b,
    )
    rejected = broker.submit(intent_b)
    assert accepted.status is OrderStatus.ACCEPTED
    assert isinstance(rejected, PaperRejection)
    assert rejected.code is RejectionCode.INVALID_INTENT
    rejected_snapshot = broker.snapshot()
    assert broker.submit(intent_b) == rejected
    assert broker.snapshot() == rejected_snapshot

    instrument = make_offline_fixture_envelopes()[2]
    before = broker.snapshot()
    with pytest.raises(PaperBrokerInputError, match="session"):
        broker.process_market_data(replace(instrument, session_id=session_b))
    assert broker.snapshot() == before
    broker.process_market_data(instrument)
    assert broker.snapshot().bound_source_session_id == OFFLINE_FIXTURE_SESSION_ID


def test_concurrent_source_intents_linearize_to_one_session() -> None:
    broker = _unbound_broker()
    session_b = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    intents = (
        _intent("race-a", source_sequence=3),
        replace(
            _intent("race-b", source_sequence=3),
            source_session_id=session_b,
        ),
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(broker.submit, intents))

    accepted = [outcome for outcome in outcomes if not isinstance(outcome, PaperRejection)]
    rejected = [outcome for outcome in outcomes if isinstance(outcome, PaperRejection)]
    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected[0].code is RejectionCode.INVALID_INTENT
    assert broker.list_orders() == (accepted[0],)

    instrument = make_offline_fixture_envelopes()[2]
    accepted_session = accepted[0].intent.source_session_id
    assert accepted_session is not None
    broker.process_market_data(replace(instrument, session_id=accepted_session))
    assert broker.snapshot().bound_source_session_id == accepted_session


def test_fifo_partial_fills_shared_liquidity_and_independent_sides() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    buy_one = broker.submit(_intent("buy-1", quantity="3"))
    buy_two = broker.submit(_intent("buy-2", quantity="3"))
    sell = broker.submit(_intent("sell", side=OrderSide.SELL, quantity="3"))
    result = broker.process_market_data(_shift_quote(quote, 3, bid_qty=2, ask_qty=4))

    assert [fill.paper_order_id for fill in result.fills] == [
        buy_one.paper_order_id,
        buy_two.paper_order_id,
        sell.paper_order_id,
    ]
    assert [fill.quantity for fill in result.fills] == [
        Decimal("3"),
        Decimal("1"),
        Decimal("2"),
    ]
    assert broker.get_order(buy_two.paper_order_id).status is OrderStatus.PARTIALLY_FILLED
    assert broker.get_order(sell.paper_order_id).status is OrderStatus.PARTIALLY_FILLED

    second = broker.process_market_data(_shift_quote(quote, 7, bid_qty=2, ask_qty=2, seconds=1))
    assert [fill.quantity for fill in second.fills] == [Decimal("2"), Decimal("1")]
    assert all(order.status is OrderStatus.FILLED for order in broker.list_orders())


def test_n_plus_one_limit_and_causal_time_rules() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    order = broker.submit(_intent("caused", source_sequence=3))
    same = broker.process_market_data(quote)
    assert not same.fills

    not_crossed = broker.submit(
        _intent(
            "limit",
            order_type=OrderType.LIMIT,
            limit_price="20001",
        )
    )
    next_quote = broker.process_market_data(_shift_quote(quote, 4, seconds=1))
    assert [fill.paper_order_id for fill in next_quote.fills] == [order.paper_order_id]
    assert broker.get_order(not_crossed.paper_order_id).status is OrderStatus.ACCEPTED


def test_exact_metadata_version_scale_and_multi_instrument_fail_closed() -> None:
    broker = _broker()
    instrument_envelope, quote = _prime(broker)
    instrument = instrument_envelope.payload
    assert isinstance(instrument, Instrument)
    broker.submit(_intent("waiting"))

    wrong_version = replace(
        quote, ingest_sequence=3, metadata_version=2, dedupe_key="wrong-version"
    )
    assert not broker.process_market_data(wrong_version).fills

    other = replace(
        instrument,
        instrument_id="TAIFEX:0:MXF",
        symbol="MXF",
        metadata_version=2,
        quantity_scale=Decimal("0.5"),
    )
    broker.process_market_data(
        replace(
            instrument_envelope,
            payload=other,
            ingest_sequence=4,
            sequence=4,
            dedupe_key="instrument:other",
            metadata_version=2,
        )
    )
    assert len(broker.snapshot().instruments) == 2


def test_duplicate_sequence_dedupe_session_and_source_fences() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    first = broker.process_market_data(quote)
    before = broker.snapshot()
    duplicate = broker.process_market_data(quote)
    assert duplicate.disposition is MatchDisposition.DUPLICATE
    assert broker.snapshot() == before

    with pytest.raises(PaperBrokerInputError, match="content conflict"):
        broker.process_market_data(replace(quote, dedupe_key="changed"))
    with pytest.raises(PaperBrokerInputError, match="dedupe key"):
        broker.process_market_data(
            replace(
                quote,
                ingest_sequence=4,
                sequence=4,
                dedupe_key=quote.dedupe_key,
            )
        )
    with pytest.raises(PaperBrokerInputError, match="session"):
        broker.process_market_data(
            replace(
                quote,
                session_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                ingest_sequence=5,
                sequence=5,
                dedupe_key="other-session",
            )
        )
    assert first.snapshot_version == before.snapshot_version


def test_capacity_and_base_exception_leave_snapshot_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _broker(
        PaperBrokerLimits(
            max_orders=2,
            max_open_orders=2,
            max_fills=1,
            max_events=10,
            max_market_data_records=10,
            max_instrument_versions=2,
        )
    )
    _, quote = _prime(broker)
    broker.submit(_intent("one"))
    broker.submit(_intent("two"))
    before_capacity = broker.snapshot()
    with pytest.raises(PaperBrokerCapacityError):
        broker.process_market_data(_shift_quote(quote, 3, ask_qty=2))
    assert broker.snapshot() == before_capacity

    class InjectedFailure(BaseException):
        pass

    def fail(*args: object) -> Decimal:
        raise InjectedFailure

    monkeypatch.setattr("tx_trade.orders.paper_broker.weighted_average", fail)
    with pytest.raises(InjectedFailure):
        broker.process_market_data(_shift_quote(quote, 3, ask_qty=1))
    assert broker.snapshot() == before_capacity


def test_commit_boundary_base_exception_leaves_state_byte_identical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _unbound_broker()
    before = canonical_json(broker.snapshot()).encode()

    class CommitFailure(BaseException):
        pass

    def fail_commit(self: PaperBroker, staged: object) -> None:
        raise CommitFailure

    monkeypatch.setattr(PaperBroker, "_commit", fail_commit)
    with pytest.raises(CommitFailure):
        broker.submit(_intent("commit-failure", source_sequence=3))

    assert canonical_json(broker.snapshot()).encode() == before
    session_b = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    envelope_b = replace(
        make_offline_fixture_envelopes()[2],
        session_id=session_b,
    )
    with pytest.raises(CommitFailure):
        broker.process_market_data(envelope_b)
    assert canonical_json(broker.snapshot()).encode() == before


def test_market_and_instrument_records_are_bounded_atomically() -> None:
    market_limited = _broker(
        PaperBrokerLimits(
            max_orders=2,
            max_open_orders=2,
            max_fills=2,
            max_events=10,
            max_market_data_records=1,
            max_instrument_versions=2,
        )
    )
    instrument, quote = _prime(market_limited)
    before_market_overflow = market_limited.snapshot()
    with pytest.raises(PaperBrokerCapacityError, match="record capacity"):
        market_limited.process_market_data(quote)
    assert market_limited.snapshot() == before_market_overflow
    assert all(len(digest) == 64 for digest in market_limited._state.envelope_fingerprints.values())

    metadata_limited = _broker(
        PaperBrokerLimits(
            max_orders=2,
            max_open_orders=2,
            max_fills=2,
            max_events=10,
            max_market_data_records=5,
            max_instrument_versions=1,
        )
    )
    metadata_limited.process_market_data(instrument)
    payload = instrument.payload
    assert isinstance(payload, Instrument)
    second_payload = replace(
        payload,
        instrument_id="TAIFEX:0:MXF",
        symbol="MXF",
        metadata_version=2,
    )
    second = replace(
        instrument,
        payload=second_payload,
        ingest_sequence=4,
        sequence=4,
        dedupe_key="instrument:second",
        metadata_version=2,
    )
    before_metadata_overflow = metadata_limited.snapshot()
    with pytest.raises(PaperBrokerCapacityError, match="metadata version"):
        metadata_limited.process_market_data(second)
    assert metadata_limited.snapshot() == before_metadata_overflow


def test_two_runs_produce_identical_canonical_journals() -> None:
    outputs: list[str] = []
    for _ in range(2):
        broker = _broker()
        _, quote = _prime(broker)
        broker.submit(_intent("deterministic", quantity="2"))
        broker.process_market_data(quote)
        outputs.append(canonical_json(broker.snapshot()))

    assert outputs[0].encode() == outputs[1].encode()
    events = _broker().snapshot().events
    assert events == ()


def test_broker_output_ignores_process_global_decimal_context() -> None:
    original = getcontext().copy()
    outputs: list[str] = []
    try:
        for precision in (6, 50):
            getcontext().prec = precision
            broker = _broker()
            _, quote = _prime(broker)
            broker.submit(
                _intent(
                    "decimal-context",
                    quantity="1.234567890123456789",
                )
            )
            broker.process_market_data(quote)
            outputs.append(canonical_json(broker.snapshot()))
    finally:
        getcontext().prec = original.prec
        getcontext().rounding = original.rounding

    assert outputs[0] == outputs[1]


def test_event_order_and_fee_are_fixed() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    broker.submit(_intent("events"))
    result = broker.process_market_data(quote)

    assert result.fills[0].fee == Decimal("0")
    assert [event.event_type for event in result.events] == [
        PaperEventType.FILL_RECORDED,
        PaperEventType.ORDER_FILLED,
        PaperEventType.POSITION_CHANGED,
    ]
    assert result.positions == broker.list_positions()
    position = result.positions[0]
    assert position.net_quantity == Decimal("1")
    assert position.average_open_price == result.fills[0].execution_price
    assert broker.get_position("strategy", "paper", position.instrument_id) == position
    assert result.events[2].payload == position
    assert all(
        event.source_session_id == result.source_session_id
        and event.source_ingest_sequence == result.source_ingest_sequence
        for event in result.events
    )
    assert (
        result.fills[0].execution_config_fingerprint
        == broker.snapshot().execution_config_fingerprint
    )


def test_slippage_fee_audit_and_duplicate_are_deterministic() -> None:
    instrument = make_offline_fixture_envelopes()[2].payload
    assert isinstance(instrument, Instrument)
    config = _execution_config(
        slippage_mode=SlippageMode.BASIS_POINTS,
        slippage_value="10",
        fee_instrument_id=instrument.instrument_id,
    )
    broker = _broker(execution_config=config)
    _, quote = _prime(broker)
    broker.submit(_intent("priced", quantity="2"))

    result = broker.process_market_data(quote)
    fill = result.fills[0]
    assert fill.reference_price == Decimal("20002.00")
    assert fill.execution_price == Decimal("20022.002")
    assert fill.slippage_amount == Decimal("20.002")
    assert fill.fee == Decimal("1.20")
    assert fill.fee_currency == "TWD"
    assert result.positions[0].cumulative_fees == Decimal("1.20")
    before_duplicate = broker.snapshot()
    duplicate = broker.process_market_data(quote)
    assert duplicate.disposition is MatchDisposition.DUPLICATE
    assert duplicate.fills == duplicate.events == duplicate.positions == ()
    assert broker.snapshot() == before_duplicate

    repeat = _broker(execution_config=config)
    _, repeat_quote = _prime(repeat)
    repeat.submit(_intent("priced", quantity="2"))
    repeat.process_market_data(repeat_quote)
    assert canonical_json(repeat.snapshot()) == canonical_json(broker.snapshot())

    different = _broker(
        execution_config=_execution_config(
            slippage_mode=SlippageMode.BASIS_POINTS,
            slippage_value="11",
        )
    )
    _, different_quote = _prime(different)
    different.submit(_intent("priced", quantity="2"))
    different_fill = different.process_market_data(different_quote).fills[0]
    assert different_fill.paper_fill_id != fill.paper_fill_id
    assert different_fill.execution_config_fingerprint != fill.execution_config_fingerprint


def test_post_slippage_limit_rejection_does_not_debit_fifo_liquidity() -> None:
    broker = _broker(
        execution_config=_execution_config(
            slippage_mode=SlippageMode.ABSOLUTE,
            slippage_value="2",
        )
    )
    _, quote = _prime(broker)
    skipped = broker.submit(
        _intent(
            "slipped-limit",
            order_type=OrderType.LIMIT,
            limit_price="20003",
        )
    )
    filled = broker.submit(_intent("next-market"))

    result = broker.process_market_data(_shift_quote(quote, 3, ask_qty=1))

    assert result.skip_reasons == (MatchSkipReason.SLIPPAGE_EXCEEDS_LIMIT,)
    assert [fill.paper_order_id for fill in result.fills] == [filled.paper_order_id]
    assert broker.get_order(skipped.paper_order_id).status is OrderStatus.ACCEPTED


@pytest.mark.parametrize(
    ("currency", "rule_instrument", "expected_reason"),
    [
        (None, "TAIFEX:0:TX00", MatchSkipReason.METADATA_UNAVAILABLE),
        ("USD", "TAIFEX:0:TX00", MatchSkipReason.METADATA_MISMATCH),
        ("TWD", "TAIFEX:0:MXF", MatchSkipReason.METADATA_UNAVAILABLE),
    ],
)
def test_nonzero_fee_metadata_and_rule_fail_closed(
    currency: str | None,
    rule_instrument: str,
    expected_reason: MatchSkipReason,
) -> None:
    broker = _broker(execution_config=_execution_config(fee_instrument_id=rule_instrument))
    instrument_envelope = make_offline_fixture_envelopes()[2]
    instrument = instrument_envelope.payload
    assert isinstance(instrument, Instrument)
    broker.process_market_data(
        replace(instrument_envelope, payload=replace(instrument, currency=currency))
    )
    quote = make_offline_fixture_envelopes()[3]
    broker.submit(_intent("fee-fail"))
    before = broker.snapshot()

    result = broker.process_market_data(quote)

    assert not result.fills
    assert result.skip_reasons == (expected_reason,)
    assert broker.list_positions() == ()
    assert broker.get_order(broker.list_orders()[0].paper_order_id).status is OrderStatus.ACCEPTED
    assert broker.snapshot().last_committed_ingest_sequence != before.last_committed_ingest_sequence


def test_position_ledger_is_keyed_versioned_and_sorted() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    broker.submit(_intent("long", quantity="3", strategy_id="z", account_id="b"))
    first = broker.process_market_data(_shift_quote(quote, 3, ask_qty=3))
    assert first.positions[0].net_quantity == Decimal("3")
    assert first.positions[0].version == 1

    broker.submit(
        _intent(
            "reverse",
            side=OrderSide.SELL,
            quantity="5",
            strategy_id="z",
            account_id="b",
        )
    )
    second = broker.process_market_data(_shift_quote(quote, 4, bid_qty=5, seconds=1))
    assert second.positions[0].net_quantity == Decimal("-2")
    assert second.positions[0].average_open_price == Decimal("20000.00")
    assert second.positions[0].version == 2

    broker.submit(_intent("other", strategy_id="a", account_id="a"))
    third = broker.process_market_data(_shift_quote(quote, 5, ask_qty=1, seconds=2))
    assert third.positions[0].version == 1
    assert [
        (position.strategy_id, position.account_id, position.instrument_id)
        for position in broker.list_positions()
    ] == [
        ("a", "a", "TAIFEX:0:TX00"),
        ("z", "b", "TAIFEX:0:TX00"),
    ]


@pytest.mark.parametrize(
    "limited_field",
    ["max_positions", "max_events"],
)
def test_position_and_three_event_capacity_overflow_are_atomic(
    limited_field: str,
) -> None:
    limits = replace(
        LIMITS,
        max_positions=1 if limited_field == "max_positions" else LIMITS.max_positions,
        max_events=4 if limited_field == "max_events" else LIMITS.max_events,
    )
    broker = _broker(limits)
    _, quote = _prime(broker)
    broker.submit(_intent("one", strategy_id="one"))
    broker.submit(_intent("two", strategy_id="two"))
    before = broker.snapshot()

    with pytest.raises(PaperBrokerCapacityError):
        broker.process_market_data(_shift_quote(quote, 3, ask_qty=2))

    assert broker.snapshot() == before


def test_unexpected_execution_policy_failure_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _broker()
    _, quote = _prime(broker)
    broker.submit(_intent("policy-error"))
    before = broker.snapshot()

    def fail(*args: object, **kwargs: object) -> object:
        raise ArithmeticError("injected")

    monkeypatch.setattr("tx_trade.orders.paper_broker.assess_fee", fail)
    with pytest.raises(ArithmeticError, match="injected"):
        broker.process_market_data(quote)
    assert broker.snapshot() == before


def test_typed_fee_arithmetic_failure_on_second_fifo_fill_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _broker()
    _, quote = _prime(broker)
    broker.submit(_intent("first"))
    broker.submit(_intent("second"))
    before = broker.snapshot()
    calls = 0

    def fail_second(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ExecutionPolicyError(ExecutionPolicyErrorCode.ARITHMETIC_FAILURE)
        return assess_fee(*args, **kwargs)

    monkeypatch.setattr("tx_trade.orders.paper_broker.assess_fee", fail_second)
    with pytest.raises(ExecutionPolicyError) as raised:
        broker.process_market_data(_shift_quote(quote, 3, ask_qty=2))

    assert raised.value.code is ExecutionPolicyErrorCode.ARITHMETIC_FAILURE
    assert calls == 2
    assert broker.snapshot() == before


def test_temporal_position_order_skips_only_that_order_and_does_not_debit() -> None:
    broker = _broker()
    _, quote = _prime(broker)
    broker.submit(_intent("initial"))
    broker.process_market_data(_shift_quote(quote, 3, ask_qty=1, seconds=10))
    previous = broker.get_position("strategy", "paper", "TAIFEX:0:TX00")
    assert previous is not None

    stale = broker.submit(_intent("stale-position"))
    following = broker.submit(_intent("following", strategy_id="other"))
    result = broker.process_market_data(_shift_quote(quote, 4, ask_qty=1, seconds=5))

    assert result.skip_reasons == (MatchSkipReason.ORDER_NOT_ELIGIBLE,)
    assert [fill.paper_order_id for fill in result.fills] == [following.paper_order_id]
    assert broker.get_order(stale.paper_order_id).status is OrderStatus.ACCEPTED
    assert broker.get_position("strategy", "paper", "TAIFEX:0:TX00") == previous
    other = broker.get_position("other", "paper", "TAIFEX:0:TX00")
    assert other is not None
    assert other.version == 1


def test_extreme_fee_arithmetic_failure_rolls_back_exactly() -> None:
    instrument = make_offline_fixture_envelopes()[2].payload
    assert isinstance(instrument, Instrument)
    broker = _broker(
        execution_config=_execution_config(
            fee_instrument_id=instrument.instrument_id,
            fee_per_unit="1e6144",
        )
    )
    _, quote = _prime(broker)
    broker.submit(_intent("extreme-fee", quantity="10"))
    before = broker.snapshot()

    with pytest.raises(ExecutionPolicyError) as raised:
        broker.process_market_data(_shift_quote(quote, 3, ask_qty=10))

    assert raised.value.code is ExecutionPolicyErrorCode.ARITHMETIC_FAILURE
    assert broker.snapshot() == before


def test_zero_incremental_fee_updates_large_fee_position_normally() -> None:
    instrument = make_offline_fixture_envelopes()[2].payload
    assert isinstance(instrument, Instrument)
    broker = _broker(
        execution_config=_execution_config(
            fee_instrument_id=instrument.instrument_id,
            fee_per_unit="1",
            fee_quantum="1e34",
        )
    )
    _, quote = _prime(broker)
    broker.submit(_intent("large-fee-base"))
    base_position = broker.process_market_data(
        _shift_quote(quote, 3, ask_qty=1),
    ).positions[0]
    key = ("strategy", "paper", instrument.instrument_id)
    # Exact ledger arithmetic deliberately cannot build 1e34 from zero: aligning
    # Decimal(0) to that exponent signals Rounded. Seed only this boundary state
    # so the broker integration exercises the zero-increment path under review.
    base_position = replace(
        base_position,
        cumulative_fees=Decimal("1e34"),
        fee_currency="TWD",
    )
    broker._state = replace(
        broker._state,
        positions=MappingProxyType({key: base_position}),
    )
    assert base_position.net_quantity == Decimal("1")
    assert base_position.cumulative_fees == Decimal("1e34")
    assert base_position.fee_currency == "TWD"
    assert base_position.version == 1

    broker.submit(_intent("zero-fee-increment", quantity="0.1"))
    second = broker.process_market_data(
        _shift_quote(quote, 4, ask_qty=1, seconds=1),
    )

    assert second.fills[0].fee == Decimal(0)
    assert second.fills[0].fee_currency is None
    assert [event.event_type for event in second.events] == [
        PaperEventType.FILL_RECORDED,
        PaperEventType.ORDER_FILLED,
        PaperEventType.POSITION_CHANGED,
    ]
    position = second.positions[0]
    assert position.net_quantity == Decimal("1.1")
    assert position.version == 2
    assert position.cumulative_fees == Decimal("1e34")
    assert position.fee_currency == "TWD"
    assert broker.get_position("strategy", "paper", instrument.instrument_id) == position

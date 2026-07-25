from tx_trade.market_data.ports import IngressDecision
from tx_trade.monitoring.metrics import IngressLane, IngressMetrics


def test_metrics_accounting_and_immutable_snapshot():
    metrics = IngressMetrics({IngressLane.TICK: 2})
    for decision in IngressDecision:
        metrics.record_result(
            IngressLane.TICK,
            decision,
            sequence=8 if decision is IngressDecision.DROPPED else None,
            overflow=decision is IngressDecision.DROPPED,
        )
    metrics.update_depth(IngressLane.TICK, 1)
    snapshot = metrics.snapshot()
    assert snapshot.received[IngressLane.TICK] == sum(
        mapping[IngressLane.TICK]
        for mapping in (
            snapshot.accepted,
            snapshot.coalesced,
            snapshot.dropped,
            snapshot.duplicates,
        )
    )
    assert snapshot.first_dropped_tick_sequence == 8
    assert snapshot.last_dropped_tick_sequence == 8
    assert snapshot.overflow[IngressLane.TICK] == 1
    try:
        snapshot.received[IngressLane.TICK] = 0
    except TypeError:
        pass
    else:
        raise AssertionError("snapshot mappings must be immutable")


def test_concurrent_snapshots_always_observe_accounting_invariant():
    metrics = IngressMetrics({IngressLane.TICK: 1})
    start = threading.Barrier(2)
    done = threading.Event()
    errors = []

    def produce():
        start.wait()
        for sequence in range(10_000):
            metrics.record_result(
                IngressLane.TICK,
                IngressDecision.DROPPED,
                sequence=sequence,
                overflow=True,
                depth=0,
            )
        done.set()

    thread = threading.Thread(target=produce)
    thread.start()
    start.wait()
    while not done.is_set():
        snapshot = metrics.snapshot()
        received = snapshot.received[IngressLane.TICK]
        outcomes = sum(
            values[IngressLane.TICK]
            for values in (
                snapshot.accepted,
                snapshot.coalesced,
                snapshot.dropped,
                snapshot.duplicates,
            )
        )
        if received != outcomes:
            errors.append((received, outcomes))
            break
    thread.join()
    assert not errors
import threading

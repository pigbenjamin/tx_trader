from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest

from tx_trade.market_data.sequencer import IngestSequencer

SESSION_A = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
SESSION_B = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def test_sequence_is_session_global_and_sessions_are_isolated():
    sequencer = IngestSequencer()
    assert [sequencer.next(SESSION_A) for _ in range(3)] == [0, 1, 2]
    # Connection generation is intentionally not an input and cannot reset it.
    assert sequencer.next(SESSION_A) == 3
    assert sequencer.next(SESSION_B) == 0
    assert sequencer.peek_last(SESSION_A) == 3
    assert sequencer.peek_last(UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")) is None


def test_concurrent_allocation_is_unique_and_contiguous():
    sequencer = IngestSequencer()
    count = 2000
    with ThreadPoolExecutor(max_workers=16) as pool:
        values = list(pool.map(lambda _: sequencer.next(SESSION_A), range(count)))
    assert sorted(values) == list(range(count))
    assert sequencer.peek_last(SESSION_A) == count - 1


def test_rejects_non_uuid_and_signed_64_bit_overflow():
    sequencer = IngestSequencer()
    with pytest.raises(TypeError):
        sequencer.next(str(SESSION_A))
    sequencer._last_by_session[SESSION_A] = (1 << 63) - 1
    with pytest.raises(OverflowError):
        sequencer.next(SESSION_A)

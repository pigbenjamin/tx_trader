from uuid import UUID

from tx_trade.app.phase1 import Phase1Dependencies, run_phase1
from tx_trade.storage import SQLiteMarketDataRepository, SQLiteReplaySource


def test_default_offline_app_records_and_verifies_complete_session(tmp_path):
    db_path = tmp_path / "phase1-offline.db"
    result = run_phase1({}, db_path=str(db_path))
    assert result.mode == "offline"
    assert result.status == "complete"
    assert result.integrity_valid is True
    assert result.event_count == 6

    repository = SQLiteMarketDataRepository(db_path)
    session = repository.get_session(result.session_id)
    assert session is not None
    assert session.status == "complete"
    replay = SQLiteReplaySource(repository)
    replay.open(result.session_id)
    assert replay.verify_integrity().is_valid
    assert len(tuple(replay.iter_events())) == 6
    repository.close()


def test_two_offline_runs_share_database_with_distinct_injected_sessions(tmp_path):
    db_path = tmp_path / "phase1-repeat.db"
    session_ids = iter(
        (
            UUID("11111111-1111-1111-1111-111111111111"),
            UUID("22222222-2222-2222-2222-222222222222"),
        )
    )
    dependencies = Phase1Dependencies(session_id_factory=lambda: next(session_ids))
    first = run_phase1({}, db_path=str(db_path), dependencies=dependencies)
    second = run_phase1({}, db_path=str(db_path), dependencies=dependencies)

    assert first.session_id != second.session_id
    assert first.event_count == second.event_count == 6
    assert first.status == second.status == "complete"

    repository = SQLiteMarketDataRepository(db_path)
    for expected in (first, second):
        session = repository.get_session(expected.session_id)
        assert session is not None
        assert session.status == "complete"
        replay = SQLiteReplaySource(repository)
        replay.open(expected.session_id)
        events = tuple(replay.iter_events())
        assert len(events) == 6
        assert {event.session_id for event in events} == {expected.session_id}
        assert replay.verify_integrity().is_valid
    repository.close()

PRAGMA application_id = 1415074898;
PRAGMA user_version = 1;

BEGIN EXCLUSIVE;

CREATE TABLE paper_schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL UNIQUE
);

INSERT INTO paper_schema_migrations(version, checksum)
VALUES (1, '__SCHEMA_CHECKSUM__');

CREATE TABLE research_runs (
    paper_run_id TEXT PRIMARY KEY,
    source_session_id TEXT NOT NULL,
    source_schema_version INTEGER NOT NULL CHECK (source_schema_version >= 1),
    source_event_count INTEGER NOT NULL CHECK (source_event_count >= 1),
    source_first_sequence INTEGER NOT NULL CHECK (source_first_sequence >= 0),
    source_last_sequence INTEGER NOT NULL CHECK (
        source_last_sequence >= source_first_sequence
    ),
    source_content_fingerprint TEXT NOT NULL,
    research_config_fingerprint TEXT NOT NULL,
    execution_config_fingerprint TEXT NOT NULL,
    strategy_fingerprints_json TEXT NOT NULL,
    output_schema_version INTEGER NOT NULL CHECK (output_schema_version >= 1),
    broker_algorithm_version TEXT NOT NULL,
    identity_fingerprint TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('active', 'complete', 'failed')),
    state_version INTEGER NOT NULL CHECK (state_version >= 0),
    committed_cursor INTEGER,
    committed_batch_count INTEGER NOT NULL CHECK (committed_batch_count >= 0),
    broker_checkpoint_schema_version INTEGER NOT NULL CHECK (
        broker_checkpoint_schema_version >= 1
    ),
    broker_checkpoint BLOB NOT NULL,
    broker_checkpoint_sha256 TEXT NOT NULL,
    coordinator_checkpoint_schema_version INTEGER NOT NULL CHECK (
        coordinator_checkpoint_schema_version >= 1
    ),
    coordinator_checkpoint BLOB NOT NULL,
    coordinator_checkpoint_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    CHECK (
        source_event_count
        <= source_last_sequence - source_first_sequence + 1
    ),
    CHECK (
        (committed_cursor IS NULL AND committed_batch_count = 0)
        OR (
            committed_cursor BETWEEN source_first_sequence AND source_last_sequence
            AND committed_batch_count >= 1
        )
    ),
    CHECK (
        (status = 'complete'
            AND completed_at IS NOT NULL
            AND committed_cursor = source_last_sequence
            AND committed_batch_count = source_event_count)
        OR (status != 'complete' AND completed_at IS NULL)
    ),
    CHECK (
        length(source_content_fingerprint) = 71
        AND substr(source_content_fingerprint, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(research_config_fingerprint) = 71
        AND substr(research_config_fingerprint, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(execution_config_fingerprint) = 71
        AND substr(execution_config_fingerprint, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(identity_fingerprint) = 71
        AND substr(identity_fingerprint, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(broker_checkpoint_sha256) = 71
        AND substr(broker_checkpoint_sha256, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(coordinator_checkpoint_sha256) = 71
        AND substr(coordinator_checkpoint_sha256, 1, 7) = 'sha256:'
    ),
    CHECK (typeof(broker_checkpoint) = 'blob'),
    CHECK (typeof(coordinator_checkpoint) = 'blob')
);

CREATE TABLE research_batches (
    paper_run_id TEXT NOT NULL REFERENCES research_runs(paper_run_id),
    source_session_id TEXT NOT NULL,
    source_ingest_sequence INTEGER NOT NULL CHECK (source_ingest_sequence >= 0),
    envelope_fingerprint TEXT NOT NULL,
    decision_fingerprint TEXT NOT NULL,
    batch_fingerprint TEXT NOT NULL,
    applied_state_version INTEGER NOT NULL CHECK (applied_state_version >= 1),
    committed_at TEXT NOT NULL,
    PRIMARY KEY (paper_run_id, source_ingest_sequence),
    UNIQUE (paper_run_id, batch_fingerprint),
    CHECK (
        length(envelope_fingerprint) = 71
        AND substr(envelope_fingerprint, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(decision_fingerprint) = 71
        AND substr(decision_fingerprint, 1, 7) = 'sha256:'
    ),
    CHECK (
        length(batch_fingerprint) = 71
        AND substr(batch_fingerprint, 1, 7) = 'sha256:'
    )
);

CREATE INDEX idx_research_batches_state_version
    ON research_batches(paper_run_id, applied_state_version);

CREATE TABLE research_outbox (
    paper_run_id TEXT NOT NULL REFERENCES research_runs(paper_run_id),
    output_sequence INTEGER NOT NULL CHECK (output_sequence >= 0),
    record_type TEXT NOT NULL CHECK (record_type IN ('market', 'paper', 'summary')),
    source_ingest_sequence INTEGER,
    paper_sequence INTEGER,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    payload_bytes INTEGER NOT NULL CHECK (payload_bytes >= 1),
    created_state_version INTEGER NOT NULL CHECK (created_state_version >= 1),
    PRIMARY KEY (paper_run_id, output_sequence),
    CHECK (typeof(payload) = 'blob'),
    CHECK (length(payload) = payload_bytes),
    CHECK (
        length(payload_sha256) = 71
        AND substr(payload_sha256, 1, 7) = 'sha256:'
    ),
    CHECK (
        (record_type = 'market'
            AND source_ingest_sequence IS NOT NULL
            AND paper_sequence IS NULL)
        OR (record_type = 'paper'
            AND source_ingest_sequence IS NOT NULL
            AND paper_sequence IS NOT NULL)
        OR (record_type = 'summary'
            AND source_ingest_sequence IS NULL
            AND paper_sequence IS NULL)
    )
);

CREATE UNIQUE INDEX idx_research_outbox_market_source
    ON research_outbox(paper_run_id, source_ingest_sequence)
    WHERE record_type = 'market';

CREATE UNIQUE INDEX idx_research_outbox_paper_sequence
    ON research_outbox(paper_run_id, paper_sequence)
    WHERE record_type = 'paper';

CREATE UNIQUE INDEX idx_research_outbox_single_summary
    ON research_outbox(paper_run_id)
    WHERE record_type = 'summary';

COMMIT;

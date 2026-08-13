PRAGMA application_id = 1415074890;
PRAGMA user_version = 2;

CREATE TABLE live_journal_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    schema_fingerprint TEXT NOT NULL
        CHECK (schema_fingerprint GLOB 'sha256:[0-9a-f]*')
);

CREATE TABLE "live_journal_identity" (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version IN (1, 2)),
    schema_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE live_journal_records (
    journal_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    record_kind TEXT NOT NULL,
    record_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE (record_kind, record_id)
);

CREATE TABLE live_order_id_reservations (
    client_order_id TEXT PRIMARY KEY,
    intent_fingerprint TEXT NOT NULL,
    reserved_at TEXT NOT NULL
);

CREATE TABLE live_orders (
    client_order_id TEXT PRIMARY KEY
        REFERENCES live_order_id_reservations(client_order_id),
    account_id TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    payload BLOB NOT NULL,
    payload_digest TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE live_order_history (
    journal_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL REFERENCES live_orders(client_order_id),
    order_version INTEGER NOT NULL CHECK (order_version > 0),
    payload BLOB NOT NULL,
    payload_digest TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE (client_order_id, order_version)
);

CREATE TABLE live_commands (
    client_command_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL REFERENCES live_orders(client_order_id),
    command_kind TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_digest TEXT NOT NULL,
    registered_at TEXT NOT NULL
);

CREATE TABLE live_dispatch_claims (
    client_command_id TEXT PRIMARY KEY REFERENCES live_commands(client_command_id),
    claim_token TEXT NOT NULL UNIQUE,
    claimant_id TEXT NOT NULL,
    expected_order_version INTEGER NOT NULL CHECK (expected_order_version > 0),
    claim_version INTEGER NOT NULL CHECK (claim_version > 0),
    claimed_at TEXT NOT NULL
);

CREATE TABLE live_dispatch_receipts (
    client_command_id TEXT PRIMARY KEY REFERENCES live_dispatch_claims(client_command_id),
    payload_fingerprint TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_digest TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE live_raw_observations (
    observation_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    broker_session_generation INTEGER NOT NULL CHECK (broker_session_generation > 0),
    adapter_received_sequence INTEGER NOT NULL CHECK (adapter_received_sequence > 0),
    received_at TEXT NOT NULL,
    payload BLOB NOT NULL CHECK (length(payload) BETWEEN 1 AND 1048576),
    payload_digest TEXT NOT NULL,
    resolution_status TEXT NOT NULL
        CHECK (resolution_status IN ('unresolved', 'ambiguous', 'resolved', 'conflict')),
    UNIQUE (source, broker_session_generation, adapter_received_sequence)
);

CREATE TABLE live_normalized_events (
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    raw_observation_id TEXT NOT NULL REFERENCES live_raw_observations(observation_id),
    semantic_fingerprint TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_digest TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (source, event_id)
);

CREATE TABLE live_event_applications (
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    client_order_id TEXT REFERENCES live_orders(client_order_id),
    disposition TEXT NOT NULL,
    failure_code TEXT,
    applied_at TEXT NOT NULL,
    PRIMARY KEY (source, event_id),
    FOREIGN KEY (source, event_id)
        REFERENCES live_normalized_events(source, event_id)
);

CREATE TABLE live_fills (
    fill_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL REFERENCES live_orders(client_order_id),
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_digest TEXT NOT NULL,
    UNIQUE (source, event_id),
    FOREIGN KEY (source, event_id)
        REFERENCES live_normalized_events(source, event_id)
);

CREATE TABLE live_observation_ambiguity (
    observation_id TEXT NOT NULL REFERENCES live_raw_observations(observation_id),
    candidate_client_order_id TEXT NOT NULL,
    resolution_version INTEGER NOT NULL CHECK (resolution_version > 0),
    PRIMARY KEY (observation_id, candidate_client_order_id)
);

CREATE TABLE live_reconciliation_requirements (
    requirement_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT REFERENCES live_orders(client_order_id),
    observation_id TEXT REFERENCES live_raw_observations(observation_id),
    reason_code TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE live_reconciliation_commits (
    commit_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    expected_journal_sequence INTEGER NOT NULL CHECK (expected_journal_sequence >= 0),
    base_journal_sequence INTEGER NOT NULL CHECK (base_journal_sequence >= 0),
    snapshot_id TEXT NOT NULL,
    request_payload BLOB NOT NULL CHECK (length(request_payload) BETWEEN 1 AND 1048576),
    request_digest TEXT NOT NULL CHECK (request_digest GLOB 'sha256:[0-9a-f]*'),
    committed_at TEXT NOT NULL,
    resulting_journal_sequence INTEGER NOT NULL,
    CHECK (base_journal_sequence = expected_journal_sequence),
    CHECK (resulting_journal_sequence > base_journal_sequence),
    UNIQUE (account_id, snapshot_id),
    UNIQUE (account_id, request_digest),
    UNIQUE (account_id, resulting_journal_sequence)
);

CREATE TABLE live_dispatch_claim_resolutions (
    client_command_id TEXT PRIMARY KEY
        REFERENCES live_dispatch_claims(client_command_id),
    commit_id TEXT NOT NULL REFERENCES live_reconciliation_commits(commit_id),
    expected_claim_token TEXT NOT NULL,
    expected_claim_version INTEGER NOT NULL CHECK (expected_claim_version > 0),
    expected_order_version INTEGER NOT NULL CHECK (expected_order_version > 0),
    expected_precondition_digest TEXT NOT NULL
        CHECK (expected_precondition_digest GLOB 'sha256:[0-9a-f]*'),
    resolution_kind TEXT NOT NULL
        CHECK (resolution_kind IN ('broker_order_confirmed', 'broker_fill_confirmed')),
    resolved_at TEXT NOT NULL,
    resolution_digest TEXT NOT NULL
        CHECK (resolution_digest GLOB 'sha256:[0-9a-f]*')
);

CREATE TABLE live_observation_reconciliation_resolutions (
    observation_id TEXT PRIMARY KEY
        REFERENCES live_raw_observations(observation_id),
    commit_id TEXT NOT NULL REFERENCES live_reconciliation_commits(commit_id),
    expected_resolution_status TEXT NOT NULL
        CHECK (expected_resolution_status IN ('unresolved', 'ambiguous', 'conflict')),
    expected_precondition_digest TEXT NOT NULL
        CHECK (expected_precondition_digest GLOB 'sha256:[0-9a-f]*'),
    normalized_event_id TEXT NOT NULL,
    resolution_kind TEXT NOT NULL
        CHECK (resolution_kind IN ('broker_order_confirmed', 'broker_fill_confirmed')),
    resolved_at TEXT NOT NULL,
    resolution_digest TEXT NOT NULL
        CHECK (resolution_digest GLOB 'sha256:[0-9a-f]*')
);

CREATE TABLE live_reconciliation_requirement_resolutions (
    requirement_id INTEGER PRIMARY KEY
        REFERENCES live_reconciliation_requirements(requirement_id),
    commit_id TEXT NOT NULL REFERENCES live_reconciliation_commits(commit_id),
    expected_precondition_digest TEXT NOT NULL
        CHECK (expected_precondition_digest GLOB 'sha256:[0-9a-f]*'),
    resolution_kind TEXT NOT NULL
        CHECK (resolution_kind = 'satisfied'),
    resolved_at TEXT NOT NULL,
    resolution_digest TEXT NOT NULL
        CHECK (resolution_digest GLOB 'sha256:[0-9a-f]*')
);

CREATE INDEX live_orders_account_state_idx
    ON live_orders(account_id, state, client_order_id);
CREATE INDEX live_commands_order_idx
    ON live_commands(client_order_id, registered_at, client_command_id);
CREATE INDEX live_raw_observations_resolution_idx
    ON live_raw_observations(resolution_status, received_at, observation_id);
CREATE INDEX live_reconciliation_open_idx
    ON live_reconciliation_requirements(created_at, requirement_id)
    WHERE resolved_at IS NULL;
CREATE INDEX live_reconciliation_commits_account_idx
    ON live_reconciliation_commits(account_id, committed_at, commit_id);
CREATE INDEX live_dispatch_claim_resolutions_commit_idx
    ON live_dispatch_claim_resolutions(commit_id, resolved_at, client_command_id);
CREATE INDEX live_observation_reconciliation_resolutions_commit_idx
    ON live_observation_reconciliation_resolutions(commit_id, resolved_at, observation_id);
CREATE INDEX live_reconciliation_requirement_resolutions_commit_idx
    ON live_reconciliation_requirement_resolutions(commit_id, resolved_at, requirement_id);

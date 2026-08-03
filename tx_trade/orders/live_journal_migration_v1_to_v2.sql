CREATE TABLE live_journal_identity_v2 (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version IN (1, 2)),
    schema_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL
);

INSERT INTO live_journal_identity_v2(
    singleton, journal_id, schema_version, schema_fingerprint, created_at
)
SELECT singleton, journal_id, schema_version, schema_fingerprint, created_at
FROM live_journal_identity;

DROP TABLE live_journal_identity;
ALTER TABLE live_journal_identity_v2 RENAME TO live_journal_identity;

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

CREATE INDEX live_reconciliation_commits_account_idx
    ON live_reconciliation_commits(account_id, committed_at, commit_id);
CREATE INDEX live_dispatch_claim_resolutions_commit_idx
    ON live_dispatch_claim_resolutions(commit_id, resolved_at, client_command_id);
CREATE INDEX live_observation_reconciliation_resolutions_commit_idx
    ON live_observation_reconciliation_resolutions(commit_id, resolved_at, observation_id);
CREATE INDEX live_reconciliation_requirement_resolutions_commit_idx
    ON live_reconciliation_requirement_resolutions(commit_id, resolved_at, requirement_id);

INSERT INTO live_journal_migrations(version, schema_fingerprint)
VALUES (2, 'sha256:d9c6c23fdce811b9a85efafa8eadd6083842c0d1d9007c33943d028a6d103b3b');

PRAGMA user_version = 2;

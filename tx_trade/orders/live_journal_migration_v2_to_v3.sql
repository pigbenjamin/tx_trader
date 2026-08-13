CREATE TABLE live_journal_identity_v3 (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version IN (1, 2, 3)),
    schema_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL
);

INSERT INTO live_journal_identity_v3(
    singleton, journal_id, schema_version, schema_fingerprint, created_at
)
SELECT singleton, journal_id, schema_version, schema_fingerprint, created_at
FROM live_journal_identity;

DROP TABLE live_journal_identity;
ALTER TABLE live_journal_identity_v3 RENAME TO live_journal_identity;

CREATE TABLE live_reconciliation_commit_authorizations (
    authorization_id TEXT PRIMARY KEY,
    commit_id TEXT NOT NULL UNIQUE
        REFERENCES live_reconciliation_commits(commit_id),
    journal_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    action_kind TEXT NOT NULL CHECK (action_kind = 'reconciliation_commit'),
    principal_id TEXT NOT NULL,
    authority_context_digest TEXT NOT NULL
        CHECK (
            length(authority_context_digest) = 71
            AND substr(authority_context_digest, 1, 7) = 'sha256:'
            AND substr(authority_context_digest, 8) NOT GLOB '*[^0-9a-f]*'
        ),
    source_inspection_digest TEXT NOT NULL
        CHECK (
            length(source_inspection_digest) = 71
            AND substr(source_inspection_digest, 1, 7) = 'sha256:'
            AND substr(source_inspection_digest, 8) NOT GLOB '*[^0-9a-f]*'
        ),
    operator_plan_digest TEXT NOT NULL
        CHECK (
            length(operator_plan_digest) = 71
            AND substr(operator_plan_digest, 1, 7) = 'sha256:'
            AND substr(operator_plan_digest, 8) NOT GLOB '*[^0-9a-f]*'
        ),
    request_digest TEXT NOT NULL
        CHECK (
            length(request_digest) = 71
            AND substr(request_digest, 1, 7) = 'sha256:'
            AND substr(request_digest, 8) NOT GLOB '*[^0-9a-f]*'
        ),
    broker_snapshot_id TEXT NOT NULL,
    expected_journal_sequence INTEGER NOT NULL
        CHECK (expected_journal_sequence >= 0),
    authorized_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    authorization_digest TEXT NOT NULL
        CHECK (
            length(authorization_digest) = 71
            AND substr(authorization_digest, 1, 7) = 'sha256:'
            AND substr(authorization_digest, 8) NOT GLOB '*[^0-9a-f]*'
        ),
    resulting_journal_sequence INTEGER NOT NULL
        CHECK (resulting_journal_sequence > expected_journal_sequence)
);

CREATE INDEX live_reconciliation_commit_authorizations_account_idx
    ON live_reconciliation_commit_authorizations(
        account_id, consumed_at, authorization_id
    );
CREATE INDEX live_reconciliation_commit_authorizations_principal_idx
    ON live_reconciliation_commit_authorizations(
        principal_id, consumed_at, authorization_id
    );

CREATE TRIGGER live_reconciliation_commit_authorizations_no_update
BEFORE UPDATE ON live_reconciliation_commit_authorizations
BEGIN
    SELECT RAISE(ABORT, 'append-only authorization audit');
END;

CREATE TRIGGER live_reconciliation_commit_authorizations_no_delete
BEFORE DELETE ON live_reconciliation_commit_authorizations
BEGIN
    SELECT RAISE(ABORT, 'append-only authorization audit');
END;

INSERT INTO live_journal_migrations(version, schema_fingerprint)
VALUES (3, 'sha256:9150866af5822cc4bfb4e889791e82bac84fac59fce321c7667897eed223b761');

PRAGMA user_version = 3;

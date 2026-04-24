CREATE TABLE /* TEMPLATE: schema */river_job(
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(20) NOT NULL DEFAULT 'available',
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL,
    attempted_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    finalized_at DATETIME(6) NULL,
    scheduled_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    priority SMALLINT NOT NULL DEFAULT 1,
    args JSON NULL,
    attempted_by JSON NULL, -- JSON array of strings (no native text[] in MySQL)
    errors JSON NULL, -- JSON array of error objects (no native jsonb[] in MySQL)
    kind VARCHAR(128) NOT NULL,
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    queue VARCHAR(128) NOT NULL DEFAULT 'default',
    tags JSON NULL, -- JSON array of strings (no native varchar[] in MySQL)
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR
        finalized_at IS NULL
    ),
    CONSTRAINT max_attempts_is_positive CHECK (max_attempts > 0),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (CHAR_LENGTH(queue) > 0 AND CHAR_LENGTH(queue) < 128),
    CONSTRAINT kind_length CHECK (CHAR_LENGTH(kind) > 0 AND CHAR_LENGTH(kind) < 128),
    CONSTRAINT state_valid CHECK (state IN ('available', 'cancelled', 'completed', 'discarded', 'retryable', 'running', 'scheduled'))
) ENGINE=InnoDB;

CREATE INDEX river_job_kind ON /* TEMPLATE: schema */river_job (kind);
CREATE INDEX river_job_state_and_finalized_at_index ON /* TEMPLATE: schema */river_job (state, finalized_at);
CREATE INDEX river_job_prioritized_fetching_index ON /* TEMPLATE: schema */river_job (state, queue, priority, scheduled_at, id);

-- MySQL does not support triggers for LISTEN/NOTIFY, so river_job_notify is
-- omitted. The MySQL driver operates in poll-only mode.

CREATE TABLE /* TEMPLATE: schema */river_leader (
    elected_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    leader_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    CONSTRAINT name_length CHECK (CHAR_LENGTH(name) > 0 AND CHAR_LENGTH(name) < 128),
    CONSTRAINT leader_id_length CHECK (CHAR_LENGTH(leader_id) > 0 AND CHAR_LENGTH(leader_id) < 128)
) ENGINE=InnoDB;

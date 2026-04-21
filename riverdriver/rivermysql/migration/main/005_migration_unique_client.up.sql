--
-- Rebuild the migration table so it's based on `(line, version)`.
--

DROP INDEX river_migration_version_idx ON /* TEMPLATE: schema */river_migration;

CREATE TABLE /* TEMPLATE: schema */river_migration_new (
    line VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    CONSTRAINT line_length CHECK (CHAR_LENGTH(line) > 0 AND CHAR_LENGTH(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1),
    PRIMARY KEY (line, version)
) ENGINE=InnoDB;

INSERT INTO /* TEMPLATE: schema */river_migration_new
    (created_at, line, version)
SELECT created_at, 'main', version
FROM /* TEMPLATE: schema */river_migration;

DROP TABLE /* TEMPLATE: schema */river_migration;

ALTER TABLE /* TEMPLATE: schema */river_migration_new RENAME TO /* TEMPLATE: schema */river_migration;

--
-- Add `river_job.unique_key` and bring up an index on it.
--

ALTER TABLE /* TEMPLATE: schema */river_job ADD COLUMN unique_key VARBINARY(255) NULL;

CREATE UNIQUE INDEX river_job_kind_unique_key_idx ON /* TEMPLATE: schema */river_job (kind, unique_key);

--
-- Create `river_client` and derivative.
--

CREATE TABLE /* TEMPLATE: schema */river_client (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    paused_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL,
    CONSTRAINT client_name_length CHECK (CHAR_LENGTH(id) > 0 AND CHAR_LENGTH(id) < 128)
) ENGINE=InnoDB;

CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    max_workers INT NOT NULL DEFAULT 0,
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    num_jobs_completed BIGINT NOT NULL DEFAULT 0,
    num_jobs_running BIGINT NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT fk_river_client FOREIGN KEY (river_client_id) REFERENCES river_client (id) ON DELETE CASCADE,
    CONSTRAINT cq_name_length CHECK (CHAR_LENGTH(name) > 0 AND CHAR_LENGTH(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
) ENGINE=InnoDB;

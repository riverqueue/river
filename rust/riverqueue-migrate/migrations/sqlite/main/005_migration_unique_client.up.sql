--
-- Rebuild the migration table so it's based on `(line, version)`.
--

DROP INDEX /* TEMPLATE: schema */river_migration_version_idx;

ALTER TABLE /* TEMPLATE: schema */river_migration
    RENAME TO river_migration_old;

CREATE TABLE /* TEMPLATE: schema */river_migration (
    line text NOT NULL,
    version integer NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT line_length CHECK (length(line) > 0 AND length(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1),
    PRIMARY KEY (line, version)
);

INSERT INTO /* TEMPLATE: schema */river_migration
    (created_at, line, version)
SELECT created_at, 'main', version
FROM /* TEMPLATE: schema */river_migration_old;

DROP TABLE /* TEMPLATE: schema */river_migration_old;

--
-- Normally, `unique_key` and an index are added here, but because SQLite was
-- added later, we've just pushed all of this to an initial `river_job` creation
-- in 006.
--

--
-- Create `river_client` and derivative.
--
-- This feature hasn't quite yet been implemented, but we're taking advantage of
-- the migration to add the schema early so that we can add it later without an
-- additional migration.
--

CREATE TABLE /* TEMPLATE: schema */river_client (
    id text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL,
    CONSTRAINT name_length CHECK (length(id) > 0 AND length(id) < 128)
);

-- Differs from `river_queue` in that it tracks the queue state for a particular
-- active client.
CREATE TABLE /* TEMPLATE: schema */river_client_queue (
    river_client_id text NOT NULL REFERENCES river_client (id) ON DELETE CASCADE,
    name text NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    max_workers integer NOT NULL DEFAULT 0,
    metadata blob NOT NULL DEFAULT (json('{}')),
    num_jobs_completed integer NOT NULL DEFAULT 0,
    num_jobs_running integer NOT NULL DEFAULT 0,
    updated_at timestamp NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
);
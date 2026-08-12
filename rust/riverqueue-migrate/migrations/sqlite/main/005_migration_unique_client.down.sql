--
-- Revert to migration table based only on `(version)`.
--
-- If any non-main migrations are present, 005 is considered irreversible.
--

ALTER TABLE /* TEMPLATE: schema */river_migration
    RENAME TO river_migration_old;

CREATE TABLE /* TEMPLATE: schema */river_migration (
    id integer PRIMARY KEY,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version integer NOT NULL,
    CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX /* TEMPLATE: schema */river_migration_version_idx ON river_migration (version);

INSERT INTO /* TEMPLATE: schema */river_migration
    (created_at, version)
SELECT created_at, version
FROM /* TEMPLATE: schema */river_migration_old;

DROP TABLE /* TEMPLATE: schema */river_migration_old;

--
-- Normally, `unique_key` and an index are added here, but because SQLite was
-- added later, we've just pushed all of this to an initial `river_job` creation
-- in 006.
--

--
-- Drop `river_client` and derivative.
--

DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;

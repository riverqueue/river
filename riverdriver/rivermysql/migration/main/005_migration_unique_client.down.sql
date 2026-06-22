--
-- Revert to migration table based only on `(version)`.
--

CREATE TABLE /* TEMPLATE: schema */river_migration_old (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    version BIGINT NOT NULL,
    CONSTRAINT version CHECK (version >= 1)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX river_migration_version_idx ON /* TEMPLATE: schema */river_migration_old (version);

INSERT INTO /* TEMPLATE: schema */river_migration_old
    (created_at, version)
SELECT created_at, version
FROM /* TEMPLATE: schema */river_migration;

DROP TABLE /* TEMPLATE: schema */river_migration;

ALTER TABLE /* TEMPLATE: schema */river_migration_old RENAME TO /* TEMPLATE: schema */river_migration;

--
-- Drop `river_job.unique_key` and its index.
--

DROP INDEX river_job_kind_unique_key_idx ON /* TEMPLATE: schema */river_job;
ALTER TABLE /* TEMPLATE: schema */river_job DROP COLUMN unique_key;

--
-- Drop `river_client` and derivative.
--

DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;

--
-- Revert to migration table based only on `(version)`.
--
-- If any non-main migrations are present, 005 is considered irreversible.
--

DO
$body$
BEGIN
    -- Tolerate users who may be using their own migration system rather than
    -- River's. If they are, they will have skipped version 001 containing
    -- `CREATE TABLE river_migration`, so this table won't exist.
    IF (SELECT to_regclass('/* TEMPLATE: schema */river_migration') IS NOT NULL) THEN
        IF EXISTS (
            SELECT *
            FROM /* TEMPLATE: schema */river_migration
            WHERE line <> 'main'
        ) THEN
            RAISE EXCEPTION 'Found non-main migration lines in the database; version 005 migration is irreversible because it would result in loss of migration information.';
        END IF;

        ALTER TABLE /* TEMPLATE: schema */river_migration
            RENAME TO river_migration_old;

        CREATE TABLE /* TEMPLATE: schema */river_migration(
            id bigserial PRIMARY KEY,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            version bigint NOT NULL,
            CONSTRAINT version CHECK (version >= 1)
        );

        CREATE UNIQUE INDEX ON /* TEMPLATE: schema */river_migration USING btree(version);

        INSERT INTO /* TEMPLATE: schema */river_migration
            (created_at, version)
        SELECT created_at, version
        FROM /* TEMPLATE: schema */river_migration_old;

        DROP TABLE /* TEMPLATE: schema */river_migration_old;
    END IF;
END;
$body$
LANGUAGE 'plpgsql'; 

--
-- Drop `river_job.unique_key`.
--

ALTER TABLE /* TEMPLATE: schema */river_job
    DROP COLUMN unique_key;

--
-- Drop `river_client` and derivative.
--

DROP TABLE /* TEMPLATE: schema */river_client_queue;
DROP TABLE /* TEMPLATE: schema */river_client;

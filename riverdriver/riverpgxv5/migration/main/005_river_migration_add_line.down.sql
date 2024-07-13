--
-- If any non-main migration are present, 005 is considered irreversible.
--

DO
$body$
BEGIN
    IF EXISTS (
        SELECT *
        FROM river_migration
        WHERE line <> 'main'
    ) THEN
        RAISE EXCEPTION 'Found non-main migration lines in the database; version 005 migration is irreversible because it would result in loss of migration information.';
    END IF;
END;
$body$
LANGUAGE 'plpgsql'; 

ALTER TABLE river_migration
    RENAME TO river_migration_old;

CREATE TABLE river_migration(
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  version bigint NOT NULL,
  CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX ON river_migration USING btree(version);

INSERT INTO river_migration
    (created_at, version)
SELECT created_at, version
FROM river_migration_old;

DROP TABLE river_migration_old;
ALTER TABLE river_migration
    ADD COLUMN line text;

UPDATE river_migration
SET line = 'main';

ALTER TABLE river_migration
    ALTER COLUMN line SET NOT NULL,
    ADD CONSTRAINT line_length CHECK (char_length(line) > 0 AND char_length(line) < 128);

CREATE UNIQUE INDEX river_migration_line_version_idx ON river_migration USING btree(line, version);
DROP INDEX river_migration_version_idx;
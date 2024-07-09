DROP INDEX river_migration_line_version_idx;
CREATE UNIQUE INDEX river_migration_version_idx ON river_migration USING btree(version);

ALTER TABLE river_migration
    DROP CONSTRAINT line_length,
    DROP COLUMN line;
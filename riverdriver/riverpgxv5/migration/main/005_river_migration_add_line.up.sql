ALTER TABLE river_migration
    RENAME TO river_migration_old;

CREATE TABLE river_migration(
    line TEXT NOT NULL,
    version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT line_length CHECK (char_length(line) > 0 AND char_length(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1),
    PRIMARY KEY (line, version)
);

INSERT INTO river_migration
    (created_at, line, version)
SELECT created_at, 'main', version
FROM river_migration_old;

DROP TABLE river_migration_old;
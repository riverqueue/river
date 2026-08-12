CREATE TABLE /* TEMPLATE: schema */river_migration (
    id integer PRIMARY KEY,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version integer NOT NULL,
    CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX /* TEMPLATE: schema */river_migration_version_idx ON river_migration (version);
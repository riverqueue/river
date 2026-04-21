CREATE TABLE /* TEMPLATE: schema */river_migration (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    version BIGINT NOT NULL,
    CONSTRAINT version CHECK (version >= 1)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX river_migration_version_idx ON /* TEMPLATE: schema */river_migration (version);

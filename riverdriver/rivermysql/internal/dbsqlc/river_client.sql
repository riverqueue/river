CREATE TABLE river_client (
    id VARCHAR(128) NOT NULL PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    paused_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL
);

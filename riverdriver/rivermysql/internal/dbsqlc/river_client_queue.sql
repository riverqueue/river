CREATE TABLE river_client_queue (
    river_client_id VARCHAR(128) NOT NULL,
    name VARCHAR(128) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT (NOW(6)),
    max_workers INT NOT NULL DEFAULT 0,
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    num_jobs_completed BIGINT NOT NULL DEFAULT 0,
    num_jobs_running BIGINT NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (river_client_id, name),
    CONSTRAINT fk_river_client FOREIGN KEY (river_client_id) REFERENCES river_client (id) ON DELETE CASCADE
);

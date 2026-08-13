--
-- Normally `river_job` and `river_job_notify()` are raised here, but since
-- SQLite was added well after 002 came about, we push that to version 006 index.
--

-- Dummy `river_job` table so that there's something to truncate in tests when
-- migrated to this version specifically.
CREATE TABLE /* TEMPLATE: schema */river_job (
    id integer PRIMARY KEY
);

CREATE TABLE /* TEMPLATE: schema */river_leader (
    elected_at timestamp NOT NULL,
    expires_at timestamp NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY NOT NULL,
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT leader_id_length CHECK (length(leader_id) > 0 AND length(leader_id) < 128)
);

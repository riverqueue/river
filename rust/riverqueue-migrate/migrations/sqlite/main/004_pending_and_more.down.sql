--
-- Normally, args and metadata both become `NOT NULL`, `pending` is added, and
-- the constraint `finalized_at` is changed, but because SQLite was added later,
-- we've just pushed all of this to an initial `river_job` creation in 006.
--

--
-- Drop `river_queue`.
--

DROP TABLE /* TEMPLATE: schema */river_queue;

--
-- Reverse changes to `river_leader`.
--

DROP TABLE /* TEMPLATE: schema */river_leader;

CREATE TABLE /* TEMPLATE: schema */river_leader (
    elected_at timestamp NOT NULL,
    expires_at timestamp NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY NOT NULL,
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT leader_id_length CHECK (length(leader_id) > 0 AND length(leader_id) < 128)
);
--
-- Normally, args and metadata both become `NOT NULL`, `pending` is added, and
-- the constraint `finalized_at` is changed, but because SQLite was added later,
-- we've just pushed all of this to an initial `river_job` creation in 006.
--

--
-- Create table `river_queue`.
--

CREATE TABLE /* TEMPLATE: schema */river_queue (
    name text PRIMARY KEY NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata blob NOT NULL DEFAULT (json('{}')),
    paused_at timestamp,
    updated_at timestamp NOT NULL
);

--
-- Alter `river_leader` to add a default value of 'default` to `name`. SQLite
-- doesn't allow schema modifications, so this redefines the table entirely.
--

DROP TABLE /* TEMPLATE: schema */river_leader;

CREATE TABLE /* TEMPLATE: schema */river_leader (
    elected_at timestamp NOT NULL,
    expires_at timestamp NOT NULL,
    leader_id text NOT NULL,
    name text PRIMARY KEY NOT NULL DEFAULT 'default' CHECK (name = 'default'),
    CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
    CONSTRAINT leader_id_length CHECK (length(leader_id) > 0 AND length(leader_id) < 128)
);
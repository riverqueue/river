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
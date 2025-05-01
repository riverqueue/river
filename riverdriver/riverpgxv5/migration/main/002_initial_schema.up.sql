CREATE TYPE /* TEMPLATE: schema */river_job_state AS ENUM(
  'available',
  'cancelled',
  'completed',
  'discarded',
  'retryable',
  'running',
  'scheduled'
);

CREATE TABLE /* TEMPLATE: schema */river_job(
  -- 8 bytes
  id bigserial PRIMARY KEY,

  -- 8 bytes (4 bytes + 2 bytes + 2 bytes)
  --
  -- `state` is kept near the top of the table for operator convenience -- when
  -- looking at jobs with `SELECT *` it'll appear first after ID. The other two
  -- fields aren't as important but are kept adjacent to `state` for alignment
  -- to get an 8-byte block.
  state /* TEMPLATE: schema */river_job_state NOT NULL DEFAULT 'available',
  attempt smallint NOT NULL DEFAULT 0,
  max_attempts smallint NOT NULL,

  -- 8 bytes each (no alignment needed)
  attempted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  finalized_at timestamptz,
  scheduled_at timestamptz NOT NULL DEFAULT NOW(),

  -- 2 bytes (some wasted padding probably)
  priority smallint NOT NULL DEFAULT 1,

  -- types stored out-of-band
  args jsonb,
  attempted_by text[],
  errors jsonb[],
  kind text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}',
  queue text NOT NULL DEFAULT 'default',
  tags varchar(255)[],

  CONSTRAINT finalized_or_finalized_at_null CHECK ((state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR finalized_at IS NULL),
  CONSTRAINT max_attempts_is_positive CHECK (max_attempts > 0),
  CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
  CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
  CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

-- We may want to consider adding another property here after `kind` if it seems
-- like it'd be useful for something.
CREATE INDEX river_job_kind ON /* TEMPLATE: schema */river_job USING btree(kind);

CREATE INDEX river_job_state_and_finalized_at_index ON /* TEMPLATE: schema */river_job USING btree(state, finalized_at) WHERE finalized_at IS NOT NULL;

CREATE INDEX river_job_prioritized_fetching_index ON /* TEMPLATE: schema */river_job USING btree(state, queue, priority, scheduled_at, id);

CREATE INDEX river_job_args_index ON /* TEMPLATE: schema */river_job USING GIN(args);

CREATE INDEX river_job_metadata_index ON /* TEMPLATE: schema */river_job USING GIN(metadata);

CREATE OR REPLACE FUNCTION /* TEMPLATE: schema */river_job_notify()
  RETURNS TRIGGER
  AS $$
DECLARE
  payload json;
BEGIN
  IF NEW.state = 'available' THEN
    -- Notify will coalesce duplicate notifications within a transaction, so
    -- keep these payloads generalized:
    payload = json_build_object('queue', NEW.queue);
    PERFORM
      pg_notify('river_insert', payload::text);
  END IF;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER river_notify
  AFTER INSERT ON /* TEMPLATE: schema */river_job
  FOR EACH ROW
  EXECUTE PROCEDURE /* TEMPLATE: schema */river_job_notify();

CREATE UNLOGGED TABLE /* TEMPLATE: schema */river_leader(
    -- 8 bytes each (no alignment needed)
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,

    -- types stored out-of-band
    leader_id text NOT NULL,
    name text PRIMARY KEY,

    CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128),
    CONSTRAINT leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);

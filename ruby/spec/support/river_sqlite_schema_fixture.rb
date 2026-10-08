# frozen_string_literal: true

# A snapshot of River's complete SQLite schema after main migration 008.
# Production databases must use River's migrations; schema creation belongs in
# test support here so Ruby and Go never develop competing migration histories.
module RiverSQLiteSchemaFixture
  SCHEMA = <<~SQL
    CREATE TABLE river_migration (
      line text NOT NULL,
      version integer NOT NULL,
      created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT line_length CHECK (length(line) > 0 AND length(line) < 128),
      CONSTRAINT version_gte_1 CHECK (version >= 1),
      PRIMARY KEY (line, version)
    );

    INSERT INTO river_migration (line, version) VALUES ('main', 8);

    CREATE TABLE river_job (
      id integer PRIMARY KEY AUTOINCREMENT,
      args blob NOT NULL DEFAULT (jsonb('{}')),
      attempt integer NOT NULL DEFAULT 0,
      attempted_at timestamp,
      attempted_by blob,
      created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      errors blob,
      finalized_at timestamp,
      kind text NOT NULL,
      metadata blob NOT NULL DEFAULT (jsonb('{}')),
      priority integer NOT NULL DEFAULT 1,
      queue text NOT NULL DEFAULT 'default',
      state text NOT NULL DEFAULT 'available',
      scheduled_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      tags blob NOT NULL DEFAULT (jsonb('[]')),
      unique_key blob,
      unique_states integer,
      max_attempts integer NOT NULL DEFAULT 25,
      CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
      ),
      CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
      CONSTRAINT queue_length CHECK (length(queue) > 0 AND length(queue) < 128),
      CONSTRAINT kind_length CHECK (length(kind) > 0 AND length(kind) < 128),
      CONSTRAINT state_valid CHECK (state IN ('available', 'cancelled', 'completed', 'discarded', 'pending', 'retryable', 'running', 'scheduled'))
    );

    CREATE INDEX river_job_kind ON river_job (kind);
    CREATE INDEX river_job_state_and_finalized_at_index
      ON river_job (state, finalized_at) WHERE finalized_at IS NOT NULL;
    CREATE INDEX river_job_prioritized_fetching_index
      ON river_job (state, queue, priority, scheduled_at, id);
    CREATE UNIQUE INDEX river_job_unique_idx ON river_job (unique_key)
      WHERE unique_key IS NOT NULL
        AND unique_states IS NOT NULL
        AND CASE state
          WHEN 'available' THEN unique_states & (1 << 0)
          WHEN 'cancelled' THEN unique_states & (1 << 1)
          WHEN 'completed' THEN unique_states & (1 << 2)
          WHEN 'discarded' THEN unique_states & (1 << 3)
          WHEN 'pending'   THEN unique_states & (1 << 4)
          WHEN 'retryable' THEN unique_states & (1 << 5)
          WHEN 'running'   THEN unique_states & (1 << 6)
          WHEN 'scheduled' THEN unique_states & (1 << 7)
          ELSE 0
        END >= 1;

    CREATE TABLE river_leader (
      elected_at timestamp NOT NULL,
      expires_at timestamp NOT NULL,
      leader_id text NOT NULL,
      name text PRIMARY KEY NOT NULL DEFAULT 'default' CHECK (name = 'default'),
      CONSTRAINT name_length CHECK (length(name) > 0 AND length(name) < 128),
      CONSTRAINT leader_id_length CHECK (length(leader_id) > 0 AND length(leader_id) < 128)
    );

    CREATE TABLE river_queue (
      name text PRIMARY KEY NOT NULL,
      created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      metadata blob NOT NULL DEFAULT (jsonb('{}')),
      paused_at timestamp,
      updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE river_notification (
      id integer PRIMARY KEY AUTOINCREMENT,
      created_at timestamp NOT NULL DEFAULT (datetime('now', 'subsec')),
      payload text NOT NULL,
      topic text NOT NULL,
      CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
    );

    CREATE INDEX river_notification_created_at_idx
      ON river_notification (created_at);
    CREATE INDEX river_notification_topic_id_idx
      ON river_notification (topic, id);
  SQL

  def self.load(database)
    database.execute_batch(SCHEMA)
  end
end

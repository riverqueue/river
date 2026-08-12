CREATE TABLE /* TEMPLATE: schema */river_migration(
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT NOW(),
  version bigint NOT NULL,
  CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX ON /* TEMPLATE: schema */river_migration USING btree(version);
-- name: columns
id, args, attempt, attempted_at, array_to_json(attempted_by) AS attempted_by,
created_at, array_to_json(errors) AS errors, finalized_at, kind, max_attempts,
metadata, priority, queue, scheduled_at, state, array_to_json(tags) AS tags,
unique_key, unique_states

-- name: get
SELECT {columns} FROM {schema}river_job WHERE id = ?

-- name: insert
INSERT INTO {schema}river_job
(args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
VALUES (?::jsonb, ?, ?, ?, ?::jsonb, ?, ?, ?, ?::{schema}river_job_state, ?, ?, ?::bit(8))
ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL
AND {schema}river_job_state_in_bitmask(unique_states, state)
-- Keep the existing kind, which may differ when uniqueness excludes kind.
DO UPDATE SET kind = river_job.kind
RETURNING {columns}, {duplicate} AS duplicate

-- name: insert_duplicate_nonce
false
-- name: insert_duplicate_xmax
(xmax != 0)

-- name: notify
SELECT pg_notify(coalesce(nullif('{schema_name}',''),current_schema()) || '.' || ?, ?)

-- name: migration_insert
INSERT INTO {schema}river_migration (line,version) VALUES ('main',?)
-- name: migration_insert_legacy
INSERT INTO {schema}river_migration (version) VALUES (?)
-- name: migration_delete
DELETE FROM {schema}river_migration WHERE line='main' AND version=?
-- name: migration_delete_legacy
DELETE FROM {schema}river_migration WHERE version=?
-- name: migration_versions
SELECT version FROM {schema}river_migration WHERE line='main' ORDER BY version
-- name: migration_versions_legacy
SELECT version FROM {schema}river_migration ORDER BY version
-- name: list
SELECT {columns} FROM {schema}river_job WHERE {where} ORDER BY {order} LIMIT ?
-- name: list_deletable
SELECT {columns} FROM {schema}river_job WHERE {where} AND state != 'running' ORDER BY {order} LIMIT ?
-- name: cursor_id
(id {comparison} ?)
-- name: cursor_null_asc
({field} IS NULL AND id > ?)
-- name: cursor_null_desc
({field} IS NOT NULL OR id < ?)
-- name: cursor_time
({field} {comparison} ? OR ({field} = ? AND id {comparison} ?))
-- name: cursor_time_nullable
({field} > ? OR ({field} = ? AND id > ?) OR {field} IS NULL)
-- name: delete
DELETE FROM {schema}river_job WHERE id=? AND state != 'running' RETURNING {columns}
-- name: delete_filtered
DELETE FROM {schema}river_job WHERE {where} AND id=? AND state != 'running' RETURNING {columns}
-- name: retry
UPDATE {schema}river_job SET state='available', finalized_at=NULL,
max_attempts=CASE WHEN attempt=max_attempts THEN max_attempts+1 ELSE max_attempts END, scheduled_at=?
WHERE id=? AND state != 'running' AND NOT (state='available' AND scheduled_at < ?) RETURNING {columns}
-- name: reset_jobs
DELETE FROM {schema}river_job
-- name: reset_queues
DELETE FROM {schema}river_queue
-- name: reset_leader
DELETE FROM {schema}river_leader
-- name: reset_notifications
DELETE FROM {schema}river_notification

-- name: create_schema
CREATE SCHEMA IF NOT EXISTS "{schema_name}"
-- name: migration_lock
SELECT pg_advisory_lock(1789812276)
-- name: migration_exists
SELECT to_regclass('{schema}river_migration') IS NOT NULL
-- name: migration_has_line
SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=coalesce(nullif('{schema_name}',''),current_schema()) AND table_name='river_migration' AND column_name='line')
-- name: lock_get
SELECT {columns} FROM {schema}river_job WHERE id=? FOR UPDATE
-- name: cancel
UPDATE {schema}river_job SET state=CASE WHEN state='running' THEN state ELSE 'cancelled' END,
finalized_at=CASE WHEN state='running' THEN finalized_at ELSE ? END,
metadata=jsonb_set(metadata,'{cancel_attempted_at}',?::jsonb,true)
WHERE id=? AND state NOT IN ('cancelled','completed','discarded') AND finalized_at IS NULL RETURNING {columns}
-- name: output
UPDATE {schema}river_job SET metadata=jsonb_set(metadata,'{output}',?::jsonb,true) WHERE id=? RETURNING {columns}
-- name: filter_metadata
metadata @> ?::jsonb
-- name: filter_tags_all
tags @> ARRAY(SELECT jsonb_array_elements_text(?::jsonb))::varchar[]
-- name: filter_tags_any
tags && ARRAY(SELECT jsonb_array_elements_text(?::jsonb))::varchar[]

-- name: filter_ids
id IN (SELECT jsonb_array_elements_text(?::jsonb)::bigint)

-- name: filter_kinds
kind IN (SELECT jsonb_array_elements_text(?::jsonb)::text)

-- name: filter_priorities
priority IN (SELECT jsonb_array_elements_text(?::jsonb)::integer)

-- name: filter_queues
queue IN (SELECT jsonb_array_elements_text(?::jsonb)::text)

-- name: filter_states
state::text IN (SELECT jsonb_array_elements_text(?::jsonb)::text)

-- name: queue_pause
UPDATE {schema}river_queue SET updated_at=CASE WHEN paused_at IS NULL THEN ? ELSE updated_at END,
paused_at=coalesce(paused_at, CURRENT_TIMESTAMP) WHERE (?='*' OR name=?)
-- name: queue_resume
UPDATE {schema}river_queue SET updated_at=CASE WHEN paused_at IS NOT NULL THEN ? ELSE updated_at END,
paused_at=NULL WHERE (?='*' OR name=?)
-- name: queue_heartbeat
INSERT INTO {schema}river_queue (name,created_at,updated_at) VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET updated_at=excluded.updated_at
-- name: leader_get
SELECT leader_id,elected_at FROM {schema}river_leader WHERE name='default'
-- name: leader_elect
INSERT INTO {schema}river_leader (leader_id,elected_at,expires_at) VALUES (?,?,?) ON CONFLICT(name) DO NOTHING RETURNING elected_at
-- name: leader_expire
DELETE FROM {schema}river_leader WHERE expires_at < ?
-- name: leader_renew
UPDATE {schema}river_leader SET expires_at=? WHERE leader_id=? AND elected_at=? AND expires_at >= ?
-- name: leader_resign
DELETE FROM {schema}river_leader WHERE leader_id=? AND elected_at=?
-- name: schedule
UPDATE {schema}river_job SET state='available' WHERE state IN ('scheduled','retryable') AND scheduled_at<=? RETURNING queue
-- name: notifications
SELECT id,payload FROM {schema}river_notification WHERE id>? ORDER BY id

-- name: queue_get
SELECT name,created_at,metadata,paused_at,updated_at FROM {schema}river_queue WHERE name=?
-- name: queue_list
SELECT name,created_at,metadata,paused_at,updated_at FROM {schema}river_queue ORDER BY name LIMIT ?
-- name: queue_update
UPDATE {schema}river_queue SET metadata=?::jsonb,updated_at=? WHERE name=?
RETURNING name,created_at,metadata,paused_at,updated_at

-- name: claim
UPDATE {schema}river_job SET state='running',attempt=attempt+1,attempted_at=?,
attempted_by=array_append(CASE WHEN array_length(attempted_by,1)>=100 THEN attempted_by[array_length(attempted_by,1)-98:] ELSE attempted_by END,?::text)
WHERE id IN (SELECT id FROM {schema}river_job WHERE queue=? AND state='available' AND scheduled_at<=?
AND NOT EXISTS(SELECT 1 FROM {schema}river_queue q WHERE q.name=river_job.queue AND paused_at IS NOT NULL)
ORDER BY priority,scheduled_at,id LIMIT ? FOR UPDATE SKIP LOCKED) RETURNING {columns}
-- name: claim_known
UPDATE {schema}river_job SET state='running',attempt=attempt+1,attempted_at=?,
attempted_by=array_append(CASE WHEN array_length(attempted_by,1)>=100 THEN attempted_by[array_length(attempted_by,1)-98:] ELSE attempted_by END,?::text)
WHERE id IN (SELECT id FROM {schema}river_job WHERE queue=? AND state='available' AND scheduled_at<=?
AND NOT EXISTS(SELECT 1 FROM {schema}river_queue q WHERE q.name=river_job.queue AND paused_at IS NOT NULL)
AND kind IN (SELECT jsonb_array_elements_text(?::jsonb))
ORDER BY priority,scheduled_at,id LIMIT ? FOR UPDATE SKIP LOCKED) RETURNING {columns}
-- name: complete
WITH choice AS (SELECT ?::text AS target_state, ?::timestamptz AS now)
UPDATE {schema}river_job SET
state=CASE WHEN state != 'running' THEN state WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND metadata->>'cancel_attempted_at' IS NOT NULL THEN 'cancelled' ELSE ?::{schema}river_job_state END,
finalized_at=CASE WHEN state != 'running' THEN finalized_at WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND metadata->>'cancel_attempted_at' IS NOT NULL THEN (SELECT now FROM choice) ELSE ? END,
scheduled_at=CASE WHEN state != 'running' THEN scheduled_at WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND metadata->>'cancel_attempted_at' IS NOT NULL THEN scheduled_at ELSE ? END,
attempt=CASE WHEN state != 'running' THEN attempt WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND metadata->>'cancel_attempted_at' IS NOT NULL THEN attempt ELSE ? END,
metadata=coalesce(metadata || nullif(?::jsonb,'{}'::jsonb),metadata),
errors=CASE WHEN state != 'running' OR ?::text IS NULL THEN errors ELSE array_append(errors,?::jsonb) END
WHERE id=? AND attempt=? AND attempted_at=? RETURNING {columns}

-- name: migration_unlock
SELECT pg_advisory_unlock(1789812276)

-- name: delete_finalized
DELETE FROM {schema}river_job WHERE id IN (SELECT id FROM {schema}river_job
WHERE state IN ('cancelled','completed','discarded') AND finalized_at < ?
AND queue NOT IN (SELECT jsonb_array_elements_text(?::jsonb)) AND (? OR queue IN (SELECT jsonb_array_elements_text(?::jsonb))) ORDER BY id LIMIT ?)
-- name: rescue_select
SELECT {columns} FROM {schema}river_job WHERE state='running' AND attempted_at<? AND id>? ORDER BY id LIMIT 1000 FOR UPDATE SKIP LOCKED

-- name: database_version
SELECT version(), coalesce(current_setting('yb_enable_listen_notify', true), 'off')::boolean

-- name: cancel_requested
SELECT id FROM {schema}river_job WHERE state='running' AND metadata->>'cancel_attempted_at' IS NOT NULL

-- name: notification_cursor
SELECT coalesce(max(id),0) FROM {schema}river_notification
-- name: schedule_select
SELECT id,unique_key FROM {schema}river_job WHERE state IN ('scheduled','retryable') AND scheduled_at<=? ORDER BY priority,scheduled_at,id LIMIT 10000 FOR UPDATE
-- name: schedule_collision
SELECT id FROM {schema}river_job WHERE id<>? AND unique_key=? AND unique_states IS NOT NULL AND {schema}river_job_state_in_bitmask(unique_states,state)
-- name: schedule_available
UPDATE {schema}river_job SET state='available' WHERE id=? AND state IN ('scheduled','retryable') RETURNING queue
-- name: schedule_discard
UPDATE {schema}river_job SET state='discarded',finalized_at=?,metadata=metadata || '{"unique_key_conflict":"scheduler_discarded"}'::jsonb WHERE id=? AND state IN ('scheduled','retryable')

-- name: clean_jobs
DELETE FROM {schema}river_job WHERE id IN (SELECT id FROM {schema}river_job WHERE
(state='cancelled' AND finalized_at<?) OR (state='completed' AND finalized_at<?) OR (state='discarded' AND finalized_at<?) LIMIT 1000)
-- name: clean_queues
DELETE FROM {schema}river_queue WHERE updated_at<?

-- name: rescue_update
UPDATE {schema}river_job SET
state=CASE WHEN state IN ('completed','cancelled','discarded') THEN state ELSE ?::{schema}river_job_state END,
finalized_at=CASE WHEN state IN ('completed','cancelled','discarded') THEN finalized_at ELSE ? END,
scheduled_at=CASE WHEN state IN ('completed','cancelled','discarded') THEN scheduled_at ELSE ? END,
attempt=CASE WHEN state IN ('completed','cancelled','discarded') THEN attempt ELSE ? END,
metadata=coalesce(metadata || nullif(?::jsonb,'{}'::jsonb),metadata),
errors=CASE WHEN state IN ('completed','cancelled','discarded') OR ?::text IS NULL THEN errors ELSE array_append(errors,?::jsonb) END
WHERE state='running' AND id=? AND attempt=? AND attempted_at=? RETURNING {columns}

-- name: reindex_candidate
SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=coalesce(nullif('{schema_name}',''),current_schema()) AND c.relname=? AND c.relkind='i'
AND NOT EXISTS(SELECT 1 FROM pg_class a WHERE a.relnamespace=n.oid AND (a.relname LIKE ? || '_ccnew%' OR a.relname LIKE c.relname || '_ccold%'))
-- name: reindex
REINDEX INDEX CONCURRENTLY {index}
-- name: periodic_scheduled_at
UPDATE {schema}river_job SET scheduled_at=? WHERE id=?

-- name: migration_line_delete
DELETE FROM {schema}river_migration WHERE line=? AND version=?
-- name: migration_line_insert
INSERT INTO {schema}river_migration (line,version) VALUES (?,?)
-- name: migration_line_versions
SELECT version FROM {schema}river_migration WHERE line=? ORDER BY version
-- name: migration_other_lines
SELECT EXISTS(SELECT 1 FROM {schema}river_migration WHERE line<>'main')

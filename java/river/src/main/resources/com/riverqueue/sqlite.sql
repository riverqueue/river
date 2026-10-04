-- name: columns
id, CASE WHEN typeof(args)='text' AND NOT json_valid(args) THEN args ELSE json(args) END AS args, attempt, attempted_at, CASE WHEN typeof(attempted_by)='text' AND NOT json_valid(attempted_by) THEN attempted_by ELSE json(attempted_by) END AS attempted_by,
created_at, CASE WHEN typeof(errors)='text' AND NOT json_valid(errors) THEN errors ELSE json(errors) END AS errors, finalized_at, kind, max_attempts,
CASE WHEN typeof(metadata)='text' AND NOT json_valid(metadata) THEN metadata ELSE json(metadata) END AS metadata, priority, queue, scheduled_at, state, CASE WHEN typeof(tags)='text' AND NOT json_valid(tags) THEN tags ELSE json(tags) END AS tags,
unique_key, unique_states

-- name: get
SELECT {columns} FROM {schema}river_job WHERE id = ?

-- name: insert
INSERT INTO {schema}river_job
(args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
VALUES (jsonb(?), ?, ?, ?, jsonb(?), ?, ?, ?, ?, jsonb(?), ?, ?)
ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL
AND CASE state
WHEN 'available' THEN unique_states & (1 << 0) WHEN 'cancelled' THEN unique_states & (1 << 1)
WHEN 'completed' THEN unique_states & (1 << 2) WHEN 'discarded' THEN unique_states & (1 << 3)
WHEN 'pending' THEN unique_states & (1 << 4) WHEN 'retryable' THEN unique_states & (1 << 5)
WHEN 'running' THEN unique_states & (1 << 6) WHEN 'scheduled' THEN unique_states & (1 << 7) ELSE 0 END >= 1
DO UPDATE SET kind = excluded.kind
RETURNING {columns}

-- name: notify
INSERT INTO river_notification (topic, payload) VALUES (?, ?)

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

-- name: migration_exists
SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='river_migration')
-- name: migration_has_line
SELECT EXISTS(SELECT 1 FROM pragma_table_info('river_migration') WHERE name='line')
-- name: lock_get
SELECT {columns} FROM river_job WHERE id=?
-- name: writer_lock
UPDATE river_job SET id=id WHERE false
-- name: unique_get
SELECT {columns} FROM river_job WHERE unique_key=? AND CASE state
WHEN 'available' THEN unique_states & (1 << 0) WHEN 'cancelled' THEN unique_states & (1 << 1)
WHEN 'completed' THEN unique_states & (1 << 2) WHEN 'discarded' THEN unique_states & (1 << 3)
WHEN 'pending' THEN unique_states & (1 << 4) WHEN 'retryable' THEN unique_states & (1 << 5)
WHEN 'running' THEN unique_states & (1 << 6) WHEN 'scheduled' THEN unique_states & (1 << 7) ELSE 0 END >= 1
-- name: cancel
UPDATE river_job SET state=CASE WHEN state='running' THEN state ELSE 'cancelled' END,
finalized_at=CASE WHEN state='running' THEN finalized_at ELSE ? END,
metadata=jsonb_set(metadata,'$.cancel_attempted_at',jsonb(?))
WHERE id=? AND state NOT IN ('cancelled','completed','discarded') AND finalized_at IS NULL RETURNING {columns}
-- name: output
UPDATE river_job SET metadata=jsonb_set(metadata,'$.output',jsonb(?)) WHERE id=? RETURNING {columns}
-- name: filter_metadata
NOT EXISTS(SELECT 1 FROM json_tree(?) wanted WHERE wanted.type NOT IN ('object','array') AND NOT EXISTS(SELECT 1 FROM json_tree(metadata) actual WHERE actual.fullkey=wanted.fullkey AND actual.value IS wanted.value))
-- name: filter_tags_all
NOT EXISTS(SELECT 1 FROM json_each(?) wanted WHERE wanted.value NOT IN (SELECT value FROM json_each(tags)))
-- name: filter_tags_any
EXISTS(SELECT 1 FROM json_each(?) wanted WHERE wanted.value IN (SELECT value FROM json_each(tags)))

-- name: filter_ids
id IN (SELECT value FROM json_each(?))

-- name: filter_kinds
kind IN (SELECT value FROM json_each(?))

-- name: filter_priorities
priority IN (SELECT value FROM json_each(?))

-- name: filter_queues
queue IN (SELECT value FROM json_each(?))

-- name: filter_states
state IN (SELECT value FROM json_each(?))

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
SELECT name,created_at,CASE WHEN typeof(metadata)='text' AND NOT json_valid(metadata) THEN metadata ELSE json(metadata) END AS metadata,paused_at,updated_at FROM {schema}river_queue WHERE name=?
-- name: queue_list
SELECT name,created_at,CASE WHEN typeof(metadata)='text' AND NOT json_valid(metadata) THEN metadata ELSE json(metadata) END AS metadata,paused_at,updated_at FROM {schema}river_queue ORDER BY name LIMIT ?
-- name: queue_update
UPDATE {schema}river_queue SET metadata=jsonb(?),updated_at=? WHERE name=?
RETURNING name,created_at,CASE WHEN typeof(metadata)='text' AND NOT json_valid(metadata) THEN metadata ELSE json(metadata) END AS metadata,paused_at,updated_at

-- name: claim
UPDATE river_job SET state='running',attempt=attempt+1,attempted_at=?,
attempted_by=CASE WHEN typeof(attempted_by)='text' AND NOT json_valid(attempted_by) THEN attempted_by ELSE jsonb_insert(CASE WHEN json_array_length(attempted_by)>=100 THEN jsonb_remove(attempted_by,'$[0]') ELSE coalesce(attempted_by,jsonb('[]')) END,'$[#]',?) END
WHERE id IN (SELECT id FROM river_job WHERE queue=? AND state='available' AND scheduled_at<=?
AND NOT EXISTS(SELECT 1 FROM river_queue q WHERE q.name=river_job.queue AND paused_at IS NOT NULL)
ORDER BY priority,scheduled_at,id LIMIT ?) RETURNING {columns}
-- name: claim_known
UPDATE river_job SET state='running',attempt=attempt+1,attempted_at=?,
attempted_by=CASE WHEN typeof(attempted_by)='text' AND NOT json_valid(attempted_by) THEN attempted_by ELSE jsonb_insert(CASE WHEN json_array_length(attempted_by)>=100 THEN jsonb_remove(attempted_by,'$[0]') ELSE coalesce(attempted_by,jsonb('[]')) END,'$[#]',?) END
WHERE id IN (SELECT id FROM river_job WHERE queue=? AND state='available' AND scheduled_at<=?
AND NOT EXISTS(SELECT 1 FROM river_queue q WHERE q.name=river_job.queue AND paused_at IS NOT NULL)
AND kind IN (SELECT value FROM json_each(?))
ORDER BY priority,scheduled_at,id LIMIT ?) RETURNING {columns}
-- name: complete
WITH choice AS (SELECT ? AS target_state, ? AS now)
UPDATE river_job SET
state=CASE WHEN state IN ('completed','cancelled','discarded') THEN state WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND (CASE WHEN typeof(metadata)<>'text' OR json_valid(metadata) THEN json_extract(metadata,'$.cancel_attempted_at') END) IS NOT NULL THEN 'cancelled' ELSE ? END,
finalized_at=CASE WHEN state IN ('completed','cancelled','discarded') THEN finalized_at WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND (CASE WHEN typeof(metadata)<>'text' OR json_valid(metadata) THEN json_extract(metadata,'$.cancel_attempted_at') END) IS NOT NULL THEN (SELECT now FROM choice) ELSE ? END,
scheduled_at=CASE WHEN state IN ('completed','cancelled','discarded') THEN scheduled_at WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND (CASE WHEN typeof(metadata)<>'text' OR json_valid(metadata) THEN json_extract(metadata,'$.cancel_attempted_at') END) IS NOT NULL THEN scheduled_at ELSE ? END,
attempt=CASE WHEN state IN ('completed','cancelled','discarded') THEN attempt WHEN (SELECT target_state FROM choice) IN ('available','retryable','scheduled') AND (CASE WHEN typeof(metadata)<>'text' OR json_valid(metadata) THEN json_extract(metadata,'$.cancel_attempted_at') END) IS NOT NULL THEN attempt ELSE ? END,
metadata=CASE WHEN typeof(metadata)='text' AND NOT json_valid(metadata) THEN metadata ELSE jsonb_patch(metadata,jsonb(?)) END,
errors=CASE WHEN state IN ('completed','cancelled','discarded') OR ? IS NULL THEN errors ELSE jsonb_insert(CASE WHEN typeof(errors)='text' AND NOT json_valid(errors) THEN jsonb_array(errors) WHEN coalesce(json_type(errors),'array') <> 'array' THEN jsonb_array(json(errors)) ELSE coalesce(errors,jsonb('[]')) END,'$[#]',jsonb(?)) END
WHERE id=? AND attempt=? AND attempted_at=? RETURNING {columns}

-- name: delete_finalized
DELETE FROM {schema}river_job WHERE id IN (SELECT id FROM {schema}river_job
WHERE state IN ('cancelled','completed','discarded') AND finalized_at < ?
AND queue NOT IN (SELECT value FROM json_each(?)) AND (? OR queue IN (SELECT value FROM json_each(?))) ORDER BY id LIMIT ?)
-- name: rescue_select
SELECT {columns} FROM {schema}river_job WHERE state='running' AND attempted_at<? AND id>? ORDER BY id LIMIT 1000

-- name: cancel_requested
SELECT id FROM river_job WHERE state='running' AND (CASE WHEN typeof(metadata)<>'text' OR json_valid(metadata) THEN json_extract(metadata,'$.cancel_attempted_at') END) IS NOT NULL

-- name: notification_cursor
SELECT coalesce(max(id),0) FROM {schema}river_notification
-- name: schedule_select
SELECT id,unique_key FROM {schema}river_job WHERE state IN ('scheduled','retryable') AND scheduled_at<=? ORDER BY priority,scheduled_at,id LIMIT 10000
-- name: schedule_collision
SELECT id FROM {schema}river_job WHERE id<>? AND unique_key=? AND unique_states IS NOT NULL AND CASE state WHEN 'available' THEN unique_states & 1 WHEN 'cancelled' THEN unique_states & 2 WHEN 'completed' THEN unique_states & 4 WHEN 'discarded' THEN unique_states & 8 WHEN 'pending' THEN unique_states & 16 WHEN 'retryable' THEN unique_states & 32 WHEN 'running' THEN unique_states & 64 WHEN 'scheduled' THEN unique_states & 128 ELSE 0 END >= 1
-- name: schedule_available
UPDATE {schema}river_job SET state='available' WHERE id=? AND state IN ('scheduled','retryable') RETURNING queue
-- name: schedule_discard
UPDATE {schema}river_job SET state='discarded',finalized_at=?,metadata=jsonb_patch(metadata,'{"unique_key_conflict":"scheduler_discarded"}') WHERE id=? AND state IN ('scheduled','retryable')

-- name: clean_jobs
DELETE FROM {schema}river_job WHERE id IN (SELECT id FROM {schema}river_job WHERE
(state='cancelled' AND finalized_at<?) OR (state='completed' AND finalized_at<?) OR (state='discarded' AND finalized_at<?) LIMIT 1000)
-- name: clean_queues
DELETE FROM {schema}river_queue WHERE updated_at<?

-- name: rescue_update
UPDATE river_job SET
state=CASE WHEN state IN ('completed','cancelled','discarded') THEN state ELSE ? END,
finalized_at=CASE WHEN state IN ('completed','cancelled','discarded') THEN finalized_at ELSE ? END,
scheduled_at=CASE WHEN state IN ('completed','cancelled','discarded') THEN scheduled_at ELSE ? END,
attempt=CASE WHEN state IN ('completed','cancelled','discarded') THEN attempt ELSE ? END,
metadata=CASE WHEN typeof(metadata)='text' AND NOT json_valid(metadata) THEN metadata ELSE jsonb_patch(metadata,jsonb(?)) END,
errors=CASE WHEN state IN ('completed','cancelled','discarded') OR ? IS NULL THEN errors ELSE jsonb_insert(CASE WHEN typeof(errors)='text' AND NOT json_valid(errors) THEN jsonb_array(errors) WHEN coalesce(json_type(errors),'array') <> 'array' THEN jsonb_array(json(errors)) ELSE coalesce(errors,jsonb('[]')) END,'$[#]',jsonb(?)) END
WHERE state='running' AND id=? AND attempt=? AND attempted_at=? RETURNING {columns}
-- name: periodic_scheduled_at
UPDATE river_job SET scheduled_at=? WHERE id=?

-- name: migration_line_delete
DELETE FROM {schema}river_migration WHERE line=? AND version=?
-- name: migration_line_insert
INSERT INTO {schema}river_migration (line,version) VALUES (?,?)
-- name: migration_line_versions
SELECT version FROM {schema}river_migration WHERE line=? ORDER BY version
-- name: migration_other_lines
SELECT EXISTS(SELECT 1 FROM {schema}river_migration WHERE line<>'main')

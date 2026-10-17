-- name: columns
id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states

-- name: get
SELECT {columns} FROM {schema}river_job WHERE id = ?

-- name: insert
INSERT INTO {schema}river_job
(args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
VALUES (CAST(? AS JSON), ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, CAST(? AS JSON), ?, ?)
ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)

-- name: notify
INSERT INTO {schema}river_notification (topic, payload) VALUES (?, ?)

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
SELECT {columns} FROM {schema}river_job WHERE {where} AND state != 'running' ORDER BY {order} LIMIT ? FOR UPDATE SKIP LOCKED

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
DELETE FROM {schema}river_job WHERE id=? AND state != 'running'

-- name: delete_filtered
DELETE FROM {schema}river_job WHERE {where} AND id=? AND state != 'running'

-- name: retry
UPDATE {schema}river_job SET finalized_at=NULL,
max_attempts=CASE WHEN attempt=max_attempts THEN max_attempts+1 ELSE max_attempts END, scheduled_at=?, state='available'
WHERE id=? AND state != 'running' AND NOT (state='available' AND scheduled_at < ?)

-- name: reset_jobs
DELETE FROM {schema}river_job

-- name: reset_queues
DELETE FROM {schema}river_queue

-- name: reset_leader
DELETE FROM {schema}river_leader

-- name: reset_notifications
DELETE FROM {schema}river_notification

-- name: migration_exists
SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=COALESCE(NULLIF('{schema_name}',''),DATABASE()) AND table_name='river_migration')

-- name: migration_has_line
SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=COALESCE(NULLIF('{schema_name}',''),DATABASE()) AND table_name='river_migration' AND column_name='line')

-- name: lock_get
SELECT {columns} FROM {schema}river_job WHERE id=? FOR UPDATE

-- name: cancel
UPDATE {schema}river_job SET
finalized_at=CASE WHEN state='running' THEN finalized_at ELSE ? END,
metadata=JSON_SET(metadata, '$.cancel_attempted_at', CAST(? AS JSON)),
state=CASE WHEN state='running' THEN state ELSE 'cancelled' END
WHERE id=? AND state NOT IN ('cancelled','completed','discarded') AND finalized_at IS NULL

-- name: output
UPDATE {schema}river_job SET metadata=JSON_SET(metadata,'$.output',CAST(? AS JSON)) WHERE id=?

-- name: filter_tags_all
JSON_CONTAINS(tags, CAST(? AS JSON))

-- name: filter_tags_any
JSON_OVERLAPS(tags, CAST(? AS JSON))

-- name: filter_ids
id IN (SELECT value FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value BIGINT PATH '$')) AS wanted)

-- name: filter_kinds
CAST(kind AS BINARY) IN (SELECT value FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) AS wanted)

-- name: filter_priorities
priority IN (SELECT value FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value SMALLINT PATH '$')) AS wanted)

-- name: filter_queues
CAST(queue AS BINARY) IN (SELECT value FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) AS wanted)

-- name: filter_states
state IN (SELECT value FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) AS wanted)

-- name: queue_pause
UPDATE {schema}river_queue SET updated_at=CASE WHEN paused_at IS NULL THEN ? ELSE updated_at END, paused_at=COALESCE(paused_at, UTC_TIMESTAMP(6)) WHERE (?='*' OR name=CAST(? AS BINARY))

-- name: queue_resume
UPDATE {schema}river_queue SET updated_at=CASE WHEN paused_at IS NOT NULL THEN ? ELSE updated_at END,
paused_at=NULL WHERE (?='*' OR name=CAST(? AS BINARY))

-- name: queue_heartbeat
INSERT INTO {schema}river_queue (name,created_at,updated_at) VALUES (?,?,?) ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)

-- name: leader_get
SELECT leader_id,elected_at FROM {schema}river_leader WHERE name='default'

-- name: leader_elect
INSERT INTO {schema}river_leader (name,leader_id,elected_at,expires_at) VALUES ('default',?,?,?) ON DUPLICATE KEY UPDATE name=name

-- name: leader_expire
DELETE FROM {schema}river_leader WHERE expires_at < ?

-- name: leader_renew
UPDATE {schema}river_leader SET expires_at=? WHERE leader_id=CAST(? AS BINARY) AND elected_at=? AND expires_at >= ?

-- name: leader_resign
DELETE FROM {schema}river_leader WHERE leader_id=CAST(? AS BINARY) AND elected_at=?

-- name: notifications
SELECT id,topic,payload FROM {schema}river_notification WHERE id>? ORDER BY id LIMIT 256 FOR SHARE

-- name: queue_get
SELECT name,created_at,metadata,paused_at,updated_at FROM {schema}river_queue WHERE name=?

-- name: queue_list
SELECT name,created_at,metadata,paused_at,updated_at FROM {schema}river_queue ORDER BY name LIMIT ?

-- name: queue_update
UPDATE {schema}river_queue SET metadata=CAST(? AS JSON), updated_at=? WHERE name=?

-- name: complete
WITH input AS (SELECT ? AS target_state, ? AS now, ? AS state, ? AS finalized_at,
? AS scheduled_at, ? AS attempt, CAST(? AS JSON) AS metadata, ? AS error_present, CAST(? AS JSON) AS error,
? AS id, ? AS old_attempt, ? AS attempted_at)
UPDATE {schema}river_job j JOIN input i ON j.id=i.id SET
j.errors=CASE WHEN j.state != 'running' OR i.error_present IS NULL THEN j.errors ELSE JSON_ARRAY_APPEND(CASE WHEN j.errors IS NULL THEN JSON_ARRAY() WHEN JSON_TYPE(j.errors)='ARRAY' THEN j.errors ELSE JSON_ARRAY(j.errors) END,'$',i.error) END,
j.finalized_at=CASE WHEN j.state != 'running' THEN j.finalized_at WHEN i.target_state IN ('available','retryable','scheduled') AND JSON_CONTAINS_PATH(j.metadata,'one','$.cancel_attempted_at') THEN i.now ELSE i.finalized_at END,
j.scheduled_at=CASE WHEN j.state != 'running' OR (i.target_state IN ('available','retryable','scheduled') AND JSON_CONTAINS_PATH(j.metadata,'one','$.cancel_attempted_at')) THEN j.scheduled_at ELSE i.scheduled_at END,
j.attempt=CASE WHEN j.state != 'running' OR (i.target_state IN ('available','retryable','scheduled') AND JSON_CONTAINS_PATH(j.metadata,'one','$.cancel_attempted_at')) THEN j.attempt ELSE i.attempt END,
j.state=CASE WHEN j.state != 'running' THEN j.state WHEN i.target_state IN ('available','retryable','scheduled') AND JSON_CONTAINS_PATH(j.metadata,'one','$.cancel_attempted_at') THEN 'cancelled' ELSE i.state END,
j.metadata=i.metadata
WHERE j.attempt=i.old_attempt AND j.attempted_at=i.attempted_at

-- name: delete_finalized
DELETE FROM {schema}river_job WHERE state IN ('cancelled','completed','discarded') AND finalized_at<?
AND NOT JSON_CONTAINS(CAST(? AS JSON), JSON_QUOTE(queue)) AND (? OR JSON_CONTAINS(CAST(? AS JSON), JSON_QUOTE(queue)))
ORDER BY id LIMIT ?

-- name: rescue_select
SELECT {columns} FROM {schema}river_job WHERE state='running' AND attempted_at<? AND id>? ORDER BY id LIMIT 1000 FOR UPDATE SKIP LOCKED

-- name: cancel_requested
SELECT id FROM {schema}river_job WHERE state='running' AND JSON_CONTAINS_PATH(metadata, 'one', '$.cancel_attempted_at')

-- name: notification_cursor
SELECT id FROM {schema}river_notification ORDER BY id FOR UPDATE

-- name: schedule_select
SELECT id,unique_key FROM {schema}river_job WHERE state IN ('scheduled','retryable') AND scheduled_at<=? ORDER BY priority,scheduled_at,id LIMIT 10000 FOR UPDATE SKIP LOCKED

-- name: schedule_collision
SELECT id FROM {schema}river_job WHERE id<>? AND unique_key=? AND unique_states IS NOT NULL AND (unique_states & CASE state WHEN 'available' THEN 1 WHEN 'cancelled' THEN 2 WHEN 'completed' THEN 4 WHEN 'discarded' THEN 8 WHEN 'pending' THEN 16 WHEN 'retryable' THEN 32 WHEN 'running' THEN 64 WHEN 'scheduled' THEN 128 ELSE 0 END) != 0 FOR UPDATE

-- name: schedule_available
UPDATE {schema}river_job SET state='available' WHERE id=? AND state IN ('scheduled','retryable')

-- name: schedule_discard
UPDATE {schema}river_job SET finalized_at=?,
metadata=JSON_SET(metadata, '$.unique_key_conflict', 'scheduler_discarded'), state='discarded'
WHERE id=? AND state IN ('scheduled','retryable')

-- name: clean_jobs
DELETE FROM {schema}river_job WHERE id IN ({ids})

-- name: clean_jobs_select
SELECT id FROM {schema}river_job
WHERE (state='cancelled' AND finalized_at<?) OR (state='completed' AND finalized_at<?) OR (state='discarded' AND finalized_at<?)
ORDER BY id LIMIT 1000 FOR UPDATE SKIP LOCKED

-- name: clean_queues
DELETE FROM {schema}river_queue WHERE updated_at<?

-- name: clean_notifications
DELETE FROM {schema}river_notification WHERE created_at<? ORDER BY created_at,id LIMIT 1000

-- name: rescue_update
WITH input AS (SELECT ? AS state, ? AS finalized_at, ? AS scheduled_at, ? AS attempt,
CAST(? AS JSON) AS metadata, ? AS error_present, CAST(? AS JSON) AS error, ? AS id, ? AS old_attempt, ? AS attempted_at)
UPDATE {schema}river_job j JOIN input i ON j.id=i.id SET
j.finalized_at=i.finalized_at, j.scheduled_at=i.scheduled_at, j.attempt=i.attempt, j.metadata=i.metadata,
j.errors=CASE WHEN i.error_present IS NULL THEN j.errors ELSE JSON_ARRAY_APPEND(CASE WHEN j.errors IS NULL THEN JSON_ARRAY() WHEN JSON_TYPE(j.errors)='ARRAY' THEN j.errors ELSE JSON_ARRAY(j.errors) END,'$',i.error) END,
j.state=i.state
WHERE j.state='running' AND j.attempt=i.old_attempt AND j.attempted_at=i.attempted_at

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

-- name: inserted
SELECT {columns} FROM {schema}river_job WHERE id = LAST_INSERT_ID() FOR UPDATE

-- name: delete_rows
SELECT {columns} FROM {schema}river_job WHERE id=? AND state != 'running' FOR UPDATE

-- name: empty_jobs
SELECT {columns} FROM {schema}river_job WHERE id=? AND false

-- name: empty_queues
SELECT name,created_at,metadata,paused_at,updated_at FROM {schema}river_queue WHERE name=? AND false

-- name: filter_metadata
JSON_CONTAINS(metadata, CAST(? AS JSON))

-- name: leader_elected
SELECT elected_at FROM {schema}river_leader WHERE leader_id=CAST(? AS BINARY) AND elected_at=? FOR UPDATE

-- name: scheduled_queue
SELECT queue FROM {schema}river_job WHERE id=? FOR UPDATE

-- name: migration_lock
SELECT GET_LOCK(CONCAT('river_migrate:',COALESCE(NULLIF('{schema_name}',''),DATABASE())),0)

-- name: migration_unlock
SELECT RELEASE_LOCK(CONCAT('river_migrate:',COALESCE(NULLIF('{schema_name}',''),DATABASE())))

-- name: create_schema
CREATE DATABASE IF NOT EXISTS `{schema_name}`

-- name: claim_select
SELECT id FROM {schema}river_job WHERE queue=? AND queue=CAST(? AS BINARY) AND state='available' AND scheduled_at<=?
AND NOT EXISTS(SELECT 1 FROM {schema}river_queue q WHERE q.name=CAST(river_job.queue AS BINARY) AND paused_at IS NOT NULL)
ORDER BY priority,scheduled_at,id LIMIT ? FOR UPDATE SKIP LOCKED

-- name: claim_known_select
SELECT id FROM {schema}river_job WHERE queue=? AND queue=CAST(? AS BINARY) AND state='available' AND scheduled_at<=?
AND NOT EXISTS(SELECT 1 FROM {schema}river_queue q WHERE q.name=CAST(river_job.queue AS BINARY) AND paused_at IS NOT NULL)
AND CAST(kind AS BINARY) IN (SELECT value FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) AS wanted)
ORDER BY priority,scheduled_at,id LIMIT ? FOR UPDATE SKIP LOCKED

-- name: claim_one
UPDATE {schema}river_job SET attempted_at=?,
attempted_by=JSON_ARRAY_APPEND(CASE WHEN JSON_LENGTH(attempted_by)>=100 THEN JSON_EXTRACT(attempted_by, '$[last-98 to last]') ELSE COALESCE(attempted_by,JSON_ARRAY()) END, '$', ?),
attempt=attempt+1, state='running' WHERE id=?

-- name: claimed
SELECT {columns} FROM JSON_TABLE(CAST(? AS JSON), '$[*]' COLUMNS(value BIGINT PATH '$')) AS wanted STRAIGHT_JOIN {schema}river_job FORCE INDEX (PRIMARY) ON river_job.id=wanted.value ORDER BY priority,scheduled_at,id

-- name: filter_metadata_container
({type}='{kind}' AND {children})

-- name: filter_metadata_element
(SELECT 1 FROM JSON_TABLE(CASE WHEN {type}='{kind}' THEN {value} END, '$[*]' COLUMNS(value JSON PATH '$')) AS {alias} WHERE {predicate} LIMIT 1) IS NOT NULL

-- name: filter_metadata_member
(SELECT 1 FROM JSON_TABLE(JSON_EXTRACT(CASE WHEN {type}='{kind}' THEN {value} END, ?), '$' COLUMNS(value JSON PATH '$')) AS {alias} WHERE {predicate} LIMIT 1) IS NOT NULL

-- name: filter_metadata_scalar
(SELECT 1 FROM JSON_TABLE(CAST(? AS JSON), '$' COLUMNS(value JSON PATH '$')) AS wanted
WHERE {type} NOT IN ('object','array') AND ({value} IS NULL AND wanted.value IS NULL OR JSON_CONTAINS({value}, wanted.value)) LIMIT 1) IS NOT NULL

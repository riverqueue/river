-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock(@key);

-- name: PGGetProductAndVersion :one
SELECT
    version()::text AS product,
    current_setting('server_version_num')::int AS version_num,
    coalesce(current_setting('yb_enable_listen_notify', true), 'off')::boolean AS yb_listen_notify_enabled;

-- name: PGNotifyMany :exec
WITH topic_to_notify AS (
    SELECT
        concat(coalesce(sqlc.narg('schema')::text, current_schema()), '.', @topic::text) AS topic,
        unnest(@payload::text[]) AS payload
)
SELECT pg_notify(
    topic_to_notify.topic,
    topic_to_notify.payload
  )
FROM topic_to_notify;

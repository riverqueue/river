-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock(@key);

-- name: PGGetProductAndVersion :one
SELECT
    version()::text AS product,
    current_setting('server_version_num')::int AS version_num;

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

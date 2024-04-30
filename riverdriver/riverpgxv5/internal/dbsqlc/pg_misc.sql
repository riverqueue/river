-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock(@key);

-- name: PGNotifyMany :exec
WITH topic_to_notify AS (
    SELECT
        concat(current_schema(), '.', @topic::text) AS topic,
        unnest(@payload::text[]) AS payload
)
SELECT pg_notify(
    topic_to_notify.topic,
    topic_to_notify.payload
  )
FROM topic_to_notify;

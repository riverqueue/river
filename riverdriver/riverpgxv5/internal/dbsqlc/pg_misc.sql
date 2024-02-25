-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock(@key);

-- name: PGNotify :exec
SELECT pg_notify(@topic, @payload);
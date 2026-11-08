-- Successful jobs without other updates do not need to materialize old state
-- or inspect cancellation metadata. Completion wins over a cancellation request.
UPDATE /* TEMPLATE: schema */river_job AS job
JOIN JSON_TABLE(?, '$[*]' COLUMNS (
    id BIGINT PATH '$.id' ERROR ON ERROR,
    finalized_at DATETIME(6) PATH '$.finalized_at' ERROR ON ERROR
)) AS batch ON batch.id = job.id
SET job.state = 'completed',
    job.finalized_at = COALESCE(batch.finalized_at, job.finalized_at)
WHERE job.state = 'running';

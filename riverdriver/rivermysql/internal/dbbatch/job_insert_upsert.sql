-- MySQL cannot target a particular unique index. Only accept a conflict when
-- both rows enforce River uniqueness for the same key. An invalid JSON document
-- makes unrelated conflicts fail atomically, including outside strict SQL mode.
ON DUPLICATE KEY UPDATE id = IF(
    batch.unique_active
    AND river_job.unique_key = UNHEX(batch.unique_key)
    AND CASE river_job.state
        WHEN 'available' THEN river_job.unique_states & (1 << 0)
        WHEN 'cancelled' THEN river_job.unique_states & (1 << 1)
        WHEN 'completed' THEN river_job.unique_states & (1 << 2)
        WHEN 'discarded' THEN river_job.unique_states & (1 << 3)
        WHEN 'pending'   THEN river_job.unique_states & (1 << 4)
        WHEN 'retryable' THEN river_job.unique_states & (1 << 5)
        WHEN 'running'   THEN river_job.unique_states & (1 << 6)
        WHEN 'scheduled' THEN river_job.unique_states & (1 << 7)
        ELSE 0
    END >= 1,
    river_job.id,
    CAST('non-River unique constraint conflict' AS JSON)
)

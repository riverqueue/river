-- Match the stored index expression, including its explicit string charset.
CASE WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL AND
        CASE state
            WHEN _utf8mb4'available' THEN unique_states & (1 << 0)
            WHEN _utf8mb4'cancelled' THEN unique_states & (1 << 1)
            WHEN _utf8mb4'completed' THEN unique_states & (1 << 2)
            WHEN _utf8mb4'discarded' THEN unique_states & (1 << 3)
            WHEN _utf8mb4'pending'   THEN unique_states & (1 << 4)
            WHEN _utf8mb4'retryable' THEN unique_states & (1 << 5)
            WHEN _utf8mb4'running'   THEN unique_states & (1 << 6)
            WHEN _utf8mb4'scheduled' THEN unique_states & (1 << 7)
            ELSE 0
        END >= 1
    THEN unique_key
    ELSE NULL
    END

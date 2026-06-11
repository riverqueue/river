--
-- Notification outbox rollback.
--

DROP TABLE /* TEMPLATE: schema */river_notification;

--
-- SQLite JSONB conversion rollback.
--
-- No-op. PostgreSQL already stores River JSON columns as jsonb.

--
-- Normally `river_job` and `river_job_notify()` are dropped here, but since
-- SQLite was added well after 002 came about, we push that to version 006 index.
--

DROP TABLE /* TEMPLATE: schema */river_job;

DROP TABLE /* TEMPLATE: schema */river_leader;
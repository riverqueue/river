/**
 * Transactional River jobs on PostgreSQL and SQLite. Define a {@link com.riverqueue.JobType},
 * insert through a {@link com.riverqueue.Client}, and register worker lambdas through {@link
 * com.riverqueue.Workers.Builder}. Only a running {@code Workers} instance owns resources; close it
 * or call {@code stop()} when the application stops.
 *
 * <p>Connection-first overloads require an existing JDBC transaction with auto-commit disabled.
 * They use savepoints and leave commit, rollback, and close to the caller. Other overloads open and
 * close their own connections. Database errors become {@link com.riverqueue.RiverException};
 * application callbacks may throw checked exceptions, including {@link java.sql.SQLException}.
 *
 * <p>Builders are mutable configuration objects. Jobs are database snapshots; changing their JSON
 * values does not update the database. Record components may be null where River has no
 * corresponding value, such as a job's finalization time. Missing jobs throw an exception with code
 * {@code NOT_FOUND}.
 *
 * <p>{@link com.riverqueue.Plugin}, {@link com.riverqueue.Client.Driver}, and members explicitly
 * marked internal are integration seams for the matching Pro release, not stable application APIs.
 */
package com.riverqueue;

# Running Rust and Go River together

River's Rust and Go clients share one database schema and protocol. A Rust
service can insert jobs that Go workers run, work jobs that Go services insert,
and take part in leader election and maintenance alongside Go clients. This
guide covers what must match between them and how to roll Rust into an
existing Go deployment and back out again.

## Matching versions

Each `riverqueue` minor release is matched to the River Go minor release with
the same number: `riverqueue` 0.48 runs alongside River Go 0.48. Patch releases
of either implementation can be mixed within a matched minor release. Upgrade
both implementations together when moving to a new minor release, following
the rolling procedure below.

Every River client in a deployment must understand the same schema. Run
migrations once with either implementation, before starting clients of the
new version:

- Go: `river migrate-up`, or `rivermigrate` from Go code.
- Rust: `riverqueue migrate-up` from `riverqueue-cli`, or `PostgresMigrator`/
  `SqliteMigrator` from `riverqueue-migrate`.

Both write the same `river_migration` history, so either can migrate a database
the other created.

## Queues and job kinds

Clients fetch work by queue, not by kind. A client that fetches a job whose
kind it has no worker for records a retryable "job kind is not registered"
error, and the job is retried until another client works it or it runs out of
attempts. Give each language its own queues for the kinds only it works:

```text
Go:   queues "default", "billing"      workers for billing kinds
Rust: queues "rust_default", "images"  workers for image kinds
```

Either language can insert into any queue.

The elected leader runs maintenance for the whole database, including the job
rescuer. Like River Go, the rescuer discards stuck jobs whose kind the leader
has no worker for rather than retrying them. If kinds are split between
languages, either register a worker for every kind in every client that can be
elected leader, or keep clients that don't know every kind from running
maintenance.

## Unique jobs

Unique keys hash job arguments as encoded JSON, so unique jobs inserted from
both languages must serialize the same way:

- Use the same JSON field names. Go uses struct tags; Rust follows Serde's
  rename rules.
- Unique fields are compared by their selected top-level keys, which are
  sorted. Nested objects keep their field order, so a nested Go struct needs
  the same field order in Rust, and a Go map needs sorted keys in Rust (a
  `BTreeMap`). Selecting individual scalar fields avoids depending on nested
  order.
- Rust encodes floating point numbers and escapes strings the way Go's
  `encoding/json` does, so `1.0` hashes the same as Go's `1`.
- `ByPeriod` periods are measured in UTC from the job's scheduled time. River
  Go releases before the version that includes this behavior derive periods
  from insertion time in the process's local time zone; don't mix those
  releases with Rust clients for scheduled or non-UTC by-period jobs.

## Periodic jobs

Periodic jobs are enqueued only by the elected leader. Configure the same
periodic jobs, with the same IDs and schedules, in every client that can become
leader, whichever language it's written in; otherwise a job stops being
enqueued whenever a client without it is elected.

Cron schedules use Go River's standard five-field syntax in both languages.

## SQLite

SQLite databases shared between processes must use WAL mode and a busy timeout
in every process. Go clients sharing a SQLite file with Rust should open it with
`_txlock=immediate` so that transactions that read before writing don't fail
with `SQLITE_BUSY` when a Rust process commits in between.

## Rolling deployment

1. Upgrade River Go to the matched release and run migrations.
2. Deploy a small number of Rust clients alongside the Go clients.
3. Watch queue depth, retries, rescued jobs, leadership changes, and database
   connection counts while increasing Rust's share.
4. Keep at least one Go deployment available until you're confident in the
   Rust services.

## Rolling back

Rolling back doesn't touch the schema. Stop Rust clients gracefully with
`RunHandle::shutdown` and let the Go clients continue. Jobs that Rust inserted
are ordinary River rows that Go workers can run, and any job a stopped Rust
client left running is recovered by the rescuer. Only migrate down as a
separately planned operation once no deployed client needs the newer schema.

// Package riverfdb provides an experimental FoundationDB driver for River.
//
// Build with -tags foundationdb and install the FoundationDB C client library
// and headers. The application must select an API version with fdb.APIVersion
// before opening its database. River does not select a process-wide API version.
//
// This driver supports watch-based notifications, polling, transactional inserts,
// unique jobs, job state transitions, queues, and leader election. SQL migrations,
// SQL job listing and bulk deletion, advisory locks, and nested transactions are not
// supported. Unsupported operations return riverdriver.ErrNotImplemented.
//
// Each driver owns a caller-supplied, nonempty key prefix. Client.Schema further
// partitions that prefix. No SQL migration is necessary. Use disjoint prefixes
// for separate applications and never write application data inside a driver's
// prefix. The experimental storage format is not yet stable.
//
// FoundationDB transactions are short lived and size limited. Keep batches and
// externally managed transactions small. Standalone operations retry transaction
// conflicts; callers using InsertTx must retry their entire FoundationDB
// transaction, preferably with Database.Transact. All writes in the callback
// must be safe to retry, as required by FoundationDB.
//
// This initial implementation uses a transactional ID counter and scans for
// some maintenance and reporting operations. It is intended for evaluation on
// small datasets, not production scale. Job records must fit in a 100,000-byte
// FoundationDB value, including JSON encoding overhead. Clients must have
// synchronized clocks because scheduling and leases use application time.
package riverfdb

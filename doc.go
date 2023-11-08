/*
Package river is a robust high-performance job processing system for Go.

Because it is built using Postgres, River enables you to use the same database
for both your application data and your job queue. This simplifies operations,
but perhaps more importantly it makes it possible to enqueue jobs
transactionally with other database changes. This avoids a whole class of
distributed systems issues like jobs that execute before the database
transaction that enqueued them has even committed, or jobs that attempt to
utilize database changes which were rolled back. It also makes it possible for
your job to make database changes atomically with the job being marked as
complete.

# Job args

Jobs need to be able to serialize their state to JSON so that they can round
tripped from the database and back. Each job has an args struct with JSON tags
on its properties to allow for this:

	// SortArgs are arguments for SortWorker.
	type SortArgs struct {
		// Strings is a slice of strings to sort.
		Strings []string `json:"strings"`
	}

	func (SortArgs) Kind() string { return "sort_job" }

Args are created to enqueue a new job and are what a worker receives to work
one. Each one implements [JobArgs].Kind, which returns a unique string that's
used to recognize the job as it round trips from the database.

# Job workers

Each job kind also has a corresponding worker struct where its core work
function is defined:

	// SortWorker is a job worker for sorting strings.
	type SortWorker struct {
		river.WorkerDefaults[SortArgs]
	}

	func (w *SortWorker) Work(ctx context.Context, job *river.Job[SortArgs]) error {
		sort.Strings(job.Args.Strings)
		fmt.Printf("Sorted strings: %+v\n", job.Args.Strings)
		return nil
	}

A few details to notice:

  - Although not strictly necessary, workers embed [WorkerDefaults] with a
    reference to their args type. This allows them to inherit defaults for the
    [Worker] interface, and helps futureproof in case its ever expanded.

  - Each worker implements [Worker].Work, which is where the async heavy-lifting
    for a background job is done. Work implementations receive a generic like
    river.Job[SortArgs] for easy access to job arguments.

# Registering workers

As a program is initially starting up, worker structs are registered so that
River can know how to work them:

	workers := river.NewWorkers()
	river.AddWorker(workers, &SortWorker{})

# River client

The main River client takes a [pgx] connection pool wrapped with River's Pgx v5
driver using [riverpgxv5.New] and a set of registered workers (see above). Each
queue can receive configuration like the maximum number of goroutines that'll be
used to work it:

	dbConfig, err := pgxpool.ParseConfig("postgres://localhost/river")
	if err != nil {
		return err
	}

	dbPool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	riverClient, err := river.NewClient(&river.Config{
		Driver: riverpgxv5.New(dbPool),
		Queues: map[string]river.QueueConfig{
			river.DefaultQueue: {MaxWorkers: 100},
		},
		Workers: workers,
	})

	if err := riverClient.Start(ctx); err != nil {
		...
	}

	...

	// Before program exit, try to shut down cleanly.
	if err := riverClient.Shutdown(ctx); err != nil {
		return err
	}

For programs that'll be inserting jobs only, the Queues and Workers
configuration keys can be omitted for brevity:

	riverClient, err := river.NewClient(&river.Config{
		DBPool: dbPool,
	})

However, if Workers is specified, the client can validate that an inserted job
has a worker that's registered with the workers bundle, so it's recommended that
Workers is configured anyway if your project is set up to easily allow it.

See [Config] for details on all configuration options.

# Inserting jobs

Insert jobs by opening a transaction and calling [Client.InsertTx] with a job
args instance (a non-transactional [Client.Insert] is also available) and the
transaction wrapped with [riverpgxv5Tx]:

	tx, err := dbPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = riverClient.InsertTx(ctx, tx, SortArgs{
		Strings: []string{
			"whale", "tiger", "bear",
		},
	}, nil)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

Due to rules around transaction visibility, inserted jobs aren't visible to
workers until the transaction that inserted them is committed. This prevents
a whole host of problems like workers trying to work a job before its viable to
do so because not all its requisite data has been persisted yet.

See the InsertAndWork example for all this code in one place.

# Other features

  - Periodic jobs that run on a predefined interval. See the PeriodicJob example
    below.

# Verifying inserted jobs

See the rivertest package for test helpers that can be used to easily verified
inserted jobs in a test suite. For example:

	job := rivertest.RequireInserted(ctx, t, dbPool, &RequiredArgs{}, nil)
	fmt.Printf("Test passed with message: %s\n", job.Args.Message)

[pgx]: https://github.com/jackc/pgx
*/
package river

import "github.com/riverqueue/river/riverdriver/riverpgxv5"

// This is really dumb, but the package must be imported to make it linkable the
// Godoc above, so this is a trivial import to make sure it is.
var _ = riverpgxv5.New(nil)

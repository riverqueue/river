/*
Package river is a robust high-performance job processing system for Go and
Postgres.

See [homepage], [docs], and [godoc], as well as the [River UI].

Being built for Postgres, River encourages the use of the same database for
application data and job queue. By enqueueing jobs transactionally along with
other database changes, whole classes of distributed systems problems are
avoided. Jobs are guaranteed to be enqueued if their transaction commits, are
removed if their transaction rolls back, and aren't visible for work _until_
commit. See [transactional enqueueing] for more background on this philosophy.

# Job args and workers

Jobs are defined in struct pairs, with an implementation of [`JobArgs`] and one
of [`Worker`].

Job args contain `json` annotations and define how jobs are serialized to and
from the database, along with a "kind", a stable string that uniquely identifies
the job.

	type SortArgs struct {
		// Strings is a slice of strings to sort.
		Strings []string `json:"strings"`
	}

	func (SortArgs) Kind() string { return "sort" }

Workers expose a `Work` function that dictates how jobs run.

	type SortWorker struct {
	    // An embedded WorkerDefaults sets up default methods to fulfill the rest of
	    // the Worker interface:
	    river.WorkerDefaults[SortArgs]
	}

	func (w *SortWorker) Work(ctx context.Context, job *river.Job[SortArgs]) error {
	    sort.Strings(job.Args.Strings)
	    fmt.Printf("Sorted strings: %+v\n", job.Args.Strings)
	    return nil
	}

# Registering workers

Jobs are uniquely identified by their "kind" string. Workers are registered on
start up so that River knows how to assign jobs to workers:

	workers := river.NewWorkers()
	// AddWorker panics if the worker is already registered or invalid:
	river.AddWorker(workers, &SortWorker{})

# Starting a client

A River [`Client`] provides an interface for job insertion and manages job
processing and [maintenance services]. A client's created with a database pool,
[driver], and config struct containing a `Workers` bundle and other settings.
Here's a client `Client` working one queue (`"default"`) with up to 100 worker
goroutines at a time:

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
	    Queues: map[string]river.QueueConfig{
	        river.QueueDefault: {MaxWorkers: 100},
	    },
	    Workers: workers,
	})
	if err != nil {
	    panic(err)
	}

	// Run the client inline. All executed jobs will inherit from ctx:
	if err := riverClient.Start(ctx); err != nil {
	    panic(err)
	}

## Insert-only clients

It's often desirable to have a client that'll be used for inserting jobs, but
not working them. This is possible by omitting the `Queues` configuration, and
skipping the call to `Start`:

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
	    Workers: workers,
	})
	if err != nil {
	    panic(err)
	}

`Workers` can also be omitted, but it's better to include it so River can check
that inserted job kinds have a worker that can run them.

## Stopping

The client should also be stopped on program shutdown:

	// Stop fetching new work and wait for active jobs to finish.
	if err := riverClient.Stop(ctx); err != nil {
	    panic(err)
	}

There are some complexities around ensuring clients stop cleanly, but also in a
timely manner. See [graceful shutdown] for more details on River's stop modes.

# Inserting jobs

[`Client.InsertTx`] is used in conjunction with an instance of job args to
insert a job to work on a transaction:

	_, err = riverClient.InsertTx(ctx, tx, SortArgs{
	    Strings: []string{
	        "whale", "tiger", "bear",
	    },
	}, nil)

	if err != nil {
	    panic(err)
	}

See the [`InsertAndWork` example] for complete code.

# Other features

  - [Batch job insertion] for efficiently inserting many jobs at once using
    Postgres `COPY FROM`.

  - [Cancelling jobs] from inside a work function.

  - [Error and panic handling].

  - [Multiple queues] to better guarantee job throughput, worker availability,
    and isolation between components.

  - [Periodic and cron jobs].

  - [Scheduled jobs] that run automatically at their scheduled time in the
    future.

  - [Snoozing jobs] from inside a work function.

  - [Subscriptions] to queue activity and statistics, providing easy hooks for
    telemetry like logging and metrics.

  - [Test helpers] to verify that jobs are inserted as expected.

  - [Transactional job completion] to guarantee job completion commits with
    other changes in a transaction.

  - [Unique jobs] by args, period, queue, and state.

  - [Web UI] for inspecting and interacting with jobs and queues.

  - [Work functions] for simplified worker implementation.

# Development

See [developing River].

[`Client`]: https://pkg.go.dev/github.com/riverqueue/river#Client
[`Client.InsertTx`]: https://pkg.go.dev/github.com/riverqueue/river#Client.InsertTx
[`InsertAndWork` example]: https://pkg.go.dev/github.com/riverqueue/river#example-package-InsertAndWork
[`JobArgs`]: https://pkg.go.dev/github.com/riverqueue/river#JobArgs
[`Worker`]: https://pkg.go.dev/github.com/riverqueue/river#Worker
[Batch job insertion]: https://riverqueue.com/docs/batch-job-insertion
[Cancelling jobs]: https://riverqueue.com/docs/cancelling-jobs
[Error and panic handling]: https://riverqueue.com/docs/error-handling
[Multiple queues]: https://riverqueue.com/docs/multiple-queues
[Periodic and cron jobs]: https://riverqueue.com/docs/periodic-jobs
[River UI]: https://github.com/riverqueue/riverui
[Scheduled jobs]: https://riverqueue.com/docs/scheduled-jobs
[Snoozing jobs]: https://riverqueue.com/docs/snoozing-jobs
[Subscriptions]: https://riverqueue.com/docs/subscriptions
[Test helpers]: https://riverqueue.com/docs/testing
[Transactional job completion]: https://riverqueue.com/docs/transactional-job-completion
[Unique jobs]: https://riverqueue.com/docs/unique-jobs
[Web UI]: https://github.com/riverqueue/riverui
[Work functions]: https://riverqueue.com/docs/work-functions
[docs]: https://riverqueue.com/docs
[developing River]: https://github.com/riverqueue/river/blob/master/docs/development.md
[driver]: https://riverqueue.com/docs/database-drivers
[godoc]: https://pkg.go.dev/github.com/riverqueue/river
[graceful shutdown]: https://riverqueue.com/docs/graceful-shutdown
[homepage]: https://riverqueue.com
[maintenance services]: https://riverqueue.com/docs/maintenance-services
[transactional enqueueing]: https://riverqueue.com/docs/transactional-enqueueing
*/
package river

//go:build foundationdb

package riverdrivertest

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverfdb"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

func TestFoundationDB(t *testing.T) {
	t.Parallel()

	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		t.Skip("set FDB_CLUSTER_FILE to run FoundationDB integration tests")
	}
	require.NoError(t, fdb.APIVersion(730))
	db, err := fdb.OpenDatabase(clusterFile)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	type testBundle struct {
		driver *riverfdb.Driver
		exec   riverdriver.Executor
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		prefix := []byte("river-test/" + rand.Text() + "/")
		driver, err := riverfdb.New(db, prefix)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				if err := tx.Options().SetTimeout(5000); err != nil {
					return nil, err
				}
				keyRange, err := fdb.PrefixRange(prefix)
				if err != nil {
					return nil, err
				}
				tx.ClearRange(keyRange)
				return struct{}{}, nil
			})
			require.NoError(t, err)
		})
		return &testBundle{driver: driver, exec: driver.GetExecutor()}
	}

	t.Run("BatchFailureIsAtomic", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		params := &riverdriver.JobInsertFastManyParams{Jobs: []*riverdriver.JobInsertFastParams{
			{EncodedArgs: []byte(`{}`), Kind: "good", MaxAttempts: 25, Priority: 1, Queue: "default", State: rivertype.JobStateAvailable},
			{EncodedArgs: []byte(`{}`), Kind: "bad", MaxAttempts: 25, Priority: 9, Queue: "default", State: rivertype.JobStateAvailable},
		}}
		_, err := bundle.exec.JobInsertFastMany(ctx, params)
		require.Error(t, err)
		count, err := bundle.exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{State: rivertype.JobStateAvailable})
		require.NoError(t, err)
		require.Zero(t, count)

		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback(context.Background())) })
		_, err = tx.JobInsertFastMany(ctx, params)
		require.Error(t, err)
		require.Error(t, tx.Commit(ctx))
		count, err = bundle.exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{State: rivertype.JobStateAvailable})
		require.NoError(t, err)
		require.Zero(t, count)
	})

	t.Run("CanceledContext", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, bundle.exec.Ping(ctx), context.Canceled)
		_, err := bundle.exec.Begin(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Client", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		workers := river.NewWorkers()
		river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[foundationDBArgs]) error {
			if job.Args.Fail && job.Attempt == 1 {
				return errors.New("try again")
			}
			if job.Args.CompleteTx {
				_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
					return river.JobCompleteTx[*riverfdb.Driver](ctx, tx, job)
				})
				return err
			}
			return nil
		}))
		client, err := river.NewClient(bundle.driver, &river.Config{
			FetchCooldown: time.Millisecond, FetchPollInterval: 10 * time.Millisecond,
			Queues: map[string]river.QueueConfig{"default": {MaxWorkers: 2}}, ReindexerIndexNames: []string{},
			RetryPolicy: &foundationDBRetryPolicy{}, Workers: workers,
		})
		require.NoError(t, err)
		require.NoError(t, client.Start(ctx))
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, client.Stop(stopCtx))
		})
		inserted, err := client.Insert(ctx, foundationDBArgs{Fail: true}, nil)
		require.NoError(t, err)
		transactional, err := client.Insert(ctx, foundationDBArgs{CompleteTx: true}, nil)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			job, err := client.JobGet(ctx, inserted.Job.ID)
			if err != nil || job.State != rivertype.JobStateCompleted || job.Attempt != 2 || len(job.Errors) != 1 {
				return false
			}
			job, err = client.JobGet(ctx, transactional.Job.ID)
			return err == nil && job.State == rivertype.JobStateCompleted && job.Attempt == 1
		}, 10*time.Second, 10*time.Millisecond)
	})

	for _, transactional := range []bool{false, true} {
		name := "ClientCancelRunningJob"
		if transactional {
			name += "Tx"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			exerciseClientCancelRunningJob(context.Background(), t, setup(t).driver, "", false, transactional)
		})
	}

	t.Run("ClientNotifications", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		exerciseClientNotifications(context.Background(), t, bundle.driver, "")
	})

	t.Run("CommitConflict", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})
		tx1, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx1.Rollback(context.Background())) })
		tx2, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx2.Rollback(context.Background())) })
		params := &riverdriver.JobGetAvailableParams{ClientID: "test", MaxAttemptedBy: 5, MaxToLock: 1, Queue: "default"}
		first, err := tx1.JobGetAvailable(ctx, params)
		require.NoError(t, err)
		second, err := tx2.JobGetAvailable(ctx, params)
		require.NoError(t, err)
		require.Len(t, first, 1)
		require.Len(t, second, 1)
		require.Equal(t, first[0].ID, second[0].ID)
		require.NoError(t, tx1.Commit(ctx))
		require.Error(t, tx2.Commit(ctx))
	})

	t.Run("CommitRespectsCancellation", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback(context.Background())) })
		job := testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})
		cancel()
		require.ErrorIs(t, tx.Commit(ctx), context.Canceled)
		_, err = bundle.exec.JobGetByID(t.Context(), &riverdriver.JobGetByIDParams{ID: job.ID})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("ConcurrentClaims", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		for range 20 {
			_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})
		}
		var (
			mu   sync.Mutex
			seen = make(map[int64]bool)
			wg   sync.WaitGroup
		)
		for range 8 {
			wg.Go(func() {
				for {
					jobs, err := bundle.exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{ClientID: rand.Text(), MaxAttemptedBy: 5, MaxToLock: 3, Queue: "default"})
					if err != nil {
						t.Error(err)
						return
					}
					if len(jobs) == 0 {
						return
					}
					mu.Lock()
					for _, job := range jobs {
						if seen[job.ID] {
							t.Errorf("job %d claimed twice", job.ID)
						}
						seen[job.ID] = true
					}
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		require.Len(t, seen, 20)
	})

	t.Run("ConcurrentUniqueInsert", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{Jobs: []*riverdriver.JobInsertFastParams{{
					EncodedArgs: []byte(`{}`), Kind: "unique", MaxAttempts: 25, Priority: 1, Queue: "default", State: rivertype.JobStateAvailable,
					UniqueKey: []byte("concurrent"), UniqueStates: uniquestates.UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable}),
				}}})
				if err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		count, err := bundle.exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{State: rivertype.JobStateAvailable})
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("Conformance", func(t *testing.T) {
		t.Parallel()

		ExerciseCore(t.Context(), t, func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[fdb.Transaction]) {
			t.Helper()

			bundle := setup(t)
			return bundle.exec, bundle.driver
		})
	})

	t.Run("Listener", func(t *testing.T) {
		t.Parallel()

		exerciseListener(t.Context(), t, func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[fdb.Transaction], string) {
			t.Helper()

			return setup(t).driver, ""
		})
	})

	t.Run("ListenerWatches", func(t *testing.T) {
		t.Parallel()

		exerciseFoundationDBWatches(t, func(t *testing.T) *riverfdb.Driver {
			t.Helper()
			return setup(t).driver
		})
	})

	t.Run("PrefixValidation", func(t *testing.T) {
		t.Parallel()

		for _, prefix := range [][]byte{nil, {}, {0xff}, make([]byte, 1025)} {
			_, err := riverfdb.New(db, prefix)
			require.Error(t, err)
		}
		_, err := riverfdb.New(fdb.Database{}, []byte("river/"))
		require.Error(t, err)
	})

	t.Run("RecordLimit", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		_, err := bundle.exec.JobInsertFastMany(t.Context(), &riverdriver.JobInsertFastManyParams{Jobs: []*riverdriver.JobInsertFastParams{{
			EncodedArgs: []byte(`{"large":"` + strings.Repeat("a", 100_000) + `"}`), Kind: "large", MaxAttempts: 25, Priority: 1, Queue: "default", State: rivertype.JobStateAvailable,
		}}})
		require.ErrorContains(t, err, "FoundationDB permits 100000")
	})

	t.Run("SchemaIsolation", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})
		_, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: "other"})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		other := setup(t)
		_, err = other.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("Transactions", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		job := testfactory.Job(ctx, t, tx, &testfactory.JobOpts{})
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		require.NoError(t, tx.Rollback(ctx))
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		client, err := river.NewClient(bundle.driver, &river.Config{})
		require.NoError(t, err)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			if err := tx.Options().SetTimeout(5000); err != nil {
				return nil, err
			}
			result, err := client.InsertTx(ctx, tx, foundationDBArgs{}, nil)
			if err != nil {
				return nil, err
			}
			job = result.Job
			return result, nil
		})
		require.NoError(t, err)
		stored, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)
		require.Equal(t, job.ID, stored.ID)
	})
}

type foundationDBArgs struct {
	CompleteTx bool `json:"complete_tx"`
	Fail       bool `json:"fail"`
}

func (foundationDBArgs) Kind() string { return "foundationdb_test" }

type foundationDBRetryPolicy struct{}

func (*foundationDBRetryPolicy) NextRetry(*rivertype.JobRow) time.Time { return time.Now() }

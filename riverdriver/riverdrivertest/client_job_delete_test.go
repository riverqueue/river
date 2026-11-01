package riverdrivertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

// exerciseClientJobDeleteManyWhere exercises custom delete predicates through
// the public client API on every driver.
func exerciseClientJobDeleteManyWhere[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T) (riverdriver.Driver[TTx], string),
) {
	t.Helper()

	t.Run("JobDeleteManyWhere", func(t *testing.T) {
		t.Parallel()

		type testBundle struct {
			client *river.Client[TTx]
			driver riverdriver.Driver[TTx]
			exec   riverdriver.Executor
			schema string
		}

		setup := func(t *testing.T) *testBundle {
			t.Helper()

			driver, schema := driverWithSchema(ctx, t)
			client, err := river.NewClient(driver, newTestConfig(t, schema))
			require.NoError(t, err)

			return &testBundle{
				client: client,
				driver: driver,
				exec:   driver.GetExecutor(),
				schema: schema,
			}
		}

		t.Run("ArgsWithKindFilter", func(t *testing.T) {
			t.Parallel()

			bundle := setup(t)

			var (
				job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{EncodedArgs: []byte(`{"customer_id": 1}`), Kind: new("send_email"), Schema: bundle.schema})
				job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{EncodedArgs: []byte(`{"customer_id": 2}`), Kind: new("send_email"), Schema: bundle.schema})
				job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{EncodedArgs: []byte(`{"customer_id": 3}`), Kind: new("send_email"), Schema: bundle.schema})
				job4 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{EncodedArgs: []byte(`{"customer_id": 1}`), Kind: new("other_kind"), Schema: bundle.schema})
			)

			predicate := "(args->>'customer_id')::bigint IN (@customer_a, @customer_b)"
			if bundle.driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
				predicate = "args ->> '$.customer_id' IN (@customer_a, @customer_b)"
			}

			deleteRes, err := bundle.client.JobDeleteMany(ctx, river.NewJobDeleteManyParams().
				Kinds("send_email").
				Where(predicate, river.NamedArgs{"customer_a": int64(1), "customer_b": int64(2)}))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			_, err = bundle.client.JobGet(ctx, job3.ID)
			require.NoError(t, err)
			_, err = bundle.client.JobGet(ctx, job4.ID)
			require.NoError(t, err)
		})

		t.Run("OrDoesNotWidenDeletion", func(t *testing.T) {
			t.Parallel()

			bundle := setup(t)

			var (
				job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: new("target_kind"), Priority: new(1), Schema: bundle.schema})
				job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: new("target_kind"), Priority: new(1), Schema: bundle.schema, State: new(rivertype.JobStateRunning)})
				job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: new("other_kind"), Priority: new(2), Schema: bundle.schema})
			)

			// Without parentheses, the OR bypasses both the kind filter and
			// the running-job guard, deleting all three jobs.
			deleteRes, err := bundle.client.JobDeleteMany(ctx, river.NewJobDeleteManyParams().
				Kinds("target_kind").
				Where("priority = @priority1 OR priority = @priority2", river.NamedArgs{"priority1": 1, "priority2": 2}))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			_, err = bundle.client.JobGet(ctx, job2.ID)
			require.NoError(t, err)
			_, err = bundle.client.JobGet(ctx, job3.ID)
			require.NoError(t, err)
		})

		t.Run("TrailingCommentsAndMultiplePredicates", func(t *testing.T) {
			t.Parallel()

			bundle := setup(t)

			var (
				job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(1), Schema: bundle.schema})
				job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(1), Queue: new("other_queue"), Schema: bundle.schema})
				job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(2), Schema: bundle.schema})
				job4 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(1), Schema: bundle.schema, State: new(rivertype.JobStateRunning)})
			)

			deleteRes, err := bundle.client.JobDeleteMany(ctx, river.NewJobDeleteManyParams().
				Where("priority = @priority -- selected priority", river.NamedArgs{"priority": 1}).
				Where("queue = @queue -- selected queue", river.NamedArgs{"queue": job1.Queue}))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			for _, job := range []*rivertype.JobRow{job2, job3, job4} {
				_, err = bundle.client.JobGet(ctx, job.ID)
				require.NoError(t, err)
			}
		})

		t.Run("TransactionRollback", func(t *testing.T) {
			t.Parallel()

			bundle := setup(t)

			var (
				job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(1), Schema: bundle.schema})
				job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(2), Schema: bundle.schema})
			)

			execTx, err := bundle.exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = execTx.Rollback(ctx) })
			tx := bundle.driver.UnwrapTx(execTx)

			deleteRes, err := bundle.client.JobDeleteManyTx(ctx, tx, river.NewJobDeleteManyParams().
				Where("priority = @priority", river.NamedArgs{"priority": 1}))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			_, err = bundle.client.JobGetTx(ctx, tx, job1.ID)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			_, err = bundle.client.JobGetTx(ctx, tx, job2.ID)
			require.NoError(t, err)

			require.NoError(t, execTx.Rollback(ctx))

			_, err = bundle.client.JobGet(ctx, job1.ID)
			require.NoError(t, err)
		})

		t.Run("WithoutUnsafeAll", func(t *testing.T) {
			t.Parallel()

			bundle := setup(t)

			var (
				job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(1), Schema: bundle.schema})
				job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Priority: new(2), Schema: bundle.schema})
			)

			deleteRes, err := bundle.client.JobDeleteMany(ctx, river.NewJobDeleteManyParams().Where("priority = @priority", river.NamedArgs{"priority": 1}))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			_, err = bundle.client.JobGet(ctx, job2.ID)
			require.NoError(t, err)
		})
	})
}

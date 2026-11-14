package river

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

func TestNeverSchedule(t *testing.T) {
	t.Parallel()

	t.Run("NextReturnsMaximumTime", func(t *testing.T) {
		t.Parallel()

		schedule := NeverSchedule()
		now := time.Now()
		next := schedule.Next(now)
		require.Equal(t, time.Unix(1<<63-62135596801, 999999999), next)
		require.False(t, next.Before(now))
		// use an arbitrary duration to check that
		// the next schedule is far in the future
		require.Greater(t, next.Year()-now.Year(), 1000)
	})
}

func TestPeriodicJobBundle(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*PeriodicJobBundle, *testBundle) { //nolint:unparam
		t.Helper()

		periodicJobEnqueuer, err := maintenance.NewPeriodicJobEnqueuer(
			riversharedtest.BaseServiceArchetype(t),
			&maintenance.PeriodicJobEnqueuerConfig{},
			nil,
		)
		require.NoError(t, err)

		return newPeriodicJobBundle(newTestConfig(t, ""), periodicJobEnqueuer), &testBundle{}
	}

	t.Run("ConstructorFuncDefaultsScheduleWithoutMutatingOptions", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		occurrenceAt := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
		periodicJobBundle.mapper.archetype.Time.StubNow(occurrenceAt.Add(-50 * time.Millisecond))
		opts := &InsertOpts{UniqueOpts: UniqueOpts{ByPeriod: time.Hour}}
		periodicJob := NewPeriodicJob(PeriodicInterval(time.Hour), func() (JobArgs, *InsertOpts) {
			return noOpArgs{}, opts
		}, nil)
		internalPeriodicJob := periodicJobBundle.mapper.toInternal(periodicJob)

		// Constructors commonly reuse their options. Each occurrence must
		// get its own schedule and key without mutating that shared value or
		// a previously returned params struct's scheduled time.
		firstParams, err := internalPeriodicJob.ConstructorFunc(occurrenceAt)
		require.NoError(t, err)
		secondParams, err := internalPeriodicJob.ConstructorFunc(occurrenceAt.Add(time.Hour))
		require.NoError(t, err)

		firstKey := sha256.Sum256([]byte("&kind=" + (noOpArgs{}).Kind() + "&period=" + occurrenceAt.Format(time.RFC3339)))
		secondKey := sha256.Sum256([]byte("&kind=" + (noOpArgs{}).Kind() + "&period=" + occurrenceAt.Add(time.Hour).Format(time.RFC3339)))
		require.Equal(t, occurrenceAt, *firstParams.ScheduledAt)
		require.Equal(t, occurrenceAt.Add(time.Hour), *secondParams.ScheduledAt)
		require.Equal(t, firstKey[:], firstParams.UniqueKey)
		require.Equal(t, secondKey[:], secondParams.UniqueKey)
		require.Equal(t, rivertype.JobStateAvailable, firstParams.State)
		require.Equal(t, rivertype.JobStateAvailable, secondParams.State)
		require.Equal(t, InsertOpts{UniqueOpts: UniqueOpts{ByPeriod: time.Hour}}, *opts)
	})

	t.Run("ConstructorFuncGeneratesNewArgsOnEachCall", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		type TestJobArgs struct {
			testutil.JobArgsReflectKind[TestJobArgs]

			JobNum int `json:"job_num"`
		}

		var jobNum int

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) {
				jobNum++
				return TestJobArgs{JobNum: jobNum}, nil
			},
			nil,
		)

		internalPeriodicJob := periodicJobBundle.mapper.toInternal(periodicJob)

		firstScheduledAt := time.Now()
		insertParams1, err := internalPeriodicJob.ConstructorFunc(firstScheduledAt)
		require.NoError(t, err)
		require.Equal(t, 1, mustUnmarshalJSON[TestJobArgs](t, insertParams1.EncodedArgs).JobNum)
		require.Equal(t, firstScheduledAt, *insertParams1.ScheduledAt)

		secondScheduledAt := firstScheduledAt.Add(15 * time.Minute)
		insertParams2, err := internalPeriodicJob.ConstructorFunc(secondScheduledAt)
		require.NoError(t, err)
		require.Equal(t, 2, mustUnmarshalJSON[TestJobArgs](t, insertParams2.EncodedArgs).JobNum)
		require.Equal(t, secondScheduledAt, *insertParams2.ScheduledAt)
		require.Equal(t, firstScheduledAt, *insertParams1.ScheduledAt)
	})

	t.Run("ConstructorFuncRespectsConstructorScheduledAtOverJobArgs", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		occurrenceAt := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
		periodicJobBundle.mapper.archetype.Time.StubNow(occurrenceAt.Add(-50 * time.Millisecond))
		args := &customInsertOptsJobArgs{ScheduledAt: occurrenceAt.Add(2 * time.Hour)}
		opts := &InsertOpts{ScheduledAt: occurrenceAt.Add(time.Hour), UniqueOpts: UniqueOpts{ByPeriod: time.Hour}}
		periodicJob := NewPeriodicJob(PeriodicInterval(time.Hour), func() (JobArgs, *InsertOpts) {
			return args, opts
		}, nil)

		params, err := periodicJobBundle.mapper.toInternal(periodicJob).ConstructorFunc(occurrenceAt)
		require.NoError(t, err)
		wantKey := sha256.Sum256([]byte("&kind=" + args.Kind() + "&period=" + opts.ScheduledAt.Format(time.RFC3339)))
		require.Equal(t, opts.ScheduledAt, *params.ScheduledAt)
		require.Equal(t, wantKey[:], params.UniqueKey)
		require.Equal(t, rivertype.JobStateScheduled, params.State)
	})

	t.Run("ConstructorFuncRespectsJobArgsScheduledAt", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		occurrenceAt := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
		periodicJobBundle.mapper.archetype.Time.StubNow(occurrenceAt.Add(-50 * time.Millisecond))
		args := &customInsertOptsJobArgs{ScheduledAt: occurrenceAt.Add(time.Hour)}
		periodicJob := NewPeriodicJob(PeriodicInterval(time.Hour), func() (JobArgs, *InsertOpts) {
			return args, &InsertOpts{UniqueOpts: UniqueOpts{ByPeriod: time.Hour}}
		}, nil)

		params, err := periodicJobBundle.mapper.toInternal(periodicJob).ConstructorFunc(occurrenceAt)
		require.NoError(t, err)
		wantKey := sha256.Sum256([]byte("&kind=" + args.Kind() + "&period=" + args.ScheduledAt.Format(time.RFC3339)))
		require.Equal(t, args.ScheduledAt, *params.ScheduledAt)
		require.Equal(t, wantKey[:], params.UniqueKey)
		require.Equal(t, rivertype.JobStateScheduled, params.State)
	})

	t.Run("ReturningNilDoesntInsertNewJob", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) {
				// Returning nil from the constructor function should not insert a new job.
				return nil, nil
			},
			nil,
		)

		internalPeriodicJob := periodicJobBundle.mapper.toInternal(periodicJob)

		_, err := internalPeriodicJob.ConstructorFunc(time.Now())
		require.ErrorIs(t, err, maintenance.ErrNoJobToInsert)
	})

	t.Run("AddError", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) { return nil, nil },
			&PeriodicJobOpts{ID: "periodic_job_id"},
		)

		periodicJobBundle.Add(periodicJob)

		require.PanicsWithError(t, "periodic job with ID already registered: periodic_job_id", func() {
			periodicJobBundle.Add(periodicJob)
		})

		_, err := periodicJobBundle.AddSafely(periodicJob)
		require.EqualError(t, err, "periodic job with ID already registered: periodic_job_id")
	})

	t.Run("AddManyError", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) { return nil, nil },
			&PeriodicJobOpts{ID: "periodic_job_id"},
		)

		periodicJobBundle.Add(periodicJob)

		require.PanicsWithError(t, "periodic job with ID already registered: periodic_job_id", func() {
			periodicJobBundle.AddMany([]*PeriodicJob{periodicJob})
		})

		_, err := periodicJobBundle.AddManySafely([]*PeriodicJob{periodicJob})
		require.EqualError(t, err, "periodic job with ID already registered: periodic_job_id")
	})
}

// TestPeriodicJobByPeriodUnique exercises a cron occurrence inserted just
// before the hour it belongs to. The enqueuer uses one timer for all periodic
// jobs and includes occurrences less than 100 ms in the future whenever that
// timer wakes. A second schedule can therefore wake it at 11:59:59.950 and
// cause the noon occurrence to be inserted 50 ms early.
//
// Previously, the constructor computed the unique key before receiving the
// noon scheduled time, putting this occurrence in the 11:00 period instead.
// An existing 11:00 job then suppressed the noon insert, even if already
// completed, because completed jobs participate in uniqueness by default.
// The key must use noon, unless the constructor supplies its own schedule.
func TestPeriodicJobByPeriodUnique(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name        string
		pending     bool
		scheduledAt time.Time
	}{
		{name: "ConstructorScheduledAt", scheduledAt: time.Date(2026, 10, 4, 13, 0, 0, 0, time.UTC)},
		{name: "OccurrenceTime"},
		{name: "Pending", pending: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbPool := riversharedtest.DBPool(ctx, t)
			driver := riverpgxv5.New(dbPool)
			schema := riverdbtest.TestSchema(ctx, t, driver, nil)
			client := newTestClient(t, dbPool, newTestConfig(t, schema))
			svc := client.PeriodicJobs().periodicJobEnqueuer
			svc.StaggerStartupDisable(true)
			svc.TestSignals.Init(t)

			occurrenceAt := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
			svc.Time.StubNow(occurrenceAt.Add(-50 * time.Millisecond))
			uniqueOpts := UniqueOpts{ByPeriod: time.Hour}
			previous, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
				ScheduledAt: occurrenceAt.Add(-time.Hour),
				UniqueOpts:  uniqueOpts,
			})
			require.NoError(t, err)

			schedule, err := cron.ParseStandard("@hourly")
			require.NoError(t, err)
			opts := &InsertOpts{Pending: testCase.pending, ScheduledAt: testCase.scheduledAt, UniqueOpts: uniqueOpts}
			client.PeriodicJobs().Add(NewPeriodicJob(schedule, func() (JobArgs, *InsertOpts) {
				return noOpArgs{}, opts
			}, nil))
			// Wake the shared timer before the hourly schedule. Returning nil
			// keeps this second schedule from inserting an unrelated job.
			client.PeriodicJobs().Add(NewPeriodicJob(PeriodicInterval(10*time.Millisecond), func() (JobArgs, *InsertOpts) {
				return nil, nil
			}, nil))

			require.NoError(t, svc.Start(ctx))
			t.Cleanup(svc.Stop)
			svc.TestSignals.InsertedJobs.WaitOrTimeout()
			svc.Stop()

			jobs, err := driver.GetExecutor().JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{(noOpArgs{}).Kind()},
				Schema: schema,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 2, "the noon occurrence must not collide with the 11:00 job")
			var job *rivertype.JobRow
			for _, insertedJob := range jobs {
				if insertedJob.ID != previous.Job.ID {
					job = insertedJob
				}
			}
			require.NotNil(t, job)

			wantScheduledAt := occurrenceAt
			wantState := rivertype.JobStateAvailable
			if !testCase.scheduledAt.IsZero() {
				wantScheduledAt = testCase.scheduledAt
				wantState = rivertype.JobStateScheduled
			}
			if testCase.pending {
				wantState = rivertype.JobStatePending
			}
			wantKey := sha256.Sum256([]byte("&kind=" + job.Kind + "&period=" + wantScheduledAt.Format(time.RFC3339)))
			require.Equal(t, wantScheduledAt, job.ScheduledAt)
			require.Equal(t, wantKey[:], job.UniqueKey)
			require.Equal(t, wantState, job.State)
			require.Equal(t, testCase.scheduledAt, opts.ScheduledAt)
		})
	}
}

func mustUnmarshalJSON[T any](t *testing.T, data []byte) *T {
	t.Helper()

	var val T
	err := json.Unmarshal(data, &val)
	require.NoError(t, err)
	return &val
}

package riverdrivertest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseJobInsert[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		return exec, &testBundle{
			driver: driver,
		}
	}

	t.Run("JobInsertFastMany", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			var (
				idStart = rand.Int64()
				now     = time.Now().UTC()
			)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					ID:           ptrutil.Ptr(idStart + int64(i)),
					CreatedAt:    ptrutil.Ptr(now.Add(time.Duration(i) * 5 * time.Second)),
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  ptrutil.Ptr(now.Add(time.Duration(i) * time.Minute)),
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    []byte("unique-key-fast-many-" + strconv.Itoa(i)),
					UniqueStates: 0xff,
				}
			}

			resultRows, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: insertParams,
			})
			require.NoError(t, err)
			require.Len(t, resultRows, len(insertParams))

			for i, result := range resultRows {
				require.False(t, result.UniqueSkippedAsDuplicate)
				job := result.Job

				// SQLite needs to set a special metadata key to be able to
				// check for duplicates. Remove this for purposes of comparing
				// inserted metadata.
				job.Metadata, err = sjson.DeleteBytes(job.Metadata, rivercommon.MetadataKeyUniqueNonce)
				require.NoError(t, err)

				require.Equal(t, idStart+int64(i), job.ID)
				require.Equal(t, 0, job.Attempt)
				require.Nil(t, job.AttemptedAt)
				require.Empty(t, job.AttemptedBy)
				require.WithinDuration(t, now.Add(time.Duration(i)*5*time.Second), job.CreatedAt, time.Millisecond)
				require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
				require.Empty(t, job.Errors)
				require.Nil(t, job.FinalizedAt)
				require.Equal(t, "test_kind", job.Kind)
				require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
				require.JSONEq(t, `{"meta": "data"}`, string(job.Metadata))
				require.Equal(t, rivercommon.PriorityDefault, job.Priority)
				require.Equal(t, rivercommon.QueueDefault, job.Queue)
				require.WithinDuration(t, now.Add(time.Duration(i)*time.Minute), job.ScheduledAt, bundle.driver.TimePrecision())
				require.Equal(t, rivertype.JobStateAvailable, job.State)
				require.Equal(t, []string{"tag"}, job.Tags)
				require.Equal(t, []byte("unique-key-fast-many-"+strconv.Itoa(i)), job.UniqueKey)
				require.Equal(t, rivertype.JobStates(), job.UniqueStates)
			}
		})

		t.Run("MissingValuesDefaultAsExpected", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  nil, // explicit nil
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    nil,  // explicit nil
					UniqueStates: 0x00, // explicit 0
				}
			}

			results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: insertParams,
			})
			require.NoError(t, err)
			require.Len(t, results, len(insertParams))

			// Especially in SQLite where both the database and the drivers
			// suck, it's really easy to accidentally insert an empty value
			// instead of a real null so double check that we got real nulls.
			var (
				attemptedAtIsNull  bool
				attemptedByIsNull  bool
				uniqueKeyIsNull    bool
				uniqueStatesIsNull bool
			)
			require.NoError(t, exec.QueryRow(ctx, "SELECT attempted_at IS NULL, attempted_by IS NULL, unique_key IS NULL, unique_states IS NULL FROM river_job").Scan(
				&attemptedAtIsNull,
				&attemptedByIsNull,
				&uniqueKeyIsNull,
				&uniqueStatesIsNull,
			))
			require.True(t, attemptedAtIsNull)
			require.True(t, attemptedByIsNull)
			require.True(t, uniqueKeyIsNull)
			require.True(t, uniqueStatesIsNull)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.NotZero(t, job.ID)
				require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)

				// UniqueKey and UniqueStates are not set in the insert params, so they should
				// be nil and an empty slice respectively.
				require.Nil(t, job.UniqueKey)
				var emptyJobStates []rivertype.JobState
				require.Equal(t, emptyJobStates, job.UniqueStates)
			}
		})

		t.Run("UniqueConflict", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			uniqueKey := "unique-key-fast-conflict"

			results1, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, results1, 1)
			require.False(t, results1[0].UniqueSkippedAsDuplicate)

			results2, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, results2, 1)
			require.True(t, results2[0].UniqueSkippedAsDuplicate)

			require.Equal(t, results1[0].Job.ID, results2[0].Job.ID)
		})

		t.Run("BinaryNonUTF8UniqueKey", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			uniqueKey := []byte{0x00, 0x01, 0x02}
			results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						ScheduledAt:  nil, // explicit nil
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    uniqueKey,
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, uniqueKey, results[0].Job.UniqueKey)

			jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Equal(t, uniqueKey, jobs[0].UniqueKey)
		})
	})

	t.Run("JobInsertFastManyNoReturning", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			exec, bundle := setup(ctx, t)

			// This test needs to use a time from before the transaction begins, otherwise
			// the newly-scheduled jobs won't yet show as available because their
			// scheduled_at (which gets a default value from time.Now() in code) will be
			// after the start of the transaction.
			now := time.Now().UTC().Add(-1 * time.Minute)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					CreatedAt:    ptrutil.Ptr(now.Add(time.Duration(i) * 5 * time.Second)),
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  &now,
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    []byte("unique-key-no-returning-" + strconv.Itoa(i)),
					UniqueStates: 0xff,
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for i, job := range jobsAfter {
				require.Equal(t, 0, job.Attempt)
				require.Nil(t, job.AttemptedAt)
				require.WithinDuration(t, now.Add(time.Duration(i)*5*time.Second), job.CreatedAt, time.Millisecond)
				require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
				require.Empty(t, job.Errors)
				require.Nil(t, job.FinalizedAt)
				require.Equal(t, "test_kind", job.Kind)
				require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
				require.JSONEq(t, `{"meta": "data"}`, string(job.Metadata))
				require.Equal(t, rivercommon.PriorityDefault, job.Priority)
				require.Equal(t, rivercommon.QueueDefault, job.Queue)
				require.WithinDuration(t, now, job.ScheduledAt, bundle.driver.TimePrecision())
				require.Equal(t, rivertype.JobStateAvailable, job.State)
				require.Equal(t, []string{"tag"}, job.Tags)
				require.Equal(t, []byte("unique-key-no-returning-"+strconv.Itoa(i)), job.UniqueKey)
			}
		})

		t.Run("MissingValuesDefaultAsExpected", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  nil, // explicit nil
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    nil,  // explicit nil
					UniqueStates: 0x00, // explicit 0
				}
			}

			rowsAffected, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: insertParams,
			})
			require.NoError(t, err)
			require.Equal(t, len(insertParams), rowsAffected)

			// Especially in SQLite where both the database and the drivers
			// suck, it's really easy to accidentally insert an empty value
			// instead of a real null so double check that we got real nulls.
			var (
				attemptedAtIsNull  bool
				attemptedByIsNull  bool
				uniqueKeyIsNull    bool
				uniqueStatesIsNull bool
			)
			require.NoError(t, exec.QueryRow(ctx, "SELECT attempted_at IS NULL, attempted_by IS NULL, unique_key IS NULL, unique_states IS NULL FROM river_job").Scan(
				&attemptedAtIsNull,
				&attemptedByIsNull,
				&uniqueKeyIsNull,
				&uniqueStatesIsNull,
			))
			require.True(t, attemptedAtIsNull)
			require.True(t, attemptedByIsNull)
			require.True(t, uniqueKeyIsNull)
			require.True(t, uniqueStatesIsNull)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)

				// UniqueKey and UniqueStates are not set in the insert params, so they should
				// be nil and an empty slice respectively.
				require.Nil(t, job.UniqueKey)
				var emptyJobStates []rivertype.JobState
				require.Equal(t, emptyJobStates, job.UniqueStates)
			}
		})

		t.Run("UniqueConflict", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			uniqueKey := "unique-key-fast-conflict"

			rowsAffected1, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, rowsAffected1)

			rowsAffected2, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{
					{
						EncodedArgs:  []byte(`{"encoded": "args"}`),
						Kind:         "test_kind",
						MaxAttempts:  rivercommon.MaxAttemptsDefault,
						Metadata:     []byte(`{"meta": "data"}`),
						Priority:     rivercommon.PriorityDefault,
						Queue:        rivercommon.QueueDefault,
						State:        rivertype.JobStateAvailable,
						Tags:         []string{"tag"},
						UniqueKey:    []byte(uniqueKey),
						UniqueStates: 0xff,
					},
				},
			})
			if err != nil {
				// PgxV5 uses copy/from which means that it can't support `ON
				// CONFLICT` and therefore returns an error here. Callers that
				// want to bulk insert unique jobs should use the returning
				// variant instead.
				if reflect.TypeOf(bundle.driver).Elem().PkgPath() == "github.com/riverqueue/river/riverdriver/riverpgxv5" {
					var pgErr *pgconn.PgError
					require.ErrorAs(t, err, &pgErr)
					require.Equal(t, pgerrcode.UniqueViolation, pgErr.Code)
					require.Equal(t, "river_job_unique_idx", pgErr.ConstraintName)
					return
				}
			}
			require.NoError(t, err)
			require.Zero(t, rowsAffected2)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind: []string{"test_kind"},
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, 1)
		})

		t.Run("MissingCreatedAtDefaultsToNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					CreatedAt:   nil, // explicit nil
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: ptrutil.Ptr(time.Now().UTC()),
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			}
		})

		t.Run("MissingScheduledAtDefaultsToNow", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Metadata:    []byte(`{"meta": "data"}`),
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
					ScheduledAt: nil, // explicit nil
					State:       rivertype.JobStateAvailable,
					Tags:        []string{"tag"},
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: "",
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
			for _, job := range jobsAfter {
				require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 2*time.Second)
			}
		})

		t.Run("AlternateSchema", func(t *testing.T) {
			t.Parallel()

			var (
				driver, schema = driverWithSchema(ctx, t, nil)
				exec           = driver.GetExecutor()
			)

			// This test needs to use a time from before the transaction begins, otherwise
			// the newly-scheduled jobs won't yet show as available because their
			// scheduled_at (which gets a default value from time.Now() in code) will be
			// after the start of the transaction.
			now := time.Now().UTC().Add(-1 * time.Minute)

			insertParams := make([]*riverdriver.JobInsertFastParams, 10)
			for i := range insertParams {
				insertParams[i] = &riverdriver.JobInsertFastParams{
					CreatedAt:    ptrutil.Ptr(now.Add(time.Duration(i) * 5 * time.Second)),
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Metadata:     []byte(`{"meta": "data"}`),
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					ScheduledAt:  &now,
					State:        rivertype.JobStateAvailable,
					Tags:         []string{"tag"},
					UniqueKey:    []byte("unique-key-no-returning-" + strconv.Itoa(i)),
					UniqueStates: 0xff,
				}
			}

			count, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs:   insertParams,
				Schema: schema,
			})
			require.NoError(t, err)
			require.Len(t, insertParams, count)

			jobsAfter, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{"test_kind"},
				Schema: schema,
			})
			require.NoError(t, err)
			require.Len(t, jobsAfter, len(insertParams))
		})
	})

	t.Run("JobInsertFull", func(t *testing.T) {
		t.Parallel()

		t.Run("MinimalArgsWithDefaults", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Kind:        "test_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateAvailable,
			})
			require.NoError(t, err)
			require.Equal(t, 0, job.Attempt)
			require.Nil(t, job.AttemptedAt)
			require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
			require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
			require.Empty(t, job.Errors)
			require.Nil(t, job.FinalizedAt)
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)
			require.Equal(t, rivercommon.QueueDefault, job.Queue)
			require.Equal(t, rivertype.JobStateAvailable, job.State)

			// Especially in SQLite where both the database and the drivers
			// suck, it's really easy to accidentally insert an empty value
			// instead of a real null so double check that we got real nulls.
			var (
				attemptedAtIsNull  bool
				attemptedByIsNull  bool
				errorsIsNull       bool
				finalizedAtIsNull  bool
				uniqueKeyIsNull    bool
				uniqueStatesIsNull bool
			)
			require.NoError(t, exec.QueryRow(ctx, "SELECT attempted_at IS NULL, attempted_by IS NULL, errors IS NULL, finalized_at IS NULL, unique_key IS NULL, unique_states IS NULL FROM river_job").Scan(&attemptedAtIsNull, &attemptedByIsNull, &errorsIsNull, &finalizedAtIsNull, &uniqueKeyIsNull, &uniqueStatesIsNull))
			require.True(t, attemptedAtIsNull)
			require.True(t, attemptedByIsNull)
			require.True(t, errorsIsNull)
			require.True(t, finalizedAtIsNull)
			require.True(t, uniqueKeyIsNull)
			require.True(t, uniqueStatesIsNull)
		})

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				Attempt:     3,
				AttemptedAt: &now,
				AttemptedBy: []string{"worker1", "worker2"},
				CreatedAt:   &now,
				EncodedArgs: []byte(`{"encoded": "args"}`),
				Errors:      [][]byte{[]byte(`{"error": "message"}`)},
				FinalizedAt: &now,
				Kind:        "test_kind",
				MaxAttempts: 6,
				Metadata:    []byte(`{"meta": "data"}`),
				Priority:    2,
				Queue:       "queue_name",
				ScheduledAt: &now,
				State:       rivertype.JobStateCompleted,
				Tags:        []string{"tag"},
				UniqueKey:   []byte("unique-key"),
			})
			require.NoError(t, err)
			require.Equal(t, 3, job.Attempt)
			require.WithinDuration(t, now, *job.AttemptedAt, bundle.driver.TimePrecision())
			require.Equal(t, []string{"worker1", "worker2"}, job.AttemptedBy)
			require.WithinDuration(t, now, job.CreatedAt, bundle.driver.TimePrecision())
			require.JSONEq(t, `{"encoded": "args"}`, string(job.EncodedArgs))
			require.Equal(t, "message", job.Errors[0].Error)
			require.WithinDuration(t, now, *job.FinalizedAt, bundle.driver.TimePrecision())
			require.Equal(t, "test_kind", job.Kind)
			require.Equal(t, 6, job.MaxAttempts)
			require.JSONEq(t, `{"meta": "data"}`, string(job.Metadata))
			require.Equal(t, 2, job.Priority)
			require.Equal(t, "queue_name", job.Queue)
			require.WithinDuration(t, now, job.ScheduledAt, bundle.driver.TimePrecision())
			require.Equal(t, rivertype.JobStateCompleted, job.State)
			require.Equal(t, []string{"tag"}, job.Tags)
			require.Equal(t, []byte("unique-key"), job.UniqueKey)
		})

		t.Run("JobFinalizedAtConstraint", func(t *testing.T) {
			t.Parallel()

			capitalizeJobState := func(state rivertype.JobState) string {
				return cases.Title(language.English, cases.NoLower).String(string(state))
			}

			for _, state := range []rivertype.JobState{
				rivertype.JobStateCancelled,
				rivertype.JobStateCompleted,
				rivertype.JobStateDiscarded,
			} {
				t.Run(fmt.Sprintf("CannotSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but without a finalized_at,
					// expect an error:
					params := testfactory.Job_Build(t, &testfactory.JobOpts{
						State: &state,
					})
					params.FinalizedAt = nil
					_, err := exec.JobInsertFull(ctx, params)
					require.Error(t, err)
					// two separate error messages here for Postgres and SQLite
					require.Regexp(t, `(CHECK constraint failed: finalized_or_finalized_at_null|violates check constraint "finalized_or_finalized_at_null")`, err.Error())
				})

				t.Run(fmt.Sprintf("CanSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but with a finalized_at, expect
					// no error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						FinalizedAt: ptrutil.Ptr(time.Now()),
						State:       &state,
					}))
					require.NoError(t, err)
				})
			}

			for _, state := range []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			} {
				t.Run(fmt.Sprintf("CanSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but without a finalized_at,
					// expect no error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						State: &state,
					}))
					require.NoError(t, err)
				})

				t.Run(fmt.Sprintf("CannotSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
					t.Parallel()

					exec, _ := setup(ctx, t)

					// Create a job with the target state but with a finalized_at, expect
					// an error:
					_, err := exec.JobInsertFull(ctx, testfactory.Job_Build(t, &testfactory.JobOpts{
						FinalizedAt: ptrutil.Ptr(time.Now()),
						State:       &state,
					}))
					require.Error(t, err)
					// two separate error messages here for Postgres and SQLite
					require.Regexp(t, `(CHECK constraint failed: finalized_or_finalized_at_null|violates check constraint "finalized_or_finalized_at_null")`, err.Error())
				})
			}
		})
	})

	t.Run("JobInsertFullMany", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		jobParams1 := testfactory.Job_Build(t, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateCompleted),
		})
		jobParams2 := testfactory.Job_Build(t, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		results, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
			Jobs: []*riverdriver.JobInsertFullParams{jobParams1, jobParams2},
		})
		require.NoError(t, err)

		require.Len(t, results, 2)
		now := time.Now().UTC()

		assertJobEqualsInput := func(t *testing.T, job *rivertype.JobRow, input *riverdriver.JobInsertFullParams) {
			t.Helper()

			require.Equal(t, input.Attempt, job.Attempt)
			if input.AttemptedAt == nil {
				require.Nil(t, job.AttemptedAt)
			} else {
				t.Logf("job: %+v", job)
				t.Logf("input: %+v", input)
				require.WithinDuration(t, *input.AttemptedAt, *job.AttemptedAt, bundle.driver.TimePrecision())
			}
			require.Equal(t, input.AttemptedBy, job.AttemptedBy)
			require.WithinDuration(t, now, job.CreatedAt, 5*time.Second)
			require.Equal(t, input.EncodedArgs, job.EncodedArgs)
			require.Empty(t, job.Errors)
			if input.FinalizedAt == nil || input.FinalizedAt.IsZero() {
				require.Nil(t, job.FinalizedAt)
			} else {
				require.WithinDuration(t, input.FinalizedAt.UTC(), job.FinalizedAt.UTC(), bundle.driver.TimePrecision())
			}
			require.Equal(t, input.Kind, job.Kind)
			require.Equal(t, input.MaxAttempts, job.MaxAttempts)
			require.Equal(t, input.Metadata, job.Metadata)
			require.Equal(t, input.Priority, job.Priority)
			require.Equal(t, input.Queue, job.Queue)
			if input.ScheduledAt == nil {
				require.WithinDuration(t, now, job.ScheduledAt, 5*time.Second)
			} else {
				require.WithinDuration(t, input.ScheduledAt.UTC(), job.ScheduledAt, bundle.driver.TimePrecision())
			}
			require.Equal(t, input.State, job.State)
			require.Equal(t, input.Tags, job.Tags)
			require.Equal(t, input.UniqueKey, job.UniqueKey)
			require.Empty(t, job.UniqueStates)
		}

		assertJobEqualsInput(t, results[0], jobParams1)
		assertJobEqualsInput(t, results[1], jobParams2)
	})
}

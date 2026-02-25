package riverdrivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func Benchmark[TTx any](ctx context.Context, b *testing.B,
	driverWithPool func(ctx context.Context, b *testing.B) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, b *testing.B) riverdriver.Executor,
) {
	b.Helper()

	setup := func(ctx context.Context, b *testing.B) (riverdriver.Executor, string) {
		b.Helper()
		_, schema := driverWithPool(ctx, b)
		return executorWithTx(ctx, b), schema
	}

	b.Run("JobSetStateIfRunningMany", func(b *testing.B) {
		exec, schema := setup(ctx, b)

		const (
			totalJobs  = 100000
			batchSize  = 2000
			numBatches = totalJobs / batchSize
		)

		// Build a single batch of job parameters to be reused
		insertParams := make([]*riverdriver.JobInsertFullParams, batchSize)
		for i := range batchSize {
			// Create a mix of jobs with different states and metadata
			var (
				metadata []byte
				state    rivertype.JobState
			)

			switch i % 10 {
			case 0, 1, 2, 3, 4, 5:
				// Most jobs are running
				state = rivertype.JobStateRunning
				if i%20 == 0 {
					// Every 20th job has cancel_attempted_at
					metadata = []byte(`{"cancel_attempted_at": "2024-01-01T00:00:00Z"}`)
				}
			case 6, 7:
				// Some jobs are completed (to test no-op path)
				state = rivertype.JobStateCompleted
			case 8, 9:
				// Some jobs are available (to test no-op path)
				state = rivertype.JobStateAvailable
				if i%15 == 0 {
					// Every 15th job has metadata to merge
					metadata = []byte(`{"key": "value"}`)
				}
			}

			insertParams[i] = testfactory.Job_Build(b, &testfactory.JobOpts{
				Metadata: metadata,
				State:    &state,
			})
		}

		// Insert the batch multiple times to reach our total
		var batchJobs []*rivertype.JobRow
		for i := range numBatches {
			results, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
				Jobs:   insertParams,
				Schema: schema,
			})
			if err != nil {
				b.Fatalf("failed to insert jobs: %v", err)
			}
			if i == 0 {
				batchJobs = results
			}
		}

		// Take a batch of 2000 jobs for this iteration
		// Prepare update parameters
		params := &riverdriver.JobSetStateIfRunningManyParams{
			ID:              make([]int64, len(batchJobs)),
			Attempt:         make([]*int, len(batchJobs)),
			ErrData:         make([][]byte, len(batchJobs)),
			FinalizedAt:     make([]*time.Time, len(batchJobs)),
			MetadataDoMerge: make([]bool, len(batchJobs)),
			MetadataUpdates: make([][]byte, len(batchJobs)),
			ScheduledAt:     make([]*time.Time, len(batchJobs)),
			State:           make([]rivertype.JobState, len(batchJobs)),
			Schema:          schema,
		}

		now := time.Now().UTC()
		for j, result := range batchJobs {
			params.ID[j] = result.ID
			switch j % 100 {
			case 0:
				// Retry the job
				params.ErrData[j] = []byte(`{"error": "test error"}`)
				params.ScheduledAt[j] = &now
				params.State[j] = rivertype.JobStateRetryable
			case 1:
				// Completions with metadata updates
				params.MetadataDoMerge[j] = true
				params.MetadataUpdates[j] = []byte(`{"updated": true}`)
				params.FinalizedAt[j] = &now
				params.State[j] = rivertype.JobStateCompleted
			case 2:
				// Snooze the job
				params.Attempt[j] = ptrutil.Ptr(1)
				params.ScheduledAt[j] = &now
				params.State[j] = rivertype.JobStateScheduled
			default:
				// Mostly regular completions
				params.FinalizedAt[j] = &now
				params.State[j] = rivertype.JobStateCompleted
			}
		}

		// Give the db some time to chill
		time.Sleep(time.Second * 5)

		b.ResetTimer()
		for range b.N {
			// Execute the update
			if _, err := exec.JobSetStateIfRunningMany(ctx, params); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobSetStateIfRunningMany_LargeMetadata", func(b *testing.B) {
		if testing.Short() {
			b.Skip("skipping benchmark in short mode")
		}

		exec, schema := setup(ctx, b)

		testCases := []struct {
			name              string
			metadataSizeBytes int
		}{
			{name: "Metadata2MB", metadataSizeBytes: 2 * 1024 * 1024},
			{name: "Metadata8MB", metadataSizeBytes: 8 * 1024 * 1024},
		}

		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				largeMetadata := makeBenchmarkMetadataWithRiverLogSize(tc.metadataSizeBytes)
				stateRunning := rivertype.JobStateRunning

				insertedJobs, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
					Jobs: []*riverdriver.JobInsertFullParams{
						testfactory.Job_Build(b, &testfactory.JobOpts{
							Metadata: largeMetadata,
							State:    &stateRunning,
						}),
					},
					Schema: schema,
				})
				if err != nil {
					b.Fatalf("failed to insert benchmark job: %v", err)
				}
				if len(insertedJobs) != 1 {
					b.Fatalf("expected exactly one inserted job, got %d", len(insertedJobs))
				}

				now := time.Now().UTC()
				params := &riverdriver.JobSetStateIfRunningManyParams{
					ID:              []int64{insertedJobs[0].ID},
					Attempt:         []*int{nil},
					ErrData:         [][]byte{nil},
					FinalizedAt:     []*time.Time{nil},
					MetadataDoMerge: []bool{true},
					MetadataUpdates: [][]byte{largeMetadata},
					ScheduledAt:     []*time.Time{&now},
					Schema:          schema,
					State:           []rivertype.JobState{rivertype.JobStateScheduled},
				}

				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					if _, err := exec.JobSetStateIfRunningMany(ctx, params); err != nil {
						b.Fatalf("failed to update benchmark job: %v", err)
					}
				}
			})
		}
	})

	b.Run("JobGetAvailable_LargeMetadata", func(b *testing.B) {
		if testing.Short() {
			b.Skip("skipping benchmark in short mode")
		}

		exec, schema := setup(ctx, b)

		testCases := []struct {
			name              string
			metadataSizeBytes int
		}{
			{name: "Metadata2MB", metadataSizeBytes: 2 * 1024 * 1024},
			{name: "Metadata8MB", metadataSizeBytes: 8 * 1024 * 1024},
		}

		for _, tc := range testCases {
			b.Run(tc.name, func(b *testing.B) {
				largeMetadata := makeBenchmarkMetadataWithRiverLogSize(tc.metadataSizeBytes)
				now := time.Now().UTC()

				insertedJobs, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
					Jobs: []*riverdriver.JobInsertFullParams{
						testfactory.Job_Build(b, &testfactory.JobOpts{
							Metadata:    largeMetadata,
							ScheduledAt: &now,
						}),
					},
					Schema: schema,
				})
				if err != nil {
					b.Fatalf("failed to insert benchmark job: %v", err)
				}
				if len(insertedJobs) != 1 {
					b.Fatalf("expected exactly one inserted job, got %d", len(insertedJobs))
				}

				schemaPrefix := ""
				if schema != "" {
					schemaPrefix = schema + "."
				}
				resetSQL := fmt.Sprintf(
					"UPDATE %sriver_job SET state = 'available', attempt = 0 WHERE id = %d",
					schemaPrefix,
					insertedJobs[0].ID,
				)

				getAvailableParams := &riverdriver.JobGetAvailableParams{
					ClientID:       "bench-client-id",
					MaxAttemptedBy: 100,
					MaxToLock:      1,
					Now:            &now,
					Queue:          insertedJobs[0].Queue,
					Schema:         schema,
				}

				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					jobs, err := exec.JobGetAvailable(ctx, getAvailableParams)
					if err != nil {
						b.Fatalf("failed to fetch benchmark job: %v", err)
					}
					if len(jobs) != 1 {
						b.Fatalf("expected exactly one fetched job, got %d", len(jobs))
					}

					if len(jobs[0].Metadata) == 0 {
						b.Fatal("expected non-empty job metadata")
					}

					// Reset job state for the next benchmark iteration without
					// using a RETURNING query that would read metadata again.
					if err := exec.Exec(ctx, resetSQL); err != nil {
						b.Fatalf("failed to reset benchmark job: %v", err)
					}
				}
			})
		}
	})
}

type benchmarkLogAttempt struct {
	Attempt int    `json:"attempt"`
	Log     string `json:"log"`
}

func makeBenchmarkMetadataWithRiverLogSize(targetBytes int) []byte {
	if targetBytes <= 0 {
		return []byte(`{}`)
	}

	const perEntryLogSize = 1024
	payload := strings.Repeat("x", perEntryLogSize)
	numEntries := max(1, targetBytes/perEntryLogSize)

	logs := make([]benchmarkLogAttempt, numEntries)
	for i := range numEntries {
		logs[i] = benchmarkLogAttempt{
			Attempt: i + 1,
			Log:     payload,
		}
	}

	metadataBytes, err := json.Marshal(map[string]any{
		"river:log": logs,
	})
	if err != nil {
		panic(err)
	}

	return metadataBytes
}

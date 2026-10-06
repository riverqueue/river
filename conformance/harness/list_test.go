package harness

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// cursorKind is a job kind that Go's encoding/json escapes (`<`, `>`, and
// `&` become `<`, `>`, and `&`) and whose cursor text always
// contains `-`, wherever the kind falls in the Base64 groups: one of three
// consecutive `~` bytes ends a group, and its low six bits encode as `-`.
const cursorKind = "conformance_cursor<>&~~~"

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestList(t *testing.T) {
	t.Parallel()

	// Job list cursors are interchangeable for each sort field: both
	// implementations emit the same cursor text for the same page, and each
	// resumes from the other's cursor to the same next page, in both
	// directions. Time ordering over mixed states uses the first listed
	// state's field for every job and its cursor, with nulls last ascending
	// and first descending.
	t.Run("CursorInterchange", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, writer, reader *Adapter) {
			idsByKind := map[string][]int64{}
			for i := range 3 {
				// Fractional seconds that Go encodes with trailing zeros
				// trimmed, like `.12`.
				scheduledAt := time.Date(2099, 1, 1, 0, 0, i+1, (i+1)*100_000_000+20_000_000, time.UTC)
				scheduled := writer.InsertJob(t, withOpts(echo(fmt.Sprintf("cursor %d", i), protocol.BehaviorComplete),
					protocol.InsertOpts{ScheduledAt: &scheduledAt}))
				idsByKind[protocol.KindEcho] = append(idsByKind[protocol.KindEcho], scheduled.ID)
				idsByKind[cursorKind] = append(idsByKind[cursorKind], env.DB.InsertRaw(t, RawJob{Kind: cursorKind}))
			}

			type listCase struct {
				kind    string
				orderBy string
				// order lists the kind's jobs by insertion index in ascending
				// list order, or nil for insertion order.
				order  []int
				states []string
			}
			verifyCases := func(cases []listCase) {
				for _, current := range cases {
					for _, direction := range []string{"asc", "desc"} {
						description := fmt.Sprintf("kind %s ordered by %s %s in %v", current.kind, current.orderBy, direction, current.states)
						expected := slices.Clone(idsByKind[current.kind])
						if current.order != nil {
							expected = expected[:0]
							for _, i := range current.order {
								expected = append(expected, idsByKind[current.kind][i])
							}
						}
						if direction == "desc" {
							slices.Reverse(expected)
						}
						params := protocol.ListParams{Direction: direction, Kinds: []string{current.kind}, Limit: 2, OrderBy: current.orderBy, States: current.states}

						writerPage, readerPage := writer.List(t, params), reader.List(t, params)
						require.Equal(t, expected[:2], listedIDs(writerPage.Jobs), description)
						require.Equal(t, writerPage, readerPage, description)
						require.NotNil(t, writerPage.Cursor, description)
						if current.kind == cursorKind {
							require.Contains(t, *writerPage.Cursor, "-", description)
						}

						params.After = *writerPage.Cursor
						require.Equal(t, expected[2:], listedIDs(reader.List(t, params).Jobs), description)
					}
				}
			}

			verifyCases([]listCase{
				{kind: protocol.KindEcho, orderBy: "id"},
				{kind: protocol.KindEcho, orderBy: "scheduled_at", states: []string{"scheduled"}},
				{kind: protocol.KindEcho, orderBy: "time", states: []string{"scheduled"}},
				{kind: cursorKind, orderBy: "id"},
			})

			// Cancelling in ID order sets increasing finalized_at times.
			for _, kind := range []string{protocol.KindEcho, cursorKind} {
				for _, id := range idsByKind[kind] {
					writer.Cancel(t, protocol.JobParams{ID: id})
				}
			}
			verifyCases([]listCase{
				{kind: protocol.KindEcho, orderBy: "finalized_at", states: []string{"cancelled"}},
				{kind: protocol.KindEcho, orderBy: "time", states: []string{"cancelled"}},
				{kind: cursorKind, orderBy: "finalized_at", states: []string{"cancelled"}},
				{kind: cursorKind, orderBy: "time", states: []string{"cancelled"}},
			})

			// Retrying the middle job makes it available again, scheduled now
			// and without a finalized time. Listed with the cancelled jobs,
			// every job is ordered by the first state's field, so a page can
			// end on a job of the other state, and the retried job's null
			// finalized_at sorts last ascending.
			echoIDs := idsByKind[protocol.KindEcho]
			writer.Retry(t, protocol.JobParams{ID: echoIDs[1]})
			verifyCases([]listCase{
				{kind: protocol.KindEcho, orderBy: "time", order: []int{0, 2, 1}, states: []string{"cancelled", "available"}},
				{kind: protocol.KindEcho, orderBy: "time", order: []int{1, 0, 2}, states: []string{"available", "cancelled"}},
			})

			// With the last job retried too, pages end on a null finalized_at.
			writer.Retry(t, protocol.JobParams{ID: echoIDs[2]})
			verifyCases([]listCase{
				{kind: protocol.KindEcho, orderBy: "time", order: []int{0, 1, 2}, states: []string{"cancelled", "available"}},
			})
		})
	})

	// Filtered pages agree between implementations and resume from each
	// other's cursors. SQLite can't filter by metadata.
	t.Run("Filters", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, writer, reader *Adapter) {
			paginationIDs := make([]int64, 0, 3)
			for i := range 3 {
				scheduledAt := time.Date(2099, 1, 1, 0, 0, i+1, 0, time.UTC)
				job := writer.InsertJob(t, withOpts(echo(fmt.Sprintf("pagination %d", i), protocol.BehaviorComplete), protocol.InsertOpts{
					Metadata:    metadata(t, map[string]any{"pagination_writer": writer.Label}),
					Priority:    i + 1,
					ScheduledAt: &scheduledAt,
					Tags:        []string{"pagination_jobs"},
				}))
				paginationIDs = append(paginationIDs, job.ID)
			}
			// A job outside every filter must never appear.
			writer.InsertJob(t, echo("pagination excluded", protocol.BehaviorComplete))

			params := protocol.ListParams{
				Direction:  "desc",
				Limit:      2,
				OrderBy:    "scheduled_at",
				Priorities: []int{1, 2, 3},
				Queues:     []string{"default"},
				States:     []string{"scheduled"},
				TagsAll:    []string{"pagination_jobs"},
			}
			if env.Driver == DriverPostgres {
				params.Metadata = metadata(t, map[string]any{"pagination_writer": writer.Label})
			}
			writerPage, readerPage := writer.List(t, params), reader.List(t, params)
			require.Equal(t, writerPage, readerPage)
			require.Equal(t, []int64{paginationIDs[2], paginationIDs[1]}, listedIDs(writerPage.Jobs))
			require.NotNil(t, writerPage.Cursor)

			params.After = *writerPage.Cursor
			readerNext := reader.List(t, params)
			params.After = *readerPage.Cursor
			require.Equal(t, writer.List(t, params), readerNext)
			require.Equal(t, []int64{paginationIDs[0]}, listedIDs(readerNext.Jobs))
		})
	})
}

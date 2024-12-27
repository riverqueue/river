package dbunique

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

type JobArgsStaticKind struct {
	kind string
}

func (a JobArgsStaticKind) Kind() string {
	return a.kind
}

func TestUniqueKey(t *testing.T) {
	t.Parallel()

	// Fixed timestamp for consistency across tests:
	now := time.Now().UTC()
	stubSvc := &riversharedtest.TimeStub{}
	stubSvc.StubNowUTC(now)

	tests := []struct {
		name         string
		argsFunc     func() rivertype.JobArgs
		uniqueOpts   UniqueOpts
		expectedJSON string
	}{
		{
			name: "ByArgsWithMultipleUniqueStructTagsAndDefaultStates",
			argsFunc: func() rivertype.JobArgs {
				type EmailJobArgs struct {
					JobArgsStaticKind
					Recipient   string `json:"recipient"    river:"unique"`
					Subject     string `json:"subject"      river:"unique"`
					Body        string `json:"body"`
					TemplateID  int    `json:"template_id"`
					ScheduledAt string `json:"scheduled_at"`
				}
				return EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Recipient:         "user@example.com",
					Subject:           "Test Email",
					Body:              "This is a test email.",
					TemplateID:        101,
					ScheduledAt:       "2024-09-15T10:00:00Z",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"recipient":"user@example.com","subject":"Test Email"}`,
		},
		{
			name: "ByArgsWithUniqueFieldsSomeEmpty",
			argsFunc: func() rivertype.JobArgs {
				type SMSJobArgs struct {
					JobArgsStaticKind
					PhoneNumber string `json:"phone_number"      river:"unique"`
					Message     string `json:"message,omitempty" river:"unique"`
					TemplateID  int    `json:"template_id"`
				}
				return SMSJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_2"},
					PhoneNumber:       "555-5678",
					Message:           "", // Empty unique field, omitted from key
					TemplateID:        202,
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_2&args={"phone_number":"555-5678"}`,
		},
		{
			name: "ByArgsUniqueWithNoJSONTagsUsesFieldName",
			argsFunc: func() rivertype.JobArgs {
				type EmailJobArgs struct {
					JobArgsStaticKind
					Recipient  string `river:"unique"`
					Subject    string `river:"unique"`
					TemplateID int
				}
				return EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Recipient:         "john@example.com",
					Subject:           "Another Test Email",
					TemplateID:        102,
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"Recipient":"john@example.com","Subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithPointerToStruct",
			argsFunc: func() rivertype.JobArgs {
				type EmailJobArgs struct {
					JobArgsStaticKind
					Recipient string `json:"recipient" river:"unique"`
					Subject   string `json:"subject"   river:"unique"`
					Body      string `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Recipient:         "john@example.com",
					Subject:           "Another Test Email",
					Body:              "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"recipient":"john@example.com","subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithNoUniqueFields",
			argsFunc: func() rivertype.JobArgs {
				type GenericJobArgs struct {
					JobArgsStaticKind
					Description string `json:"description"`
					Count       int    `json:"count"`
					foo         string // won't be marshaled in JSON
				}
				return GenericJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_3"},
					Description:       "A generic job without unique fields.",
					Count:             10,
					foo:               "bar",
				}
			},
			uniqueOpts: UniqueOpts{ByArgs: true},
			// args JSON should be sorted alphabetically:
			expectedJSON: `&kind=worker_3&args={"count":10,"description":"A generic job without unique fields."}`,
		},
		{
			name: "ByArgsWithEmptyEncodedArgs",
			argsFunc: func() rivertype.JobArgs {
				type EmailJobArgs struct {
					JobArgsStaticKind
				}

				return EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
				}
			},
			uniqueOpts: UniqueOpts{ByArgs: true},
			// args JSON should be sorted alphabetically:
			expectedJSON: `&kind=worker_1&args={}`,
		},
		{
			name: "CustomByStateWithPeriod",
			argsFunc: func() rivertype.JobArgs {
				type TaskJobArgs struct {
					JobArgsStaticKind
					TaskID string
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_4"},
					TaskID:            "task_123",
				}
			},
			uniqueOpts:   UniqueOpts{ByPeriod: time.Hour, ByState: []rivertype.JobState{rivertype.JobStateCompleted}},
			expectedJSON: "&kind=worker_4&period=" + now.Truncate(time.Hour).Format(time.RFC3339),
		},
		{
			name: "ExcludeKindByArgs",
			argsFunc: func() rivertype.JobArgs {
				type TaskJobArgs struct {
					JobArgsStaticKind
					TaskID string `json:"task_id"`
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_5"},
					TaskID:            "task_123",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true, ExcludeKind: true},
			expectedJSON: `&args={"task_id":"task_123"}`,
		},
		{
			name: "ByQueue",
			argsFunc: func() rivertype.JobArgs {
				type TaskJobArgs struct {
					JobArgsStaticKind
					TaskID string `json:"task_id"`
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_6"},
					TaskID:            "task_123",
				}
			},
			uniqueOpts:   UniqueOpts{ByQueue: true},
			expectedJSON: `&kind=worker_6&queue=email_queue`,
		},
		{
			name: "EmptyUniqueOpts",
			argsFunc: func() rivertype.JobArgs {
				type TaskJobArgs struct {
					JobArgsStaticKind
					TaskID string `json:"task_id"`
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_7"},
					TaskID:            "task_123",
				}
			},
			uniqueOpts:   UniqueOpts{},
			expectedJSON: `&kind=worker_7`,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := tt.argsFunc()

			encodedArgs, err := json.Marshal(args)
			require.NoError(t, err)

			states := uniqueOptsByStateDefault
			if len(tt.uniqueOpts.ByState) > 0 {
				states = tt.uniqueOpts.ByState
			}

			jobParams := &rivertype.JobInsertParams{
				Args:         args,
				CreatedAt:    &now,
				EncodedArgs:  encodedArgs,
				Kind:         args.Kind(),
				Metadata:     []byte(`{"source":"api"}`),
				Queue:        "email_queue",
				ScheduledAt:  &now,
				State:        "Pending",
				Tags:         []string{"notification", "email"},
				UniqueStates: UniqueStatesToBitmask(states),
			}

			uniqueKeyPreHash, err := buildUniqueKeyString(stubSvc, &tt.uniqueOpts, jobParams)
			require.NoError(t, err)
			require.Equal(t, tt.expectedJSON, uniqueKeyPreHash)
			expectedHash := sha256.Sum256([]byte(tt.expectedJSON))

			uniqueKey, err := UniqueKey(stubSvc, &tt.uniqueOpts, jobParams)
			require.NoError(t, err)
			require.NotNil(t, uniqueKey)

			require.Equal(t, expectedHash[:], uniqueKey, "UniqueKey hash does not match expected value")
		})
	}
}

func TestDefaultUniqueStatesSorted(t *testing.T) {
	t.Parallel()

	states := slices.Clone(uniqueOptsByStateDefault)
	slices.Sort(states)
	require.Equal(t, states, uniqueOptsByStateDefault, "Default unique states should be sorted")
}

func TestUniqueOptsIsEmpty(t *testing.T) {
	t.Parallel()

	emptyOpts := &UniqueOpts{}
	require.True(t, emptyOpts.IsEmpty(), "Empty unique options should be empty")

	require.False(t, (&UniqueOpts{ByArgs: true}).IsEmpty(), "Unique options with ByArgs should not be empty")
	require.False(t, (&UniqueOpts{ByPeriod: time.Minute}).IsEmpty(), "Unique options with ByPeriod should not be empty")
	require.False(t, (&UniqueOpts{ByQueue: true}).IsEmpty(), "Unique options with ByQueue should not be empty")
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateAvailable}}).IsEmpty(), "Unique options with ByState should not be empty")
	require.False(t, (&UniqueOpts{ExcludeKind: true}).IsEmpty(), "Unique options with ExcludeKind should not be empty")

	nonEmptyOpts := &UniqueOpts{
		ByArgs:      true,
		ByPeriod:    time.Minute,
		ByQueue:     true,
		ByState:     []rivertype.JobState{rivertype.JobStateAvailable},
		ExcludeKind: true,
	}
	require.False(t, nonEmptyOpts.IsEmpty(), "Non-empty unique options should not be empty")
}

func TestUniqueOptsStateBitmask(t *testing.T) {
	t.Parallel()

	emptyOpts := &UniqueOpts{}
	require.Equal(t, UniqueStatesToBitmask(uniqueOptsByStateDefault), emptyOpts.StateBitmask(), "Empty unique options should have default bitmask")

	otherStates := []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}
	nonEmptyOpts := &UniqueOpts{
		ByState: otherStates,
	}
	require.Equal(t, UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}), nonEmptyOpts.StateBitmask(), "Non-empty unique options should have correct bitmask")
}

func TestUniqueStatesToBitmask(t *testing.T) {
	t.Parallel()

	bitmask := UniqueStatesToBitmask(uniqueOptsByStateDefault)
	require.Equal(t, byte(0b11110101), bitmask, "Default unique states should be all set except cancelled and discarded")

	for state, position := range jobStateBitPositions {
		bitmask = UniqueStatesToBitmask([]rivertype.JobState{state})
		// Bit shifting uses postgres bit numbering with MSB on the right, so we
		// need to flip the position when shifting manually:
		require.Equal(t, byte(1<<(7-position)), bitmask, "Bitmask should be set for single state %s", state)
	}
}

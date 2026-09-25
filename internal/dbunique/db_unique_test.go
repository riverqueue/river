package dbunique

import (
	"crypto/sha256"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/uniquestates"
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
	stubSvc.StubNow(now)

	tests := []struct {
		name                   string
		argsFunc               func() rivertype.JobArgs
		modifyInsertParamsFunc func(insertParams *rivertype.JobInsertParams)
		uniqueOpts             UniqueOpts
		expectedJSON           string
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
			name: "ByArgsUniqueWithUnnamedJSONTagUsesFieldName",
			argsFunc: func() rivertype.JobArgs {
				//nolint:tagliatelle // non-snake keys are intentional
				type EmailJobArgs struct {
					JobArgsStaticKind

					Recipient  string `json:",omitempty" river:"unique"`
					Subject    string `json:"subject"    river:"unique"`
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
			expectedJSON: `&kind=worker_1&args={"Recipient":"john@example.com","subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithCompatibleJSONNames",
			argsFunc: func() rivertype.JobArgs {
				//nolint:tagliatelle // Exercise names whose escaped sort order differs.
				type Nested struct {
					Amount int    `json:"a&b" river:"unique"`
					Upper  string `json:"Y"   river:"unique"`
				}
				//nolint:tagliatelle // Exercise valid names whose escaping must not change hashes.
				type Args struct {
					JobArgsStaticKind

					Dollar  string `json:"$x"     river:"unique"`
					Nested  Nested `json:"nested"`
					Unicode string `json:"é<"     river:"unique"`
					Upper   string `json:"Y"      river:"unique"`
				}
				return Args{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Dollar:            "dollar",
					Nested:            Nested{Amount: 1, Upper: "nested"},
					Unicode:           "unicode",
					Upper:             "upper",
				}
			},
			uniqueOpts: UniqueOpts{ByArgs: true},
			// Preserve the old byte ordering even though escaped `\$x` sorts after `Y`.
			expectedJSON: `&kind=worker_1&args={"$x":"dollar","Y":"upper","nested":{"Y":"nested","a&b":1},"é\u003c":"unicode"}`,
		},
		{
			name: "ByArgsUniqueWithPathSyntaxInJSONTags",
			argsFunc: func() rivertype.JobArgs {
				type Nested struct {
					Value string `json:"inner@key" river:"unique"`
				}
				//nolint:tagliatelle // non-snake keys are intentional
				type PathSyntaxJobArgs struct {
					JobArgsStaticKind

					Bang    string `json:"!bang"               river:"unique"`
					Colon   string `json:":x"                  river:"unique"`
					Email   string `json:"alice@example.com"   river:"unique"`
					Literal string `json:"outer.key.inner@key" river:"unique"`
					Nested  Nested `json:"outer.key"`
					UserID  string `json:"user.id"             river:"unique"`
					X       string `json:"x"                   river:"unique"`
				}
				return PathSyntaxJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Bang:              "bang",
					Colon:             "colon",
					Email:             "email",
					Literal:           "literal",
					Nested:            Nested{Value: "nested"},
					UserID:            "u1",
					X:                 "x",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"!bang":"bang",":x":"colon","alice@example.com":"email","outer.key":{"inner@key":"nested"},"outer.key.inner@key":"literal","user.id":"u1","x":"x"}`,
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
			name: "ByArgsWithSubstructNonPointer",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient" river:"unique"`
					BCC       string `json:"bcc"`
				}
				type EmailJobArgs struct {
					JobArgsStaticKind

					Addresses EmailAddresses `json:"addresses"`
					Subject   string         `json:"subject"   river:"unique"`
					Body      string         `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Addresses: EmailAddresses{
						Recipient: "john@example.com",
					},
					Subject: "Another Test Email",
					Body:    "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"addresses":{"recipient":"john@example.com"},"subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithSubstructPointer",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient" river:"unique"`
					BCC       string `json:"bcc"`
				}
				type EmailJobArgs struct {
					JobArgsStaticKind

					Addresses *EmailAddresses `json:"addresses"`
					Subject   string          `json:"subject"   river:"unique"`
					Body      string          `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Addresses: &EmailAddresses{
						Recipient: "john@example.com",
					},
					Subject: "Another Test Email",
					Body:    "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"addresses":{"recipient":"john@example.com"},"subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithEmbeddedSubstruct",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient" river:"unique"`
					BCC       string `json:"bcc"`
				}
				type EmailJobArgs struct {
					EmailAddresses
					JobArgsStaticKind

					Subject string `json:"subject" river:"unique"`
					Body    string `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					EmailAddresses: EmailAddresses{
						Recipient: "john@example.com",
					},
					Subject: "Another Test Email",
					Body:    "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"recipient":"john@example.com","subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithEmbeddedNonStruct",
			argsFunc: func() rivertype.JobArgs {
				type MyString string
				type TaskJobArgs struct {
					JobArgsStaticKind
					MyString // anonymous non-struct field; needs to be a custom type because it has to be capitalized to be exported
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_7"},
					MyString:          "my_string_in_anonymous_field",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_7&args={"MyString":"my_string_in_anonymous_field"}`,
		},
		{
			name: "ByArgsWithSubstructTagged",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient" river:"unique"`
					BCC       string `json:"bcc"`
				}
				type EmailJobArgs struct {
					JobArgsStaticKind

					Addresses EmailAddresses `json:"addresses" river:"unique"`
					Subject   string         `json:"subject"   river:"unique"`
					Body      string         `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Addresses: EmailAddresses{
						Recipient: "john@example.com",
					},
					Subject: "Another Test Email",
					Body:    "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"addresses":{"recipient":"john@example.com"},"subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithAllSubstructUntagged",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient"`
					BCC       string `json:"bcc"`
				}
				type EmailJobArgs struct {
					JobArgsStaticKind

					Addresses *EmailAddresses `json:"addresses" river:"unique"`
					Subject   string          `json:"subject"   river:"unique"`
					Body      string          `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Addresses: &EmailAddresses{
						Recipient: "john@example.com",
					},
					Subject: "Another Test Email",
					Body:    "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"addresses":{"recipient":"john@example.com","bcc":""},"subject":"Another Test Email"}`,
		},
		{
			name: "ByArgsWithMultiLevelSubstructPointer",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient" river:"unique"`
				}
				type EmailHeaders struct {
					Addresses *EmailAddresses `json:"addresses" river:"unique"`
					Subject   string          `json:"subject"   river:"unique"`
				}
				type EmailJobArgs struct {
					JobArgsStaticKind

					Headers *EmailHeaders `json:"headers" river:"unique"`
					Body    string        `json:"body"`
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Headers: &EmailHeaders{
						Addresses: &EmailAddresses{
							Recipient: "john@example.com",
						},
						Subject: "Another Test Email",
					},
					Body: "This is another test email.",
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"headers":{"addresses":{"recipient":"john@example.com"},"subject":"Another Test Email"}}`,
		},
		{
			name: "ByArgsUnexportedSkipped",
			argsFunc: func() rivertype.JobArgs {
				type EmailAddresses struct {
					Recipient string `json:"recipient" river:"unique"`
					BCC       string `json:"bcc"`
				}
				type EmailJobArgs struct {
					JobArgsStaticKind

					Subject   string `json:"subject" river:"unique"`
					Body      string `json:"body"`
					addresses EmailAddresses
				}
				return &EmailJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_1"},
					Subject:           "Another Test Email",
					Body:              "This is another test email.",
					addresses: EmailAddresses{
						Recipient: "john@example.com",
					},
				}
			},
			uniqueOpts:   UniqueOpts{ByArgs: true},
			expectedJSON: `&kind=worker_1&args={"subject":"Another Test Email"}`,
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
			name: "ByArgsRecursiveType",
			argsFunc: func() rivertype.JobArgs {
				type RecursiveType struct {
					NotUnique string         `river:"-"`
					Recursive *RecursiveType `river:"unique"` // inner recursive type ignored by River
					String    string         `river:"unique"`
				}
				type TaskJobArgs struct {
					JobArgsStaticKind

					Recursive RecursiveType `river:"unique"`
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_7"},
					Recursive: RecursiveType{
						Recursive: &RecursiveType{
							String: "level2",
						},
						String: "level1",
					},
				}
			},
			uniqueOpts: UniqueOpts{ByArgs: true},

			// Notably, "NotUnique" shows up here inside the inner recursive
			// type because when River saw the recursive typed markd with
			// `unique` again, it just gave up and returned the entire the
			// entire key which is then extracted by gjson. The top-level type
			// also has a "NotUnique", but that's not returned because River was
			// processing this type for the first time.
			expectedJSON: `&kind=worker_7&args={"Recursive":{"Recursive":{"NotUnique":"","Recursive":null,"String":"level2"},"String":"level1"}}`,
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
			name: "PeriodFromScheduledAt",
			argsFunc: func() rivertype.JobArgs {
				type TaskJobArgs struct {
					JobArgsStaticKind
				}
				return TaskJobArgs{
					JobArgsStaticKind: JobArgsStaticKind{kind: "worker_4"},
				}
			},
			modifyInsertParamsFunc: func(insertParams *rivertype.JobInsertParams) {
				insertParams.ScheduledAt = new(now.Add(time.Hour))
			},
			uniqueOpts:   UniqueOpts{ByPeriod: time.Hour},
			expectedJSON: "&kind=worker_4&period=" + now.Add(time.Hour).Truncate(time.Hour).Format(time.RFC3339),
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := tt.argsFunc()

			encodedArgs, err := json.Marshal(args)
			require.NoError(t, err)

			states := uniqueOptsByStateDefault
			if len(tt.uniqueOpts.ByState) > 0 {
				states = tt.uniqueOpts.ByState
			}

			insertParams := &rivertype.JobInsertParams{
				Args:         args,
				CreatedAt:    &now,
				EncodedArgs:  encodedArgs,
				Kind:         args.Kind(),
				Metadata:     []byte(`{"source":"api"}`),
				Queue:        "email_queue",
				ScheduledAt:  &now,
				State:        "Pending",
				Tags:         []string{"notification", "email"},
				UniqueStates: uniquestates.UniqueStatesToBitmask(states),
			}

			if tt.modifyInsertParamsFunc != nil {
				tt.modifyInsertParamsFunc(insertParams)
			}

			uniqueKeyPreHash, err := buildUniqueKeyString(stubSvc, &tt.uniqueOpts, insertParams)
			require.NoError(t, err)
			require.Equal(t, tt.expectedJSON, uniqueKeyPreHash)
			expectedHash := sha256.Sum256([]byte(tt.expectedJSON))

			uniqueKey, err := UniqueKey(stubSvc, &tt.uniqueOpts, insertParams)
			require.NoError(t, err)
			require.NotNil(t, uniqueKey)

			require.Equal(t, expectedHash[:], uniqueKey, "UniqueKey hash does not match expected value")
		})
	}
}

func TestUniqueKeyPeriodUsesUTC(t *testing.T) {
	t.Parallel()

	var (
		// A fixed zone avoids depending on the host's time zone database.
		chicago     = time.FixedZone("CDT", -5*60*60)
		nowUTC      = time.Date(2026, time.September, 24, 12, 34, 56, 0, time.UTC)
		uniqueOpts  = &UniqueOpts{ByPeriod: time.Hour}
		wantPreHash = "&kind=worker_1&period=2026-09-24T12:00:00Z"
	)

	type testBundle struct {
		params  *rivertype.JobInsertParams
		timeGen *riversharedtest.TimeStub
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		timeGen := &riversharedtest.TimeStub{}
		timeGen.StubNow(nowUTC)

		return &testBundle{
			params: &rivertype.JobInsertParams{
				Args:        JobArgsStaticKind{kind: "worker_1"},
				EncodedArgs: []byte(`{}`),
				Kind:        "worker_1",
				Queue:       "default",
			},
			timeGen: timeGen,
		}
	}

	requirePreHash := func(t *testing.T, bundle *testBundle, want string) {
		t.Helper()

		preHash, err := buildUniqueKeyString(bundle.timeGen, uniqueOpts, bundle.params)
		require.NoError(t, err)
		require.Equal(t, want, preHash)

		wantKey := sha256.Sum256([]byte(want))
		uniqueKey, err := UniqueKey(bundle.timeGen, uniqueOpts, bundle.params)
		require.NoError(t, err)
		require.Equal(t, wantKey[:], uniqueKey)
	}

	t.Run("NonUTCClock", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.timeGen.StubNow(nowUTC.In(chicago))

		requirePreHash(t, bundle, wantPreHash)
	})

	t.Run("NonUTCScheduledAt", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.params.ScheduledAt = new(nowUTC.In(chicago))

		requirePreHash(t, bundle, wantPreHash)
	})

	t.Run("PeriodBoundaryIsAbsolute", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// 06:59:59 in a UTC-05:00 zone is 11:59:59 UTC, so the job belongs to
		// the 11:00 UTC period rather than to a period derived from its local
		// wall-clock representation.
		bundle.params.ScheduledAt = new(time.Date(2026, time.September, 24, 6, 59, 59, 0, chicago))

		requirePreHash(t, bundle, "&kind=worker_1&period=2026-09-24T11:00:00Z")
	})

	t.Run("UTCClock", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		requirePreHash(t, bundle, wantPreHash)
	})
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
	require.Equal(t, uniquestates.UniqueStatesToBitmask(uniqueOptsByStateDefault), emptyOpts.StateBitmask(), "Empty unique options should have default bitmask")

	otherStates := []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}
	nonEmptyOpts := &UniqueOpts{
		ByState: otherStates,
	}
	require.Equal(t, uniquestates.UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}), nonEmptyOpts.StateBitmask(), "Non-empty unique options should have correct bitmask")
}

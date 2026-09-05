// Command generateconformance generates language-neutral protocol fixtures
// from River's Go reference implementation.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

const (
	protocolFixturePath = "conformance/fixtures/protocol_values.json"
	uniqueFixturePath   = "conformance/fixtures/unique_keys.json"
)

type allArgs struct {
	Zeta    string `json:"zeta"`
	Alpha   string `json:"alpha"`
	Maximum int64  `json:"maximum"`
}

func (allArgs) Kind() string { return "conformance_all_args" }

type mapOrderArgs struct{}

func (mapOrderArgs) Kind() string { return "conformance_all_args" }

func (mapOrderArgs) MarshalJSON() ([]byte, error) { //nolint:unparam // json.Marshaler requires an error result.
	return []byte(`{"2":2,"10":10,"zero":-0,"😀":1,"":2}`), nil
}

type nestedOrderArgs struct {
	Nested struct {
		// Deliberately non-alphabetical: nested struct wire order is significant.
		Z int `json:"z"`
		A int `json:"a"`
	} `json:"nested"`
}

func (nestedOrderArgs) Kind() string { return "conformance_all_args" }

type numericBoundaryArgs struct {
	Exponent        float64 `json:"exponent"`
	Fraction        float64 `json:"fraction"`
	Maximum         int64   `json:"maximum"`
	Minimum         int64   `json:"minimum"`
	UnsignedMaximum uint64  `json:"unsigned_maximum"`
}

func (numericBoundaryArgs) Kind() string { return "conformance_numeric_boundaries" }

type selectedAccount struct {
	ID      string `json:"id,omitempty"      river:"unique"`
	Ignored string `json:"ignored,omitempty"`
	Region  string `json:"region,omitempty"  river:"unique"`
}

type selectedArgs struct {
	Account selectedAccount `json:"account,omitzero"`
	Ignored bool            `json:"ignored,omitempty"`
	Label   string          `json:"label,omitempty"    river:"unique"`
	PathKey string          `json:"path/key,omitempty" river:"unique"`
}

func (selectedArgs) Kind() string { return "conformance_selected_args" }

type simpleArgs struct {
	ID int64 `json:"id"`
}

func (simpleArgs) Kind() string { return "conformance_simple" }

type fixture struct {
	Schema           string        `json:"$schema"`
	Cases            []fixtureCase `json:"cases"`
	ProtocolRevision int           `json:"protocol_revision"`
}

type fixtureCase struct {
	Args               json.RawMessage `json:"args"`
	ExpectedSHA256     string          `json:"expected_sha256"`
	ExpectedStateMask  byte            `json:"expected_state_mask"`
	Kind               string          `json:"kind"`
	Name               string          `json:"name"`
	Now                time.Time       `json:"now"`
	Options            fixtureOptions  `json:"options"`
	Queue              string          `json:"queue"`
	ScheduledAt        *time.Time      `json:"scheduled_at"`
	SelectedUniquePath []string        `json:"selected_unique_paths"`
}

type fixtureOptions struct {
	ByArgs        bool                 `json:"by_args"`
	ByPeriodNanos int64                `json:"by_period_nanos"`
	ByQueue       bool                 `json:"by_queue"`
	ByState       []rivertype.JobState `json:"by_state,omitempty"`
	ExcludeKind   bool                 `json:"exclude_kind"`
}

type referenceCase struct {
	args                rivertype.JobArgs
	name                string
	now                 time.Time
	opts                dbunique.UniqueOpts
	queue               string
	scheduledAt         *time.Time
	selectedUniquePaths []string
}

type staticClock struct{ now time.Time }

func (clock staticClock) Now() time.Time { return clock.now }
func (staticClock) NowOrNil() *time.Time { return nil }

type protocolFixture struct {
	Schema           string                                `json:"$schema"`
	AttemptError     rivertype.AttemptError                `json:"attempt_error"`
	JobStates        []protocolState                       `json:"job_states"`
	MetadataKeys     map[string]string                     `json:"metadata_keys"`
	Notifications    []protocolNotification                `json:"notifications"`
	ProtocolRevision int                                   `json:"protocol_revision"`
	RetryCases       []protocolRetryCase                   `json:"retry_cases"`
	Topics           map[string]notifier.NotificationTopic `json:"topics"`
}

type protocolNotification struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
	Topic   string          `json:"topic"`
}

type protocolRetryCase struct {
	ErrorCount      uint32    `json:"error_count"`
	ExpectedDelayNS int64     `json:"expected_delay_ns"`
	JobID           int64     `json:"job_id"`
	Now             time.Time `json:"now"`
	Seed            uint64    `json:"seed"`
}

type protocolState struct {
	State rivertype.JobState `json:"state"`
	Bit   byte               `json:"unique_bit"`
}

func main() {
	check := flag.Bool("check", false, "check generated fixtures without writing")
	flag.Parse()

	now := time.Date(2026, time.January, 2, 3, 4, 5, 678_900_000, time.UTC)
	scheduledAt := now.Add(2*time.Hour + 17*time.Minute)
	validCustomStates := []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStateCompleted,
		rivertype.JobStatePending,
		rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}
	references := []referenceCase{
		{
			args:                selectedArgs{},
			name:                "all_selected_fields_omitted",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: []string{"account.id", "account.region", "label", "path/key"},
		},
		{
			args:                selectedArgs{Account: selectedAccount{ID: "acct", Ignored: "irrelevant", Region: "west"}, PathKey: "slash"},
			name:                "selected_siblings_and_slash_key",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: []string{"account.id", "account.region", "label", "path/key"},
		},
		{
			args: nestedOrderArgs{Nested: struct {
				Z int `json:"z"`
				A int `json:"a"`
			}{Z: 1, A: 2}},
			name:  "nested_struct_wire_order",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args: allArgs{
				Alpha:   "<alpha>&\u2028line",
				Maximum: 9_007_199_254_740_991,
				Zeta:    "quoted \\\"value\\\" and \\\\ slash",
			},
			name:  "all_args_sorted_and_escaped",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args:  mapOrderArgs{},
			name:  "map_order_and_negative_zero",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args: numericBoundaryArgs{
				Exponent:        1e100,
				Fraction:        1.25,
				Maximum:         math.MaxInt64,
				Minimum:         math.MinInt64,
				UnsignedMaximum: math.MaxUint64,
			},
			name:  "numeric_boundaries",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args: selectedArgs{
				Account: selectedAccount{ID: "acct-123", Ignored: "not selected"},
				Ignored: true,
				Label:   "selected",
			},
			name:                "selected_nested_args",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: []string{"account.id", "account.region", "label", "path/key"},
		},
		{
			args:  simpleArgs{ID: 42},
			name:  "period_from_now",
			now:   now,
			opts:  dbunique.UniqueOpts{ByPeriod: 90 * time.Minute},
			queue: "default",
		},
		{
			args:        simpleArgs{ID: 42},
			name:        "period_from_schedule",
			now:         now,
			opts:        dbunique.UniqueOpts{ByPeriod: time.Hour},
			queue:       "default",
			scheduledAt: &scheduledAt,
		},
		{
			args:  simpleArgs{ID: 42},
			name:  "queue_without_kind",
			now:   now,
			opts:  dbunique.UniqueOpts{ByQueue: true, ExcludeKind: true},
			queue: "priority_emails",
		},
		{
			args:        simpleArgs{ID: 42},
			name:        "all_dimensions_custom_states",
			now:         now,
			opts:        dbunique.UniqueOpts{ByArgs: true, ByPeriod: time.Minute, ByQueue: true, ByState: validCustomStates},
			queue:       "priority_emails",
			scheduledAt: &scheduledAt,
		},
	}

	generated := fixture{
		Schema:           "../schema/unique-keys.schema.json",
		ProtocolRevision: 1,
	}
	for _, reference := range references {
		encodedArgs, err := json.Marshal(reference.args)
		if err != nil {
			fatal(err)
		}
		states := rivertype.UniqueOptsByStateDefault()
		if len(reference.opts.ByState) > 0 {
			states = reference.opts.ByState
		}
		key, err := dbunique.UniqueKey(staticClock{now: reference.now}, &reference.opts, &rivertype.JobInsertParams{
			Args:         reference.args,
			EncodedArgs:  encodedArgs,
			Kind:         reference.args.Kind(),
			Queue:        reference.queue,
			ScheduledAt:  reference.scheduledAt,
			UniqueStates: uniquestates.UniqueStatesToBitmask(states),
		})
		if err != nil {
			fatal(err)
		}
		generated.Cases = append(generated.Cases, fixtureCase{
			Args:              encodedArgs,
			ExpectedSHA256:    hex.EncodeToString(key),
			ExpectedStateMask: uniquestates.UniqueStatesToBitmask(states),
			Kind:              reference.args.Kind(),
			Name:              reference.name,
			Now:               reference.now,
			Options: fixtureOptions{
				ByArgs:        reference.opts.ByArgs,
				ByPeriodNanos: reference.opts.ByPeriod.Nanoseconds(),
				ByQueue:       reference.opts.ByQueue,
				ByState:       reference.opts.ByState,
				ExcludeKind:   reference.opts.ExcludeKind,
			},
			Queue:              reference.queue,
			ScheduledAt:        reference.scheduledAt,
			SelectedUniquePath: reference.selectedUniquePaths,
		})
	}

	writeGenerated(*check, uniqueFixturePath, generated)
	writeGenerated(*check, protocolFixturePath, makeProtocolFixture(now))
}

func deterministicRetryDelay(now time.Time, jobID int64, errorCount uint32, seed uint64) time.Duration {
	const maxRetryNanos = int64(math.MaxInt64)
	baseSeconds := math.Pow(float64(errorCount), 4)
	if baseSeconds*float64(time.Second) >= float64(maxRetryNanos) {
		return time.Duration(maxRetryNanos)
	}
	base := time.Duration(baseSeconds * float64(time.Second))
	var seedBytes [8]byte
	var jobIDBytes [8]byte
	var errorCountBytes [4]byte
	var nowBytes [8]byte
	binary.BigEndian.PutUint64(seedBytes[:], seed)
	jobIDUint, _ := strconv.ParseUint(strconv.FormatInt(jobID, 10), 10, 64)
	binary.BigEndian.PutUint64(jobIDBytes[:], jobIDUint)
	binary.BigEndian.PutUint32(errorCountBytes[:], errorCount)
	binary.BigEndian.PutUint64(nowBytes[:], uint64(now.UnixNano()))
	hash := sha256.New()
	_, _ = hash.Write(seedBytes[:])
	_, _ = hash.Write(jobIDBytes[:])
	_, _ = hash.Write(errorCountBytes[:])
	_, _ = hash.Write(nowBytes[:])
	sum := hash.Sum(nil)
	sample := binary.BigEndian.Uint32(sum[:4])
	ratio := float64(sample) / float64(math.MaxUint32)
	return time.Duration(math.Round(float64(base) * (0.9 + ratio*0.2)))
}

func makeProtocolFixture(now time.Time) protocolFixture {
	states := rivertype.JobStates()
	fixture := protocolFixture{
		Schema: "../schema/protocol-values.schema.json",
		AttemptError: rivertype.AttemptError{
			At:      now,
			Attempt: 3,
			Error:   "worker failed: escaped \"detail\"",
			Trace:   "frame one\nframe two",
		},
		MetadataKeys: map[string]string{
			"output":           rivertype.MetadataKeyOutput,
			"periodic_job_id":  rivercommon.MetadataKeyPeriodicJobID,
			"rescue_count":     rivercommon.MetadataKeyRescueCount,
			"resumable_cursor": rivercommon.MetadataKeyResumableCursor,
			"resumable_step":   rivercommon.MetadataKeyResumableStep,
			"unique_nonce":     rivercommon.MetadataKeyUniqueNonce,
		},
		ProtocolRevision: 1,
		Topics: map[string]notifier.NotificationTopic{
			"control":    notifier.NotificationTopicControl,
			"insert":     notifier.NotificationTopicInsert,
			"leadership": notifier.NotificationTopicLeadership,
		},
	}
	for _, state := range states {
		fixture.JobStates = append(fixture.JobStates, protocolState{
			Bit:   uniquestates.UniqueStatesToBitmask([]rivertype.JobState{state}),
			State: state,
		})
	}
	for _, notification := range []struct {
		name    string
		payload any
		topic   notifier.NotificationTopic
	}{
		{name: "cancel", payload: map[string]any{"action": "cancel", "job_id": 42, "queue": "priority"}, topic: notifier.NotificationTopicControl},
		{name: "insert", payload: map[string]any{"queue": "priority"}, topic: notifier.NotificationTopicInsert},
		{name: "metadata_changed", payload: map[string]any{"action": "metadata_changed", "metadata": map[string]any{"owner": "candidate"}, "queue": "priority"}, topic: notifier.NotificationTopicControl},
		{name: "pause", payload: map[string]any{"action": "pause", "queue": "priority"}, topic: notifier.NotificationTopicControl},
		{name: "request_resign", payload: leadership.DBNotification{Action: leadership.DBNotificationKindRequestResign}, topic: notifier.NotificationTopicLeadership},
		{name: "resigned", payload: leadership.DBNotification{Action: leadership.DBNotificationKindResigned, LeaderID: "client-1"}, topic: notifier.NotificationTopicLeadership},
		{name: "resume", payload: map[string]any{"action": "resume", "queue": "priority"}, topic: notifier.NotificationTopicControl},
	} {
		payload, err := json.Marshal(notification.payload)
		if err != nil {
			fatal(err)
		}
		fixture.Notifications = append(fixture.Notifications, protocolNotification{
			Name:    notification.name,
			Payload: payload,
			Topic:   string(notification.topic),
		})
	}
	for _, testCase := range []struct {
		errorCount uint32
		jobID      int64
		seed       uint64
	}{
		{errorCount: 1, jobID: 42, seed: 0},
		{errorCount: 2, jobID: 42, seed: 123},
		{errorCount: 3, jobID: 9_007_199_254_740_991, seed: math.MaxUint64},
		{errorCount: 11, jobID: 1, seed: 456},
		{errorCount: 310, jobID: 42, seed: 123},
	} {
		fixture.RetryCases = append(fixture.RetryCases, protocolRetryCase{
			ErrorCount:      testCase.errorCount,
			ExpectedDelayNS: deterministicRetryDelay(now, testCase.jobID, testCase.errorCount, testCase.seed).Nanoseconds(),
			JobID:           testCase.jobID,
			Now:             now,
			Seed:            testCase.seed,
		})
	}
	return fixture
}

func writeGenerated(check bool, path string, value any) {
	contents, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fatal(err)
	}
	contents = append(contents, '\n')
	if check {
		actual, err := os.ReadFile(path)
		if err != nil {
			fatal(err)
		}
		if !bytes.Equal(actual, contents) {
			fatal(fmt.Errorf("generated file is stale: %s (run make generate/conformance)", path))
		}
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatal(err)
	}
	//nolint:gosec // Generated repository artifacts are intentionally world-readable.
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

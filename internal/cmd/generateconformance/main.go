// Command generateconformance generates language-neutral protocol fixtures
// from River's Go reference implementation.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/retrypolicy"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

const (
	featureInventoryPath = "conformance/feature-inventory.json"
	protocolFixturePath  = "conformance/fixtures/protocol_values.json"
	uniqueFixturePath    = "conformance/fixtures/unique_keys.json"
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

// collectionsArgs exercises nested values, arrays, and nulls whose wire
// order and representation are preserved in hashed arguments.
type collectionsArgs struct {
	Empty   []string          `json:"empty"`
	Labels  map[string]string `json:"labels"`
	Matrix  [][]int           `json:"matrix"`
	Missing []string          `json:"missing"`
	Objects []collectionsItem `json:"objects"`
	Pointer *string           `json:"pointer"`
}

type collectionsItem struct {
	// Deliberately non-alphabetical: nested struct wire order is significant.
	Zulu  string `json:"zulu"`
	Alpha *int   `json:"alpha"`
}

func (collectionsArgs) Kind() string { return "conformance_all_args" }

type emptyArgs struct{}

func (emptyArgs) Kind() string { return "conformance_all_args" }

// escapingArgs exercises encoding/json string and key escaping, including
// keys that gjson reports unescaped and sjson rewrites while hashing.
type escapingArgs struct {
	Angle      string         `json:"a<b>"`
	Controls   string         `json:"controls"`
	HTML       string         `json:"html"`
	Keys       map[string]int `json:"keys"`
	Separators string         `json:"separators"`
	Unicode    string         `json:"unicode"`
	UnicodeAmp string         `json:"é&"`
}

func (escapingArgs) Kind() string { return "conformance_all_args" }

// selectedNullArgs selects an explicitly null field, which is retained in the
// hashed arguments, while omitted selected fields are skipped.
type selectedNullArgs struct {
	Account selectedAccount `json:"account,omitzero"`
	Label   *string         `json:"label"              river:"unique"`
	PathKey string          `json:"path/key,omitempty" river:"unique"`
}

func (selectedNullArgs) Kind() string { return "conformance_selected_args" }

// timeArgs exercises encoding/json time formatting, which trims fractional
// seconds to their shortest form.
type timeArgs struct {
	Fraction time.Time `json:"fraction"`
	Micros   time.Time `json:"micros"`
	Millis   time.Time `json:"millis"`
	Whole    time.Time `json:"whole"`
}

func (timeArgs) Kind() string { return "conformance_all_args" }

// typedFloatArgs exercises encoding/json float formatting: 'f' notation
// between 1e-6 and 1e21, exponent notation outside it, and shortest
// round-trip digits for both 64- and 32-bit floats.
type typedFloatArgs struct {
	BelowLarge    float64 `json:"below_large"`
	Large         float64 `json:"large"`
	LargeBoundary float64 `json:"large_boundary"`
	Largest       float64 `json:"largest"`
	Negative      float64 `json:"negative"`
	NegativeZero  float64 `json:"negative_zero"`
	One           float64 `json:"one"`
	Single        float32 `json:"single"`
	SingleLarge   float32 `json:"single_large"`
	SingleSmall   float32 `json:"single_small"`
	Small         float64 `json:"small"`
	SmallBoundary float64 `json:"small_boundary"`
	Smallest      float64 `json:"smallest"`
	Tenth         float64 `json:"tenth"`
}

func (typedFloatArgs) Kind() string { return "conformance_all_args" }

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
	Schema               string                                `json:"$schema"`
	AttemptError         rivertype.AttemptError                `json:"attempt_error"`
	JobStates            []protocolState                       `json:"job_states"`
	MetadataKeys         map[string]string                     `json:"metadata_keys"`
	Notifications        []protocolNotification                `json:"notifications"`
	ProtocolRevision     int                                   `json:"protocol_revision"`
	ReservedMetadataKeys []reservedMetadataKey                 `json:"reserved_metadata_keys"`
	RetryCases           []protocolRetryCase                   `json:"retry_cases"`
	Topics               map[string]notifier.NotificationTopic `json:"topics"`
}

type protocolNotification struct {
	Fields  []jsonField     `json:"fields"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
	Source  string          `json:"source"`
	Topic   string          `json:"topic"`
}

// protocolRetryCase bounds the delay River's default retry policy schedules
// after error_count failures, from internal/retrypolicy.DelayBounds.
// Implementations with seedable jitter may use seed; the bounds hold for any.
type protocolRetryCase struct {
	ErrorCount uint32    `json:"error_count"`
	JobID      int64     `json:"job_id"`
	MaxDelayNS int64     `json:"max_delay_ns"`
	MinDelayNS int64     `json:"min_delay_ns"`
	Now        time.Time `json:"now"`
	Seed       uint64    `json:"seed"`
}

// reservedMetadataKey is a job metadata key River itself reads or writes,
// as extracted from Go source and SQL into the feature inventory.
type reservedMetadataKey struct {
	Applicability string `json:"applicability"`
	Key           string `json:"key"`
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
			args: collectionsArgs{
				Empty:   []string{},
				Labels:  map[string]string{"zulu": "last", "alpha": "first", "10": "ten", "2": "two"},
				Matrix:  [][]int{{3, 1}, {}, {2}},
				Objects: []collectionsItem{{Zulu: "z", Alpha: new(1)}, {Zulu: "y"}},
			},
			name:  "typed_collections_and_nulls",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args:  emptyArgs{},
			name:  "typed_empty_args",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args: escapingArgs{
				Angle:      "<angle>",
				Controls:   "\b\f\n\r\t\x00\x01\x1f\x7f",
				HTML:       `<a href="x">&amp;</a>`,
				Keys:       map[string]int{"<k>": 1, "a&b": 2, "é": 3, "é<": 4},
				Separators: "line\u2028paragraph\u2029end",
				Unicode:    "é😀/\\",
				UnicodeAmp: "unicode key",
			},
			name:  "typed_escaping",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args:                selectedNullArgs{},
			name:                "selected_explicit_null",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: []string{"account.id", "account.region", "label", "path/key"},
		},
		{
			args: timeArgs{
				Fraction: time.Date(2026, time.January, 2, 3, 4, 5, 500_000_000, time.UTC),
				Micros:   time.Date(2026, time.January, 2, 3, 4, 5, 123_456_000, time.UTC),
				Millis:   time.Date(2026, time.January, 2, 3, 4, 5, 120_000_000, time.UTC),
				Whole:    time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
			},
			name:  "typed_time_values",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args: typedFloatArgs{
				BelowLarge:    math.Nextafter(1e21, 0),
				Large:         1e20,
				LargeBoundary: 1e21,
				Largest:       math.MaxFloat64,
				Negative:      -1.5e-9,
				NegativeZero:  math.Copysign(0, -1),
				One:           1,
				Single:        1.1,
				SingleLarge:   1e21,
				SingleSmall:   1e-7,
				Small:         1e-7,
				SmallBoundary: 1e-6,
				Smallest:      math.SmallestNonzeroFloat64,
				Tenth:         0.1,
			},
			name:  "typed_float_formatting",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
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
			// A process clock outside UTC must produce the same period as UTC.
			args:  simpleArgs{ID: 42},
			name:  "period_from_non_utc_now",
			now:   now.In(time.FixedZone("UTC-5", -5*60*60)),
			opts:  dbunique.UniqueOpts{ByPeriod: time.Hour},
			queue: "default",
		},
		{
			// A half-hour offset puts the local wall-clock hour in a different
			// UTC hour, so a local truncation would pick the wrong period.
			args:        simpleArgs{ID: 42},
			name:        "period_from_non_utc_schedule",
			now:         now,
			opts:        dbunique.UniqueOpts{ByPeriod: time.Hour},
			queue:       "default",
			scheduledAt: new(scheduledAt.In(time.FixedZone("UTC+5:30", 5*60*60+30*60))),
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
	writeGenerated(*check, maintenanceFixturePath, makeMaintenanceFixture())
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
	notifications, err := makeProtocolNotifications()
	if err != nil {
		fatal(err)
	}
	fixture.Notifications = notifications
	reserved, err := readReservedMetadataKeys()
	if err != nil {
		fatal(err)
	}
	fixture.ReservedMetadataKeys = reserved
	for _, testCase := range []struct {
		errorCount uint32
		jobID      int64
		seed       uint64
	}{
		{errorCount: 1, jobID: 42, seed: 0},
		{errorCount: 2, jobID: 42, seed: 123},
		{errorCount: 3, jobID: 9_007_199_254_740_991, seed: math.MaxUint64},
		{errorCount: 11, jobID: 1, seed: 456},
		{errorCount: 309, jobID: 42, seed: 789},
		{errorCount: 310, jobID: 42, seed: 123},
	} {
		minDelay, maxDelay := retrypolicy.DelayBounds(int(testCase.errorCount))
		fixture.RetryCases = append(fixture.RetryCases, protocolRetryCase{
			ErrorCount: testCase.errorCount,
			JobID:      testCase.jobID,
			MaxDelayNS: maxDelay.Nanoseconds(),
			MinDelayNS: minDelay.Nanoseconds(),
			Now:        now,
			Seed:       testCase.seed,
		})
	}
	return fixture
}

// makeProtocolNotifications derives notification payload goldens from the Go
// payload structs and action constants, and checks that the payloads the SQL
// queries build use the same keys.
func makeProtocolNotifications() ([]protocolNotification, error) {
	controlFields, err := sourceStructJSONFields("producer.go", "controlEventPayload")
	if err != nil {
		return nil, err
	}
	insertFields, err := sourceStructJSONFields("producer.go", "insertPayload")
	if err != nil {
		return nil, err
	}
	leadershipFields, err := sourceStructJSONFields("internal/leadership/elector.go", "DBNotification")
	if err != nil {
		return nil, err
	}
	controlActions, err := sourceStringConstants("producer.go", "controlAction")
	if err != nil {
		return nil, err
	}
	leadershipActions, err := sourceStringConstants("internal/leadership/elector.go", "DBNotificationKind")
	if err != nil {
		return nil, err
	}
	examples := map[string]any{
		"job_id":    42,
		"leader_id": "client-1",
		"metadata":  map[string]any{"owner": "candidate"},
		"queue":     "priority",
	}
	var notifications []protocolNotification
	for _, constant := range slices.Sorted(maps.Keys(controlActions)) {
		action := controlActions[constant]
		values := map[string]any{"action": action, "queue": examples["queue"]}
		switch action {
		case "cancel":
			values["job_id"] = examples["job_id"]
		case "metadata_changed":
			values["metadata"] = examples["metadata"]
		}
		notification, err := newProtocolNotification(action, string(notifier.NotificationTopicControl), "producer.go:controlEventPayload", controlFields, values)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}
	insert, err := newProtocolNotification("insert", string(notifier.NotificationTopicInsert), "producer.go:insertPayload", insertFields, map[string]any{"queue": examples["queue"]})
	if err != nil {
		return nil, err
	}
	notifications = append(notifications, insert)
	for _, constant := range slices.Sorted(maps.Keys(leadershipActions)) {
		action := leadershipActions[constant]
		leaderID := ""
		if action == string(leadership.DBNotificationKindResigned) {
			leaderID = "client-1"
		}
		notification, err := newProtocolNotification(action, string(notifier.NotificationTopicLeadership), "internal/leadership/elector.go:DBNotification", leadershipFields, map[string]any{"action": action, "leader_id": leaderID})
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}

	// Some notifications are built in SQL rather than Go. Their keys must
	// match the payload structs consumers decode them into.
	for _, check := range []struct {
		name  string
		path  string
		query string
	}{
		{name: "cancel", path: "riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql", query: "JobCancel"},
		{name: "resigned", path: "riverdriver/riverpgxv5/internal/dbsqlc/river_leader.sql", query: "LeaderResign"},
	} {
		keys, err := sqlNotificationKeys(check.path, check.query)
		if err != nil {
			return nil, err
		}
		index := slices.IndexFunc(notifications, func(notification protocolNotification) bool { return notification.Name == check.name })
		if index < 0 {
			return nil, fmt.Errorf("no %s notification to compare with %s", check.name, check.query)
		}
		var payload map[string]any
		if err := json.Unmarshal(notifications[index].Payload, &payload); err != nil {
			return nil, err
		}
		if expected := slices.Sorted(maps.Keys(payload)); !slices.Equal(expected, keys) {
			return nil, fmt.Errorf("%s notification keys %v from %s differ from the Go payload keys %v", check.name, keys, check.query, expected)
		}
		notifications[index].Source += "; " + check.path + ":" + check.query
	}
	slices.SortFunc(notifications, func(a, b protocolNotification) int { return strings.Compare(a.Name, b.Name) })
	return notifications, nil
}

// newProtocolNotification encodes values in the struct's field order,
// omitting empty omitempty fields as encoding/json does.
func newProtocolNotification(name, topic, source string, fields []jsonField, values map[string]any) (protocolNotification, error) {
	var payload bytes.Buffer
	payload.WriteByte('{')
	for _, field := range fields {
		value, ok := values[field.Name]
		if !ok && !field.OmitEmpty {
			return protocolNotification{}, fmt.Errorf("%s notification has no value for required field %s", name, field.Name)
		}
		if !ok {
			continue
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return protocolNotification{}, err
		}
		if payload.Len() > 1 {
			payload.WriteByte(',')
		}
		key, err := json.Marshal(field.Name)
		if err != nil {
			return protocolNotification{}, err
		}
		payload.Write(key)
		payload.WriteByte(':')
		payload.Write(encoded)
	}
	payload.WriteByte('}')
	for key := range values {
		if !slices.ContainsFunc(fields, func(field jsonField) bool { return field.Name == key }) {
			return protocolNotification{}, fmt.Errorf("%s notification value %s is not a payload field", name, key)
		}
	}
	return protocolNotification{Fields: fields, Name: name, Payload: payload.Bytes(), Source: source, Topic: topic}, nil
}

// readReservedMetadataKeys returns the metadata keys the feature inventory
// extracted from Go source and SQL, with their applicability.
func readReservedMetadataKeys() ([]reservedMetadataKey, error) {
	contents, err := os.ReadFile(featureInventoryPath)
	if err != nil {
		return nil, err
	}
	var inventory struct {
		Items []struct {
			Applicability string `json:"applicability"`
			Area          string `json:"area"`
			ID            string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(contents, &inventory); err != nil {
		return nil, fmt.Errorf("decode %s: %w", featureInventoryPath, err)
	}
	var keys []reservedMetadataKey
	for _, item := range inventory.Items {
		if item.Area == "metadata_key" {
			keys = append(keys, reservedMetadataKey{
				Applicability: item.Applicability,
				Key:           strings.TrimPrefix(item.ID, "metadata_key."),
			})
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%s lists no metadata keys", featureInventoryPath)
	}
	slices.SortFunc(keys, func(a, b reservedMetadataKey) int { return strings.Compare(a.Key, b.Key) })
	return keys, nil
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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/retrypolicy"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

type protocolJobState struct {
	State     rivertype.JobState `json:"state"`
	UniqueBit byte               `json:"unique_bit"`
}

type protocolNotification struct {
	Fields  []jsonField     `json:"fields"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
	Source  string          `json:"source"`
	Topic   string          `json:"topic"`
}

// protocolRetryCase bounds the delay River's default retry policy schedules
// after error_count failures. Ports with seedable jitter may use seed; the
// bounds hold for any seed.
type protocolRetryCase struct {
	ErrorCount uint32    `json:"error_count"`
	JobID      int64     `json:"job_id"`
	MaxDelayNS int64     `json:"max_delay_ns"`
	MinDelayNS int64     `json:"min_delay_ns"`
	Now        time.Time `json:"now"`
	Seed       uint64    `json:"seed"`
}

type protocolValues struct {
	Comment       string                                `json:"$comment"`
	AttemptError  rivertype.AttemptError                `json:"attempt_error"`
	JobStates     []protocolJobState                    `json:"job_states"`
	MetadataKeys  map[string]string                     `json:"metadata_keys"`
	Notifications []protocolNotification                `json:"notifications"`
	RetryCases    []protocolRetryCase                   `json:"retry_cases"`
	Topics        map[string]notifier.NotificationTopic `json:"topics"`
}

// makeProtocolValues records values that River processes must agree on to
// share a database: job states and their unique bits, the attempt error
// encoding, reserved metadata keys, notification topics and payloads, and
// retry delay bounds. root is the repository root, used to read the Go and SQL
// sources that notification payloads are derived from.
func makeProtocolValues(root string) (protocolValues, error) {
	fixture := protocolValues{
		Comment: generatedComment("River's job state, metadata key, notification, attempt error, and retry policy definitions"),
		AttemptError: rivertype.AttemptError{
			At:      referenceNow,
			Attempt: 3,
			Error:   `worker failed: escaped "detail"`,
			Trace:   "frame one\nframe two",
		},
		MetadataKeys: map[string]string{
			"output":           rivertype.MetadataKeyOutput,
			"periodic_job_id":  rivercommon.MetadataKeyPeriodicJobID,
			"rescue_count":     rivercommon.MetadataKeyRescueCount,
			"resumable_cursor": rivercommon.MetadataKeyResumableCursor,
			"resumable_step":   rivercommon.MetadataKeyResumableStep,
			"unique_nonce":     riverdriver.UniqueInsertMetadataKey,
		},
		Topics: map[string]notifier.NotificationTopic{
			"control":    notifier.NotificationTopicControl,
			"insert":     notifier.NotificationTopicInsert,
			"leadership": notifier.NotificationTopicLeadership,
		},
	}

	for _, state := range rivertype.JobStates() {
		fixture.JobStates = append(fixture.JobStates, protocolJobState{
			State:     state,
			UniqueBit: uniquestates.UniqueStatesToBitmask([]rivertype.JobState{state}),
		})
	}

	notifications, err := makeProtocolNotifications(root)
	if err != nil {
		return protocolValues{}, err
	}
	fixture.Notifications = notifications

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
			Now:        referenceNow,
			Seed:       testCase.seed,
		})
	}

	return fixture, nil
}

// makeProtocolNotifications derives notification payload goldens from the Go
// payload structs and action constants, and checks that payloads built in SQL
// use the same keys.
func makeProtocolNotifications(root string) ([]protocolNotification, error) {
	const (
		electorSource  = "internal/leadership/elector.go"
		producerSource = "producer.go"
	)

	controlFields, err := sourceStructJSONFields(filepath.Join(root, producerSource), "controlEventPayload")
	if err != nil {
		return nil, err
	}
	insertFields, err := sourceStructJSONFields(filepath.Join(root, producerSource), "insertPayload")
	if err != nil {
		return nil, err
	}
	leadershipFields, err := sourceStructJSONFields(filepath.Join(root, electorSource), "DBNotification")
	if err != nil {
		return nil, err
	}
	controlActions, err := sourceStringConstants(filepath.Join(root, producerSource), "controlAction")
	if err != nil {
		return nil, err
	}
	leadershipActions, err := sourceStringConstants(filepath.Join(root, electorSource), "DBNotificationKind")
	if err != nil {
		return nil, err
	}

	const (
		exampleJobID    = 42
		exampleLeaderID = "client-1"
		exampleQueue    = "priority"
	)

	var notifications []protocolNotification
	for _, constant := range slices.Sorted(maps.Keys(controlActions)) {
		action := controlActions[constant]
		values := map[string]any{"action": action, "queue": exampleQueue}
		switch action {
		case "cancel":
			values["job_id"] = exampleJobID
		case "metadata_changed":
			values["metadata"] = map[string]any{"owner": "candidate"}
		}
		notification, err := newProtocolNotification(action, string(notifier.NotificationTopicControl), producerSource+":controlEventPayload", controlFields, values)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}

	insert, err := newProtocolNotification("insert", string(notifier.NotificationTopicInsert), producerSource+":insertPayload", insertFields, map[string]any{"queue": exampleQueue})
	if err != nil {
		return nil, err
	}
	notifications = append(notifications, insert)

	for _, constant := range slices.Sorted(maps.Keys(leadershipActions)) {
		action := leadershipActions[constant]
		leaderID := ""
		if action == string(leadership.DBNotificationKindResigned) {
			leaderID = exampleLeaderID
		}
		notification, err := newProtocolNotification(action, string(notifier.NotificationTopicLeadership), electorSource+":DBNotification", leadershipFields, map[string]any{"action": action, "leader_id": leaderID})
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}

	// Some notifications are built in SQL rather than Go. Their keys must
	// match the payload structs consumers decode them into.
	for _, sqlSource := range []struct {
		name  string
		path  string
		query string
	}{
		{name: "cancel", path: "riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql", query: "JobCancel"},
		{name: "resigned", path: "riverdriver/riverpgxv5/internal/dbsqlc/river_leader.sql", query: "LeaderResign"},
	} {
		keys, err := sqlNotificationKeys(filepath.Join(root, sqlSource.path), sqlSource.query)
		if err != nil {
			return nil, err
		}
		index := slices.IndexFunc(notifications, func(notification protocolNotification) bool { return notification.Name == sqlSource.name })
		if index < 0 {
			return nil, fmt.Errorf("no %s notification to compare with %s", sqlSource.name, sqlSource.query)
		}
		var payload map[string]any
		if err := json.Unmarshal(notifications[index].Payload, &payload); err != nil {
			return nil, fmt.Errorf("error decoding %s notification payload: %w", sqlSource.name, err)
		}
		if expected := slices.Sorted(maps.Keys(payload)); !slices.Equal(expected, keys) {
			return nil, fmt.Errorf("%s notification keys %v from %s differ from the Go payload keys %v", sqlSource.name, keys, sqlSource.query, expected)
		}
		notifications[index].Source += "; " + sqlSource.path + ":" + sqlSource.query
	}

	slices.SortFunc(notifications, func(a, b protocolNotification) int { return strings.Compare(a.Name, b.Name) })
	return notifications, nil
}

// newProtocolNotification encodes values in the payload struct's field order,
// omitting absent omitempty fields as encoding/json does.
func newProtocolNotification(name, topic, source string, fields []jsonField, values map[string]any) (protocolNotification, error) {
	for key := range values {
		if !slices.ContainsFunc(fields, func(field jsonField) bool { return field.Name == key }) {
			return protocolNotification{}, fmt.Errorf("%s notification value %s is not a payload field", name, key)
		}
	}

	var payload bytes.Buffer
	payload.WriteByte('{')
	for _, field := range fields {
		value, ok := values[field.Name]
		if !ok {
			if !field.OmitEmpty {
				return protocolNotification{}, fmt.Errorf("%s notification has no value for required field %s", name, field.Name)
			}
			continue
		}

		key, err := json.Marshal(field.Name)
		if err != nil {
			return protocolNotification{}, fmt.Errorf("error encoding %s notification key %s: %w", name, field.Name, err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return protocolNotification{}, fmt.Errorf("error encoding %s notification value %s: %w", name, field.Name, err)
		}
		if payload.Len() > 1 {
			payload.WriteByte(',')
		}
		payload.Write(key)
		payload.WriteByte(':')
		payload.Write(encoded)
	}
	payload.WriteByte('}')

	return protocolNotification{Fields: fields, Name: name, Payload: payload.Bytes(), Source: source, Topic: topic}, nil
}

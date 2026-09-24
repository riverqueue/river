package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/tidwall/gjson"
)

const maintenanceFixturePath = "conformance/fixtures/maintenance_values.json"

// cronNextCount is the number of successive occurrences recorded per cron
// case. Each occurrence is computed from the previous one, like the periodic
// job enqueuer advancing its schedule.
const cronNextCount = 5

type maintenanceFixture struct {
	Schema           string              `json:"$schema"`
	CronCases        []cronCase          `json:"cron_cases"`
	CronInvalid      []string            `json:"cron_invalid"`
	ProtocolRevision int                 `json:"protocol_revision"`
	SnoozeCounters   []snoozeCounterCase `json:"snooze_counters"`
}

type cronCase struct {
	Expression string      `json:"expression"`
	From       time.Time   `json:"from"`
	Name       string      `json:"name"`
	Next       []time.Time `json:"next"`
}

type snoozeCounterCase struct {
	ExpectedSnoozes int64           `json:"expected_snoozes"`
	Metadata        json.RawMessage `json:"metadata"`
	Name            string          `json:"name"`
}

// makeMaintenanceFixture records Go's standard cron semantics (robfig/cron
// `ParseStandard`, as documented for River periodic jobs) and the snooze
// counter coercion used by the job executor.
func makeMaintenanceFixture() maintenanceFixture {
	fixture := maintenanceFixture{
		Schema:           "../schema/maintenance-values.schema.json",
		ProtocolRevision: 1,
	}

	utcFrom := time.Date(2026, time.January, 2, 3, 4, 5, 678_900_000, time.UTC)
	eastern := time.FixedZone("", -5*60*60)
	kolkata := time.FixedZone("", 5*60*60+30*60)
	for _, testCase := range []struct {
		expression string
		from       time.Time
		name       string
	}{
		{expression: "* * * * *", from: utcFrom, name: "every_minute"},
		{expression: "30 * * * *", from: utcFrom, name: "half_past_every_hour"},
		{expression: "0 9 * * 1", from: utcFrom, name: "monday_numeric_weekday"},
		{expression: "0 9 * * mon", from: utcFrom, name: "monday_named_weekday"},
		{expression: "0 0 * * 0", from: utcFrom, name: "sunday_is_zero"},
		{expression: "0 0 * * SUN", from: utcFrom, name: "weekday_names_ignore_case"},
		{expression: "*/15 9-17 * * mon-fri", from: utcFrom, name: "business_hours_steps"},
		{expression: "0 0 1 * *", from: utcFrom, name: "first_of_month"},
		{expression: "0 0 1 jan,JUL *", from: utcFrom, name: "named_months"},
		{expression: "0 0 29 2 *", from: utcFrom, name: "leap_day"},
		{expression: "0 0 30 2 *", from: utcFrom, name: "impossible_date_never_runs"},
		{expression: "0 12 1,15 * 5", from: utcFrom, name: "day_of_month_or_weekday"},
		{expression: "0 12 * * 5", from: utcFrom, name: "wildcard_day_of_month_and_weekday"},
		{expression: "0 12 ? * 5", from: utcFrom, name: "question_mark_wildcard"},
		{expression: "0 12 */2 * 5", from: utcFrom, name: "stepped_day_of_month_or_weekday"},
		{expression: "0 12 */1 * 5", from: utcFrom, name: "unit_step_keeps_wildcard"},
		{expression: "5/15 * * * *", from: utcFrom, name: "start_with_step"},
		{expression: "0-10/5 * * * *", from: utcFrom, name: "range_with_step"},
		{expression: "59 23 31 12 *", from: utcFrom, name: "year_end"},
		{expression: "@hourly", from: utcFrom, name: "descriptor_hourly"},
		{expression: "@daily", from: utcFrom, name: "descriptor_daily"},
		{expression: "@midnight", from: utcFrom, name: "descriptor_midnight"},
		{expression: "@weekly", from: utcFrom, name: "descriptor_weekly"},
		{expression: "@monthly", from: utcFrom, name: "descriptor_monthly"},
		{expression: "@yearly", from: utcFrom, name: "descriptor_yearly"},
		{expression: "@annually", from: utcFrom, name: "descriptor_annually"},
		{expression: "@every 1h30m", from: utcFrom, name: "every_compound_duration"},
		{expression: "@every 1.5h", from: utcFrom, name: "every_fractional_duration"},
		{expression: "@every 90s", from: utcFrom, name: "every_seconds"},
		{expression: "@every 500ms", from: utcFrom, name: "every_rounds_up_to_one_second"},
		{expression: "@every 1500ms", from: utcFrom, name: "every_truncates_subseconds"},
		{expression: "0 9 * * *", from: time.Date(2026, time.March, 7, 8, 0, 0, 0, eastern), name: "reference_time_offset"},
		{expression: "30 0 * * *", from: time.Date(2026, time.March, 7, 23, 45, 0, 0, kolkata), name: "reference_time_half_hour_offset"},
		{expression: "CRON_TZ=UTC 0 9 * * *", from: time.Date(2026, time.March, 7, 8, 0, 0, 0, eastern), name: "cron_tz_utc_prefix"},
		{expression: "TZ=UTC 0 9 * * *", from: time.Date(2026, time.March, 7, 8, 0, 0, 0, eastern), name: "tz_utc_prefix"},
		{expression: "  0   9 * *   1 ", from: utcFrom, name: "extra_whitespace"},
	} {
		schedule, err := cron.ParseStandard(testCase.expression)
		if err != nil {
			fatal(err)
		}
		next := make([]time.Time, 0, cronNextCount)
		current := testCase.from
		for range cronNextCount {
			current = schedule.Next(current)
			if current.IsZero() {
				break
			}
			next = append(next, current)
		}
		fixture.CronCases = append(fixture.CronCases, cronCase{
			Expression: testCase.expression,
			From:       testCase.from,
			Name:       testCase.name,
			Next:       next,
		})
	}

	for _, expression := range []string{
		"",
		"* * * *",
		"* * * * * *",
		"0 9 * * 7",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * 32 * *",
		"* * * 0 *",
		"* * * 13 *",
		"-1 * * * *",
		"5-1 * * * *",
		"1-2-3 * * * *",
		"1/2/3 * * * *",
		"*/0 * * * *",
		"*/x * * * *",
		"0 9 * * funday",
		"@every",
		"@every 5x",
		"@reboot",
		"CRON_TZ=Nowhere/Invalid 0 9 * * *",
	} {
		if _, err := cron.ParseStandard(expression); err == nil {
			fatal(fmt.Errorf("cron expression unexpectedly parsed: %q", expression))
		}
		fixture.CronInvalid = append(fixture.CronInvalid, expression)
	}

	for _, testCase := range []struct {
		metadata string
		name     string
	}{
		{metadata: `{}`, name: "absent"},
		{metadata: `{"snoozes":2}`, name: "integer"},
		{metadata: `{"snoozes":2.9}`, name: "fraction_truncates"},
		{metadata: `{"snoozes":-2.5}`, name: "negative_fraction_truncates_toward_zero"},
		{metadata: `{"snoozes":1e3}`, name: "exponent"},
		{metadata: `{"snoozes":9007199254740993}`, name: "beyond_float_precision"},
		{metadata: `{"snoozes":"4"}`, name: "numeric_string"},
		{metadata: `{"snoozes":"-7"}`, name: "negative_numeric_string"},
		{metadata: `{"snoozes":"4.5"}`, name: "fractional_string_is_zero"},
		{metadata: `{"snoozes":" 5"}`, name: "padded_string_is_zero"},
		{metadata: `{"snoozes":"abc"}`, name: "non_numeric_string_is_zero"},
		{metadata: `{"snoozes":true}`, name: "true_is_one"},
		{metadata: `{"snoozes":false}`, name: "false_is_zero"},
		{metadata: `{"snoozes":null}`, name: "null_is_zero"},
		{metadata: `{"snoozes":[3]}`, name: "array_is_zero"},
		{metadata: `{"snoozes":{"count":3}}`, name: "object_is_zero"},
	} {
		// Mirrors the job executor's snooze bookkeeping.
		fixture.SnoozeCounters = append(fixture.SnoozeCounters, snoozeCounterCase{
			ExpectedSnoozes: gjson.GetBytes([]byte(testCase.metadata), "snoozes").Int() + 1,
			Metadata:        json.RawMessage(testCase.metadata),
			Name:            testCase.name,
		})
	}

	return fixture
}

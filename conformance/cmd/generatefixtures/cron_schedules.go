package main

import (
	"fmt"
	"time"
	_ "time/tzdata" // named cron zones resolve the same on every host

	"github.com/robfig/cron/v3"
)

// cronNextCount is the number of successive occurrences recorded per cron
// case. Each is computed from the previous one, the way the periodic job
// enqueuer advances a schedule.
const cronNextCount = 5

type cronSchedules struct {
	Comment            string     `json:"$comment"`
	CronCases          []cronCase `json:"cron_cases"`
	CronInvalid        []string   `json:"cron_invalid"`
	CronNamedZoneCases []cronCase `json:"cron_named_zone_cases"`
}

type cronCase struct {
	Expression string      `json:"expression"`
	From       time.Time   `json:"from"`
	Name       string      `json:"name"`
	Next       []time.Time `json:"next"`
}

type cronCaseInput struct {
	expression string
	from       time.Time
	name       string
}

// makeCronSchedules records next run times from robfig/cron's
// `ParseStandard`, the parser River documents for periodic job schedules.
func makeCronSchedules() (cronSchedules, error) {
	fixture := cronSchedules{
		Comment: generatedComment("robfig/cron/v3 ParseStandard, as used for River periodic jobs"),
	}

	eastern := time.FixedZone("", -5*60*60)
	kolkata := time.FixedZone("", 5*60*60+30*60)
	for _, testCase := range []cronCaseInput{
		{expression: "* * * * *", from: referenceNow, name: "every_minute"},
		{expression: "30 * * * *", from: referenceNow, name: "half_past_every_hour"},
		{expression: "0 9 * * 1", from: referenceNow, name: "monday_numeric_weekday"},
		{expression: "0 9 * * mon", from: referenceNow, name: "monday_named_weekday"},
		{expression: "0 0 * * 0", from: referenceNow, name: "sunday_is_zero"},
		{expression: "0 0 * * SUN", from: referenceNow, name: "weekday_names_ignore_case"},
		{expression: "*/15 9-17 * * mon-fri", from: referenceNow, name: "business_hours_steps"},
		{expression: "0 0 1 * *", from: referenceNow, name: "first_of_month"},
		{expression: "0 0 1 jan,JUL *", from: referenceNow, name: "named_months"},
		{expression: "0 0 29 2 *", from: referenceNow, name: "leap_day"},
		{expression: "0 0 30 2 *", from: referenceNow, name: "impossible_date_never_runs"},
		{expression: "0 12 1,15 * 5", from: referenceNow, name: "day_of_month_or_weekday"},
		{expression: "0 12 * * 5", from: referenceNow, name: "wildcard_day_of_month_and_weekday"},
		{expression: "0 12 ? * 5", from: referenceNow, name: "question_mark_wildcard"},
		{expression: "0 12 */2 * 5", from: referenceNow, name: "stepped_day_of_month_or_weekday"},
		{expression: "0 12 */1 * 5", from: referenceNow, name: "unit_step_keeps_wildcard"},
		{expression: "5/15 * * * *", from: referenceNow, name: "start_with_step"},
		{expression: "0-10/5 * * * *", from: referenceNow, name: "range_with_step"},
		{expression: "59 23 31 12 *", from: referenceNow, name: "year_end"},
		{expression: "@hourly", from: referenceNow, name: "descriptor_hourly"},
		{expression: "@daily", from: referenceNow, name: "descriptor_daily"},
		{expression: "@midnight", from: referenceNow, name: "descriptor_midnight"},
		{expression: "@weekly", from: referenceNow, name: "descriptor_weekly"},
		{expression: "@monthly", from: referenceNow, name: "descriptor_monthly"},
		{expression: "@yearly", from: referenceNow, name: "descriptor_yearly"},
		{expression: "@annually", from: referenceNow, name: "descriptor_annually"},
		{expression: "@every 1h30m", from: referenceNow, name: "every_compound_duration"},
		{expression: "@every 1.5h", from: referenceNow, name: "every_fractional_duration"},
		{expression: "@every 90s", from: referenceNow, name: "every_seconds"},
		{expression: "@every 500ms", from: referenceNow, name: "every_rounds_up_to_one_second"},
		{expression: "@every 1500ms", from: referenceNow, name: "every_truncates_subseconds"},
		{expression: "0 9 * * *", from: time.Date(2026, time.March, 7, 8, 0, 0, 0, eastern), name: "reference_time_offset"},
		{expression: "30 0 * * *", from: time.Date(2026, time.March, 7, 23, 45, 0, 0, kolkata), name: "reference_time_half_hour_offset"},
		{expression: "CRON_TZ=UTC 0 9 * * *", from: time.Date(2026, time.March, 7, 8, 0, 0, 0, eastern), name: "cron_tz_utc_prefix"},
		{expression: "TZ=UTC 0 9 * * *", from: time.Date(2026, time.March, 7, 8, 0, 0, 0, eastern), name: "tz_utc_prefix"},
		{expression: "  0   9 * *   1 ", from: referenceNow, name: "extra_whitespace"},
	} {
		cronCase, err := makeCronCase(testCase)
		if err != nil {
			return cronSchedules{}, err
		}
		fixture.CronCases = append(fixture.CronCases, cronCase)
	}

	// IANA zones named in `CRON_TZ=`/`TZ=` prefixes, including daylight saving
	// transitions. Kept apart from `cron_cases` because an implementation may
	// need an optional time zone database for them.
	for _, testCase := range []cronCaseInput{
		{expression: "CRON_TZ=America/New_York 0 9 * * *", from: time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC), name: "new_york_across_dst_start"},
		{expression: "CRON_TZ=America/New_York 30 2 * * *", from: time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC), name: "new_york_skipped_wall_time"},
		{expression: "CRON_TZ=America/New_York 30 1 * * *", from: time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC), name: "new_york_repeated_wall_time"},
		{expression: "CRON_TZ=America/New_York 0 * * * *", from: time.Date(2026, time.November, 1, 4, 30, 0, 0, time.UTC), name: "new_york_hourly_across_dst_end"},
		{expression: "CRON_TZ=Europe/London 0 0 * * *", from: time.Date(2026, time.October, 23, 12, 0, 0, 0, time.UTC), name: "london_across_dst_end"},
		{expression: "CRON_TZ=America/Santiago 0 0 * * *", from: time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC), name: "santiago_skipped_midnight"},
		{expression: "CRON_TZ=America/Santiago 0 12 * * *", from: time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC), name: "santiago_day_after_skipped_midnight"},
		{expression: "CRON_TZ=America/Santiago 30 23 * * *", from: time.Date(2026, time.April, 2, 12, 0, 0, 0, time.UTC), name: "santiago_repeated_hour_before_midnight"},
		{expression: "TZ=Asia/Kolkata 0 9 * * mon", from: time.Date(2026, time.January, 2, 3, 4, 5, 0, eastern), name: "kolkata_tz_prefix"},
	} {
		cronCase, err := makeCronCase(testCase)
		if err != nil {
			return cronSchedules{}, err
		}
		fixture.CronNamedZoneCases = append(fixture.CronNamedZoneCases, cronCase)
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
			return cronSchedules{}, fmt.Errorf("invalid cron expression unexpectedly parsed: %q", expression)
		}
		fixture.CronInvalid = append(fixture.CronInvalid, expression)
	}

	return fixture, nil
}

// makeCronCase records the occurrences Go computes for one cron case,
// stopping early if the schedule never runs again.
func makeCronCase(input cronCaseInput) (cronCase, error) {
	schedule, err := cron.ParseStandard(input.expression)
	if err != nil {
		return cronCase{}, fmt.Errorf("error parsing cron case %s: %w", input.name, err)
	}

	next := make([]time.Time, 0, cronNextCount)
	current := input.from
	for range cronNextCount {
		current = schedule.Next(current)
		if current.IsZero() {
			break
		}
		next = append(next, current)
	}

	return cronCase{
		Expression: input.expression,
		From:       input.from,
		Name:       input.name,
		Next:       next,
	}, nil
}

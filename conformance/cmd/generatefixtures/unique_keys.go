package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

// errorNameRejected is the error name a port must report instead of a key
// for a request River rejects, such as all-args uniqueness over arguments
// that don't encode a JSON object.
const errorNameRejected = "rejected"

type allArgs struct {
	Zeta    string `json:"zeta"`
	Alpha   string `json:"alpha"`
	Maximum int64  `json:"maximum"`
}

func (allArgs) Kind() string { return "conformance_all_args" }

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

func (collectionsArgs) Kind() string { return "conformance_all_args" }

type collectionsItem struct {
	// Deliberately non-alphabetical: nested struct wire order is significant.
	Zulu  string `json:"zulu"`
	Alpha *int   `json:"alpha"`
}

type dottedSelectedArgs struct {
	At      string `json:"@user,omitempty" river:"unique"`
	Bang    string `json:"!x,omitempty"    river:"unique"`
	Brace   string `json:"{x},omitempty"   river:"unique"`
	Bracket string `json:"[x],omitempty"   river:"unique"`
	Colon   string `json:":id,omitempty"   river:"unique"`
	//nolint:tagliatelle // literal dotted names distinguish them from nested paths
	Literal string             `json:"user.id,omitempty"   river:"unique"`
	Symbols string             `json:"a*b?c#d|e,omitempty" river:"unique"`
	User    dottedSelectedUser `json:"user"`
	Unicode string             `json:"é,omitempty"         river:"unique"`
}

func (dottedSelectedArgs) Kind() string { return "conformance_dotted_selected_args" }

type dottedSelectedUser struct {
	ID string `json:"id,omitempty" river:"unique"`
}

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

// rawAllArgs keeps duplicate members and unusual top-level names intact for
// Go's all-arguments unique-key oracle.
type rawAllArgs struct{ text string }

func (rawAllArgs) Kind() string { return "conformance_all_args" }

func (args rawAllArgs) MarshalJSON() ([]byte, error) { //nolint:unparam // json.Marshaler requires an error result.
	return []byte(args.text), nil
}

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

// selectedNullArgs selects an explicitly null field, which is retained in the
// hashed arguments, while omitted selected fields are skipped.
type selectedNullArgs struct {
	Account selectedAccount `json:"account,omitzero"`
	Label   *string         `json:"label"              river:"unique"`
	PathKey string          `json:"path/key,omitempty" river:"unique"`
}

func (selectedNullArgs) Kind() string { return "conformance_selected_args" }

type simpleArgs struct {
	ID int64 `json:"id"`
}

func (simpleArgs) Kind() string { return "conformance_simple" }

type staticClock struct{ now time.Time }

func (clock staticClock) Now() time.Time { return clock.now }
func (staticClock) NowOrNil() *time.Time { return nil }

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

type uniqueKeyCase struct {
	Args json.RawMessage `json:"args"`
	// ExpectedError is the error name a port must report instead of a key, as
	// Go does for all-args uniqueness over arguments that don't encode a JSON
	// object. ExpectedSHA256 is empty when it's set.
	ExpectedError            string           `json:"expected_error,omitempty"`
	ExpectedSHA256           string           `json:"expected_sha256,omitempty"`
	ExpectedStateMask        byte             `json:"expected_state_mask"`
	Kind                     string           `json:"kind"`
	Name                     string           `json:"name"`
	Now                      time.Time        `json:"now"`
	Options                  uniqueKeyOptions `json:"options"`
	Queue                    string           `json:"queue"`
	ScheduledAt              *time.Time       `json:"scheduled_at"`
	SelectedUniqueComponents [][]string       `json:"selected_unique_components,omitempty"`
	SelectedUniquePaths      []string         `json:"selected_unique_paths"`
}

type uniqueKeyOptions struct {
	ByArgs        bool                 `json:"by_args"`
	ByPeriodNanos int64                `json:"by_period_nanos"`
	ByQueue       bool                 `json:"by_queue"`
	ByState       []rivertype.JobState `json:"by_state,omitempty"`
	ExcludeKind   bool                 `json:"exclude_kind"`
}

type uniqueKeyReference struct {
	args                rivertype.JobArgs
	expectedError       string
	name                string
	now                 time.Time
	opts                dbunique.UniqueOpts
	queue               string
	scheduledAt         *time.Time
	selectedUniquePaths []string
	typedOnly           bool
}

type uniqueKeys struct {
	Comment string          `json:"$comment"`
	Cases   []uniqueKeyCase `json:"cases"`

	// TypedOnlyCases are goldens for typed arguments whose encoded byte order
	// a producer built on dynamic objects can't reproduce, such as a map with
	// integer-like keys, which JavaScript objects enumerate first in ascending
	// numeric order. Ports with typed serializers assert them in their own
	// tests.
	TypedOnlyCases []uniqueKeyCase `json:"typed_only_cases"`
}

// makeUniqueKeys records the unique key hash and unique states bitmask River
// computes for combinations of unique options and job arguments, including
// numeric, escaping, and ordering edge cases in the hashed arguments.
func makeUniqueKeys() (uniqueKeys, error) {
	now := referenceNow
	scheduledAt := now.Add(2*time.Hour + 17*time.Minute)
	validCustomStates := []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStateCompleted,
		rivertype.JobStatePending,
		rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}
	dottedSelectedPaths := []string{`\@user`, `\!x`, `\{x\}`, `\[x\]`, `\:id`, "user.id", `user\.id`, `a\*b\?c\#d\|e`, "é"}

	references := []uniqueKeyReference{
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
			args:  rawAllArgs{`{"":0,"a.b":1,"@x":2,":lead":3,"!bang":4,"[open":5,"{brace":6,"a\\b":7}`},
			name:  "all_args_literal_path_syntax",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args:  rawAllArgs{`{"a\"b":1,"line\n":2,"é":3,"a<b":4,"a&b":5,"a` + string(rune(0x2028)) + `b":6}`},
			name:  "all_args_escaped_key_encoding",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args:      rawAllArgs{`{"a":1,"a":2,"b":3}`},
			name:      "typed_duplicate_top_level_keys",
			now:       now,
			opts:      dbunique.UniqueOpts{ByArgs: true},
			queue:     "default",
			typedOnly: true,
		},
		{
			args:  rawAllArgs{`[]`},
			name:  "all_args_empty_array",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			args:          rawAllArgs{`[1]`},
			expectedError: errorNameRejected,
			name:          "all_args_array_rejected",
			now:           now,
			opts:          dbunique.UniqueOpts{ByArgs: true},
			queue:         "default",
		},
		{
			args:          rawAllArgs{`null`},
			expectedError: errorNameRejected,
			name:          "all_args_null_rejected",
			now:           now,
			opts:          dbunique.UniqueOpts{ByArgs: true},
			queue:         "default",
		},
		{
			args:          rawAllArgs{`"args"`},
			expectedError: errorNameRejected,
			name:          "all_args_scalar_rejected",
			now:           now,
			opts:          dbunique.UniqueOpts{ByArgs: true},
			queue:         "default",
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
			args:                dottedSelectedArgs{Literal: "literal"},
			name:                "selected_literal_dotted_name",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: dottedSelectedPaths,
		},
		{
			args:                dottedSelectedArgs{User: dottedSelectedUser{ID: "nested"}},
			name:                "selected_nested_dotted_path",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: dottedSelectedPaths,
		},
		{
			args:                dottedSelectedArgs{Unicode: "café"},
			name:                "selected_unicode_field_name",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: dottedSelectedPaths,
		},
		{
			args: dottedSelectedArgs{
				At: "at", Bang: "bang", Brace: "brace", Bracket: "bracket",
				Colon: "colon", Symbols: "symbols",
			},
			name:                "selected_punctuation_field_names",
			now:                 now,
			opts:                dbunique.UniqueOpts{ByArgs: true},
			queue:               "default",
			selectedUniquePaths: dottedSelectedPaths,
		},
		{
			args: collectionsArgs{
				Empty:   []string{},
				Labels:  map[string]string{"zulu": "last", "alpha": "first", "k10": "ten", "k2": "two"},
				Matrix:  [][]int{{3, 1}, {}, {2}},
				Objects: []collectionsItem{{Zulu: "z", Alpha: new(1)}, {Zulu: "y"}},
			},
			name:  "typed_collections_and_nulls",
			now:   now,
			opts:  dbunique.UniqueOpts{ByArgs: true},
			queue: "default",
		},
		{
			// Go writes map keys in sorted byte order, so "10" precedes
			// "2". A JavaScript object can't hold that order.
			args: collectionsArgs{
				Empty:   []string{},
				Labels:  map[string]string{"zulu": "last", "alpha": "first", "10": "ten", "2": "two"},
				Matrix:  [][]int{},
				Objects: []collectionsItem{},
			},
			name:      "typed_integer_like_map_keys",
			now:       now,
			opts:      dbunique.UniqueOpts{ByArgs: true},
			queue:     "default",
			typedOnly: true,
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

	fixture := uniqueKeys{
		Comment: generatedComment("dbunique.UniqueKey and uniquestates.UniqueStatesToBitmask"),
	}
	for _, reference := range references {
		encodedArgs, err := json.Marshal(reference.args)
		if err != nil {
			return uniqueKeys{}, fmt.Errorf("error encoding args for unique key case %s: %w", reference.name, err)
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
		switch {
		case reference.expectedError != "" && err == nil:
			return uniqueKeys{}, fmt.Errorf("unique key case %s: expected an error", reference.name)
		case reference.expectedError == "" && err != nil:
			return uniqueKeys{}, fmt.Errorf("unique key case %s: %w", reference.name, err)
		}

		uniqueKeyCase := uniqueKeyCase{
			Args:              encodedArgs,
			ExpectedError:     reference.expectedError,
			ExpectedSHA256:    hex.EncodeToString(key),
			ExpectedStateMask: uniquestates.UniqueStatesToBitmask(states),
			Kind:              reference.args.Kind(),
			Name:              reference.name,
			Now:               reference.now,
			Options: uniqueKeyOptions{
				ByArgs:        reference.opts.ByArgs,
				ByPeriodNanos: reference.opts.ByPeriod.Nanoseconds(),
				ByQueue:       reference.opts.ByQueue,
				ByState:       reference.opts.ByState,
				ExcludeKind:   reference.opts.ExcludeKind,
			},
			Queue:                    reference.queue,
			ScheduledAt:              reference.scheduledAt,
			SelectedUniqueComponents: selectedUniqueComponents(reference.selectedUniquePaths),
			SelectedUniquePaths:      reference.selectedUniquePaths,
		}
		if reference.typedOnly {
			fixture.TypedOnlyCases = append(fixture.TypedOnlyCases, uniqueKeyCase)
		} else {
			fixture.Cases = append(fixture.Cases, uniqueKeyCase)
		}
	}

	return fixture, nil
}

// selectedUniqueComponents splits gjson-escaped selected unique paths into
// decoded JSON field names, so ports needn't parse gjson's path syntax. Each
// inner slice is one path.
func selectedUniqueComponents(paths []string) [][]string {
	if len(paths) == 0 {
		return nil
	}

	components := make([][]string, 0, len(paths))
	for _, path := range paths {
		var (
			part  strings.Builder
			parts []string
		)
		for index := 0; index < len(path); index++ {
			switch path[index] {
			case '\\':
				index++
				if index < len(path) {
					part.WriteByte(path[index])
				}
			case '.':
				parts = append(parts, part.String())
				part.Reset()
			default:
				part.WriteByte(path[index])
			}
		}
		parts = append(parts, part.String())
		components = append(components, parts)
	}

	return components
}

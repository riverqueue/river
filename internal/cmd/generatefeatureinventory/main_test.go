package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiffInventory(t *testing.T) {
	t.Parallel()

	extracted := []*extractedItem{
		{Area: "config", Detail: "time.Duration", ID: "config.JobTimeout", Source: "client.go:river.Config.JobTimeout"},
		{Area: "config", Detail: "string", ID: "config.Schema", Source: "client.go:river.Config.Schema"},
	}
	fileItem := func(id, detail string) *inventoryItem {
		return &inventoryItem{
			Applicability: applicabilityProtocolVisible,
			Area:          "config",
			Detail:        detail,
			ID:            id,
			Scenarios:     []string{"scenario"},
			Source:        "client.go:river.Config." + strings.TrimPrefix(id, "config."),
		}
	}

	testCases := []struct {
		items    []*inventoryItem
		name     string
		problems []string
	}{
		{
			items: []*inventoryItem{
				fileItem("config.JobTimeout", "time.Duration"),
				fileItem("config.JobTimeout", "time.Duration"),
				fileItem("config.Schema", "string"),
			},
			name:     "DuplicateItem",
			problems: []string{"duplicate item IDs: config.JobTimeout"},
		},
		{
			items: []*inventoryItem{
				fileItem("config.JobTimeout", "int64"),
				fileItem("config.Schema", "string"),
			},
			name:     "GeneratedFieldChanged",
			problems: []string{`generated fields differ for config.JobTimeout (detail "int64" != "time.Duration")`},
		},
		{
			items:    []*inventoryItem{fileItem("config.JobTimeout", "time.Duration")},
			name:     "MissingItem",
			problems: []string{"extracted items missing from the inventory: config.Schema"},
		},
		{
			items: []*inventoryItem{
				fileItem("config.JobTimeout", "time.Duration"),
				fileItem("config.Schema", "string"),
			},
			name: "UpToDate",
		},
		{
			items: []*inventoryItem{
				fileItem("config.JobTimeout", "time.Duration"),
				fileItem("config.Removed", "bool"),
				fileItem("config.Schema", "string"),
			},
			name:     "StaleItem",
			problems: []string{"stale inventory items no longer extracted: config.Removed"},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.problems, diffInventory(&inventory{Items: tt.items}, extracted))
		})
	}
}

func TestEncodeInventory(t *testing.T) {
	t.Parallel()

	t.Run("RoundTripsCanonically", func(t *testing.T) {
		t.Parallel()

		inv := &inventory{
			Items: []*inventoryItem{{
				Applicability: applicabilityNotApplicable,
				Area:          "config",
				Detail:        "bool",
				ID:            "config.TestOnly",
				Rationale:     "Go test-suite switch <not> & shared.",
				Source:        "client.go:river.Config.TestOnly",
			}},
			ProtocolRevision: 1,
			Schema:           inventorySchemaRef,
		}

		encoded, err := encodeInventory(inv)
		require.NoError(t, err)
		require.Contains(t, string(encoded), "<not> & shared")
		require.True(t, strings.HasPrefix(string(encoded), "{\n  \"$schema\": "))

		decoded, err := decodeInventory(encoded)
		require.NoError(t, err)
		require.Equal(t, inv, decoded)
	})

	t.Run("RejectsUnknownFields", func(t *testing.T) {
		t.Parallel()

		_, err := decodeInventory([]byte(`{"items": [{"id": "config.ID", "unknown": true}]}`))
		require.ErrorContains(t, err, "unknown")
	})
}

func TestExtractAll(t *testing.T) {
	t.Parallel()

	t.Run("RepositoryContainsKnownItems", func(t *testing.T) {
		t.Parallel()

		items, err := extractAll("../../..")
		require.NoError(t, err)

		byID := make(map[string]*extractedItem, len(items))
		for i, item := range items {
			if i > 0 {
				require.Less(t, items[i-1].ID, item.ID, "items must be sorted and unique")
			}
			require.True(t, strings.HasPrefix(item.ID, item.Area+"."), "ID %s must start with its area %s", item.ID, item.Area)
			require.NotEmpty(t, item.Detail, item.ID)
			require.NotEmpty(t, item.Source, item.ID)
			byID[item.ID] = item
		}

		for _, id := range []string{
			"client.Insert",
			"config.JobTimeout",
			"driver.Executor.JobInsertFastMany",
			"event_kind.job_completed",
			"extension.riverpilot.Pilot.JobGetAvailable",
			"extension.rivertype.HookWorkBegin.WorkBegin",
			"job_list_params.After",
			"job_state.available",
			"metadata_key.cancel_attempted_at",
			"metadata_key.output",
			"metadata_key.river:log",
			"metadata_key.river:periodic_job_id",
			"metadata_key.river:rescue_count",
			"metadata_key.river:resumable_cursor",
			"metadata_key.river:resumable_step",
			"metadata_key.river:unique_nonce",
			"metadata_key.snoozes",
			"metadata_key.unique_key_conflict",
			"migration.postgres.006",
			"migration.sqlite.006",
			"notification_payload.control",
			"notification_payload.control.action.cancel",
			"notification_payload.insert",
			"notification_payload.leadership",
			"notification_payload.sql.job_cancel",
			"notification_topic.river_control",
		} {
			require.Contains(t, byID, id)
		}

		require.Equal(t, "time.Duration", byID["config.JobTimeout"].Detail)
		require.Equal(t, "client.go:river.Config.JobTimeout", byID["config.JobTimeout"].Source)
		require.Equal(t, "func(context.Context, TTx, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error)", byID["client.InsertTx"].Detail)
		require.Equal(t, "action=cancel; job_id; queue", byID["notification_payload.sql.job_cancel"].Detail)
		require.Contains(t, byID["notification_payload.control"].Detail, "job_id int64 omitempty")
	})
}

func TestMergeInventory(t *testing.T) {
	t.Parallel()

	extracted := []*extractedItem{
		{Area: "config", Detail: "string", ID: "config.Schema", Source: "client.go:river.Config.Schema"},
		{Area: "config", Detail: "time.Duration", ID: "config.JobTimeout", Source: "client.go:river.Config.JobTimeout"},
	}

	t.Run("AddsNewItemsAsUnclassified", func(t *testing.T) {
		t.Parallel()

		merged, report := mergeInventory(nil, extracted)
		require.Equal(t, []string{"config.JobTimeout", "config.Schema"}, report.Added)
		require.Empty(t, report.Removed)
		require.Equal(t, defaultProtocolRevision, merged.ProtocolRevision)
		require.Equal(t, inventorySchemaRef, merged.Schema)
		require.Len(t, merged.Items, 2)
		for _, item := range merged.Items {
			require.Equal(t, applicabilityUnclassified, item.Applicability)
		}
	})

	t.Run("DropsStaleItems", func(t *testing.T) {
		t.Parallel()

		existing := &inventory{Items: []*inventoryItem{{ID: "config.Removed", Applicability: applicabilityInternal, Rationale: "gone"}}}

		merged, report := mergeInventory(existing, extracted)
		require.Equal(t, []string{"config.Removed"}, report.Removed)
		for _, item := range merged.Items {
			require.NotEqual(t, "config.Removed", item.ID)
		}
	})

	t.Run("PreservesHumanFieldsAndRewritesGeneratedFields", func(t *testing.T) {
		t.Parallel()

		existing := &inventory{
			Items: []*inventoryItem{{
				Applicability: applicabilityProtocolVisible,
				Area:          "stale_area",
				Detail:        "stale detail",
				ID:            "config.JobTimeout",
				Rationale:     "kept",
				Scenarios:     []string{"timeout_cancellation", "a_scenario", "timeout_cancellation"},
				Source:        "stale.go:Stale",
			}},
			ProtocolRevision: 7,
		}

		merged, report := mergeInventory(existing, extracted)
		require.Equal(t, []string{"config.Schema"}, report.Added)
		require.Equal(t, 7, merged.ProtocolRevision)
		require.Equal(t, &inventoryItem{
			Applicability: applicabilityProtocolVisible,
			Area:          "config",
			Detail:        "time.Duration",
			ID:            "config.JobTimeout",
			Rationale:     "kept",
			Scenarios:     []string{"a_scenario", "timeout_cancellation"},
			Source:        "client.go:river.Config.JobTimeout",
		}, merged.Items[0])
		require.Equal(t, "config.Schema", merged.Items[1].ID)
		require.Equal(t, applicabilityUnclassified, merged.Items[1].Applicability)
	})
}

func TestParseScenarioRegistry(t *testing.T) {
	t.Parallel()

	t.Run("CollectsEveryBindingMap", func(t *testing.T) {
		t.Parallel()

		registry, err := parseScenarioRegistry(map[string][]byte{
			"conformance/harness/a_test.go": []byte(`package harness_test

const scenarioOwnerMixed = "TestMixedConformance"

type scenarioBinding struct {
	owner   string
	profile string
	tier    string
}

var scenarioRegistry = map[string]scenarioBinding{
	"timeout_cancellation": {owner: scenarioOwnerMixed, tier: "runtime"},
}
`),
			"conformance/harness/b_test.go": []byte(`package harness_test

const scenarioOwnerExtra = "TestExtraConformance"

func init() {
	extra := map[string]scenarioBinding{
		"extra_scenario": {owner: scenarioOwnerExtra, profile: "p", tier: "mixed"},
		"literal_owner":  {owner: "TestLiteral", tier: "codec"},
	}
	for id, binding := range extra {
		scenarioRegistry[id] = binding
	}
}
`),
		})
		require.NoError(t, err)
		require.Equal(t, map[string]scenarioOwner{
			"extra_scenario":       {Owner: "TestExtraConformance", Tier: "mixed"},
			"literal_owner":        {Owner: "TestLiteral", Tier: "codec"},
			"timeout_cancellation": {Owner: "TestMixedConformance", Tier: "runtime"},
		}, registry)
	})

	t.Run("RejectsDuplicateAndUnknownOwners", func(t *testing.T) {
		t.Parallel()

		_, err := parseScenarioRegistry(map[string][]byte{
			"a_test.go": []byte(`package harness_test

var a = map[string]scenarioBinding{"dup": {owner: "A"}}
var b = map[string]scenarioBinding{"dup": {owner: "B"}}
`),
		})
		require.ErrorContains(t, err, `scenario "dup" is registered more than once`)

		_, err = parseScenarioRegistry(map[string][]byte{
			"a_test.go": []byte(`package harness_test

var a = map[string]scenarioBinding{"x": {owner: missingOwner}}
`),
		})
		require.ErrorContains(t, err, "unknown constant missingOwner")
	})
}

func TestRenderMatrix(t *testing.T) {
	t.Parallel()

	registry := map[string]scenarioOwner{"timeout_cancellation": {Owner: "TestMixedConformance", Tier: "runtime"}}
	extracted := []*extractedItem{
		{Area: "migration", Detail: "x", ID: "migration.postgres.001", Source: "m"},
		{Area: "config", Detail: "time.Duration", ID: "config.JobTimeout", Source: "c"},
		{Area: "config", Detail: "*slog.Logger", ID: "config.Logger", Source: "c"},
	}
	existing := &inventory{Items: []*inventoryItem{
		{ID: "config.JobTimeout", Applicability: applicabilityProtocolVisible, Scenarios: []string{"timeout_cancellation", "planned_scenario"}},
		{ID: "config.Logger", Applicability: applicabilityAPIEquivalent, Rationale: "Uses the | native\nlogger."},
	}}

	t.Run("Deterministic", func(t *testing.T) {
		t.Parallel()

		merged, _ := mergeInventory(existing, extracted)
		reversed := make([]*extractedItem, len(extracted))
		for i, item := range extracted {
			reversed[len(extracted)-1-i] = item
		}
		mergedReversed, _ := mergeInventory(existing, reversed)

		first := renderMatrix("# Header\n", merged, registry)
		require.Equal(t, first, renderMatrix("# Header\n", merged, registry))
		require.Equal(t, first, renderMatrix("# Header\n", mergedReversed, registry))
	})

	t.Run("RendersSectionsAndOwners", func(t *testing.T) {
		t.Parallel()

		merged, _ := mergeInventory(existing, extracted)
		matrix := renderMatrix("# Header\n", merged, registry)

		require.True(t, strings.HasPrefix(matrix, "# Header\n\n## Summary\n"))
		require.Contains(t, matrix, "| [`config`](#config) | 1 | 1 | 0 | 0 | 0 | 0 | 2 |")
		require.Contains(t, matrix, "| `config.JobTimeout` | protocol_visible | `planned_scenario` (unregistered)<br>`timeout_cancellation` (TestMixedConformance) |  |")
		require.Contains(t, matrix, "| `config.Logger` | api_equivalent |  | Uses the \\| native logger. |")
		require.Contains(t, matrix, "| `migration.postgres.001` | unclassified |  |  |")
		require.Less(t, strings.Index(matrix, "## config"), strings.Index(matrix, "## migration"))
	})
}

func TestSQLMetadataKeyUses(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		body string
		keys []string
		name string
	}{
		{
			body: `SET metadata = river_job.metadata || jsonb_build_object('river:rescue_count', coalesce((metadata ->> 'x')::int, 0) + 1, 'second', 'value')`,
			keys: []string{"river:rescue_count", "second"},
			name: "JSONBBuildObject",
		},
		{
			body: `SET metadata = river_job.metadata || '{"unique_key_conflict": "scheduler_discarded"}'::jsonb`,
			keys: []string{"unique_key_conflict"},
			name: "JSONLiteral",
		},
		{
			body: `SET metadata = jsonb_patch(json(metadata), json('{"b": 1, "a": 2}'))`,
			keys: []string{"a", "b"},
			name: "JSONPatch",
		},
		{
			body: `SET metadata = jsonb_set(metadata, '{cancel_attempted_at}'::text[], @x::jsonb, true)`,
			keys: []string{"cancel_attempted_at"},
			name: "PostgresJSONBSet",
		},
		{
			body: `SET metadata = jsonb_set(metadata, '$."river:rescue_count"', 1), other = jsonb_set(metadata, '$.cancel_attempted_at', 2)`,
			keys: []string{"cancel_attempted_at", "river:rescue_count"},
			name: "SQLiteJSONBSet",
		},
		{
			body: `SELECT metadata FROM river_job`,
			name: "Unrelated",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uses, err := sqlMetadataKeyUses(&sqlQuery{body: tt.body, name: "Query", path: "q.sql"})
			require.NoError(t, err)

			var keys []string
			for _, use := range uses {
				require.Equal(t, "q.sql:Query", use.source)
				keys = append(keys, use.key)
			}
			require.ElementsMatch(t, tt.keys, keys)
		})
	}
}

func TestValidateClassifications(t *testing.T) {
	t.Parallel()

	knownScenarios := map[string]struct{}{"timeout_cancellation": {}, "declared_only": {}}
	registry := map[string]scenarioOwner{"timeout_cancellation": {Owner: "TestMixedConformance"}}

	testCases := []struct {
		item     *inventoryItem
		name     string
		problems []string
	}{
		{
			item:     &inventoryItem{ID: "x.a", Applicability: "sometimes", Rationale: "r"},
			name:     "InvalidApplicability",
			problems: []string{`invalid applicability: x.a ("sometimes")`},
		},
		{
			item:     &inventoryItem{ID: "x.a", Applicability: applicabilityNotApplicable},
			name:     "MissingRationale",
			problems: []string{"non-protocol items without a rationale: x.a"},
		},
		{
			item: &inventoryItem{ID: "x.a", Applicability: applicabilityAPIEquivalent, Rationale: "r"},
			name: "NonProtocolWithRationale",
		},
		{
			item:     &inventoryItem{ID: "x.a", Applicability: applicabilityProtocolVisible},
			name:     "ProtocolVisibleWithoutScenarios",
			problems: []string{"protocol_visible items without scenarios or a recorded gap: x.a"},
		},
		{
			item: &inventoryItem{ID: "x.a", Applicability: applicabilityProtocolVisible, Gap: "no shared scenario yet"},
			name: "ProtocolVisibleWithGap",
		},
		{
			item: &inventoryItem{ID: "x.a", Applicability: applicabilityProtocolVisible, Scenarios: []string{"timeout_cancellation"}},
			name: "ProtocolVisibleWithScenario",
		},
		{
			item:     &inventoryItem{ID: "x.a", Applicability: applicabilityUnclassified},
			name:     "Unclassified",
			problems: []string{"unclassified items (set applicability and rationale/scenarios): x.a"},
		},
		{
			item: &inventoryItem{ID: "x.a", Applicability: applicabilityProtocolVisible, Scenarios: []string{"planned"}},
			name: "UnknownScenario",
			problems: []string{
				`scenario "planned" is not declared in conformance/scenarios/*.json (referenced by x.a)`,
				`scenario "planned" has no owner in the harness scenario registry (referenced by x.a)`,
			},
		},
		{
			item:     &inventoryItem{ID: "x.a", Applicability: applicabilityProtocolVisible, Scenarios: []string{"declared_only"}},
			name:     "UnregisteredScenario",
			problems: []string{`scenario "declared_only" has no owner in the harness scenario registry (referenced by x.a)`},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			problems := validateClassifications(&inventory{Items: []*inventoryItem{tt.item}}, knownScenarios, registry)
			require.Equal(t, tt.problems, problems)
		})
	}
}

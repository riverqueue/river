package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// applicability classifies how an inventory item relates to cross-language
// compatibility.
type applicability string

const (
	applicabilityAPIEquivalent   applicability = "api_equivalent"
	applicabilityDriverSpecific  applicability = "driver_specific"
	applicabilityInternal        applicability = "internal"
	applicabilityNotApplicable   applicability = "not_applicable"
	applicabilityProtocolVisible applicability = "protocol_visible"
	applicabilityUnclassified    applicability = "unclassified"
)

// applicabilityOrder is the display order of applicabilities in the matrix
// summary. It also serves as the set of valid values.
func applicabilityOrder() []applicability {
	return []applicability{
		applicabilityProtocolVisible,
		applicabilityAPIEquivalent,
		applicabilityDriverSpecific,
		applicabilityInternal,
		applicabilityNotApplicable,
		applicabilityUnclassified,
	}
}

// areaInfo describes one inventory area and its matrix section.
type areaInfo struct {
	description string
	name        string
}

// areaInfos returns every known area in matrix order.
func areaInfos() []areaInfo {
	return []areaInfo{
		{name: "config", description: "Exported fields of `river.Config`."},
		{name: "insert_opts", description: "Exported fields of `river.InsertOpts`."},
		{name: "unique_opts", description: "Exported fields of `river.UniqueOpts`."},
		{name: "queue_config", description: "Exported fields of `river.QueueConfig`."},
		{name: "periodic_job_opts", description: "Exported fields of `river.PeriodicJobOpts`."},
		{name: "client", description: "Exported methods of `*river.Client[TTx]`."},
		{name: "job_list_params", description: "Exported builder methods of `*river.JobListParams`."},
		{name: "job_delete_many_params", description: "Exported builder methods of `*river.JobDeleteManyParams`."},
		{name: "queue_list_params", description: "Exported builder methods of `*river.QueueListParams`."},
		{name: "job_state", description: "Values of `rivertype.JobStates()`."},
		{name: "event_kind", description: "Exported `river.EventKind*` constants."},
		{name: "metadata_key", description: "Reserved job metadata keys written or read by River, from Go constants, Go metadata helpers, and driver SQL."},
		{name: "notification_topic", description: "Notification topics declared by `internal/notifier`."},
		{name: "notification_payload", description: "Notification payload shapes and action values, from Go payload structs and `pg_notify` SQL."},
		{name: "driver", description: "Methods of the exported `riverdriver` interfaces."},
		{name: "extension", description: "Methods of the extension interfaces in `rivershared/riverpilot` and the hook, middleware, and plugin interfaces in `rivertype`."},
		{name: "migration", description: "Main-line migrations for PostgreSQL and SQLite."},
	}
}

// extractedItem is an item as derived from Go and SQL sources. It carries only
// the generated fields of an inventory item.
type extractedItem struct {
	Area   string
	Detail string
	ID     string
	Source string
}

// inventory is the checked-in feature inventory document.
type inventory struct {
	Schema           string           `json:"$schema"`
	Items            []*inventoryItem `json:"items"`
	ProtocolRevision int              `json:"protocol_revision"`
}

// inventoryItem is one entry in the feature inventory. Area, Detail, ID, and
// Source are generated; Applicability, Gap, Rationale, and Scenarios are
// maintained by people and preserved across regeneration.
type inventoryItem struct {
	Applicability applicability `json:"applicability"`
	Area          string        `json:"area"`
	Detail        string        `json:"detail"`
	// Gap explains why a protocol-visible item has no shared scenario yet.
	// It keeps an uncovered item visible in the matrix instead of hiding it
	// behind a weaker classification.
	Gap       string   `json:"gap,omitempty"`
	ID        string   `json:"id"`
	Rationale string   `json:"rationale,omitempty"`
	Scenarios []string `json:"scenarios,omitempty"`
	Source    string   `json:"source"`
}

// mergeReport describes the effect of merging extracted items into an
// existing inventory.
type mergeReport struct {
	Added   []string
	Removed []string
}

// scenarioOwner is the registry binding for one executable scenario.
type scenarioOwner struct {
	Owner string
	Tier  string
}

const (
	inventorySchemaRef      = "schema/feature-inventory.schema.json"
	defaultProtocolRevision = 1
)

// decodeInventory parses an inventory document.
func decodeInventory(data []byte) (*inventory, error) {
	var inv inventory
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inv); err != nil {
		return nil, fmt.Errorf("decode inventory: %w", err)
	}
	return &inv, nil
}

// encodeInventory renders an inventory in its canonical form.
func encodeInventory(inv *inventory) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(inv); err != nil {
		return nil, fmt.Errorf("encode inventory: %w", err)
	}
	return buf.Bytes(), nil
}

// mergeInventory combines freshly extracted items with an existing inventory.
// Generated fields always come from extraction, human-maintained fields are kept
// for IDs that still exist, new IDs are added as unclassified, and IDs that are
// no longer extracted are dropped. The result is sorted by ID.
func mergeInventory(existing *inventory, extracted []*extractedItem) (*inventory, *mergeReport) {
	existingByID := make(map[string]*inventoryItem)
	protocolRevision := defaultProtocolRevision
	if existing != nil {
		for _, item := range existing.Items {
			if _, ok := existingByID[item.ID]; !ok {
				existingByID[item.ID] = item
			}
		}
		if existing.ProtocolRevision > 0 {
			protocolRevision = existing.ProtocolRevision
		}
	}

	report := &mergeReport{}
	extractedIDs := make(map[string]struct{}, len(extracted))
	merged := &inventory{
		Items:            make([]*inventoryItem, 0, len(extracted)),
		ProtocolRevision: protocolRevision,
		Schema:           inventorySchemaRef,
	}
	for _, extractedItem := range extracted {
		extractedIDs[extractedItem.ID] = struct{}{}
		item := &inventoryItem{
			Applicability: applicabilityUnclassified,
			Area:          extractedItem.Area,
			Detail:        extractedItem.Detail,
			ID:            extractedItem.ID,
			Source:        extractedItem.Source,
		}
		if previous, ok := existingByID[extractedItem.ID]; ok {
			item.Applicability = previous.Applicability
			item.Gap = previous.Gap
			item.Rationale = previous.Rationale
			item.Scenarios = normalizeScenarios(previous.Scenarios)
		} else {
			report.Added = append(report.Added, extractedItem.ID)
		}
		merged.Items = append(merged.Items, item)
	}
	for id := range existingByID {
		if _, ok := extractedIDs[id]; !ok {
			report.Removed = append(report.Removed, id)
		}
	}

	sort.Slice(merged.Items, func(i, j int) bool { return merged.Items[i].ID < merged.Items[j].ID })
	sort.Strings(report.Added)
	sort.Strings(report.Removed)
	return merged, report
}

// normalizeScenarios sorts and deduplicates scenario IDs.
func normalizeScenarios(scenarios []string) []string {
	if len(scenarios) == 0 {
		return nil
	}
	normalized := slices.Clone(scenarios)
	sort.Strings(normalized)
	return slices.Compact(normalized)
}

// diffInventory compares a checked-in inventory against extracted items and
// returns a problem for every missing, stale, duplicate, or out-of-date item.
func diffInventory(existing *inventory, extracted []*extractedItem) []string {
	var (
		duplicates []string
		fileByID   = make(map[string]*inventoryItem, len(existing.Items))
		problems   []string
	)
	for _, item := range existing.Items {
		if _, ok := fileByID[item.ID]; ok {
			duplicates = append(duplicates, item.ID)
			continue
		}
		fileByID[item.ID] = item
	}
	if len(duplicates) > 0 {
		problems = append(problems, "duplicate item IDs: "+strings.Join(sortedUnique(duplicates), ", "))
	}

	var (
		changed []string
		missing []string
		seen    = make(map[string]struct{}, len(extracted))
		stale   []string
	)
	for _, extractedItem := range extracted {
		seen[extractedItem.ID] = struct{}{}
		item, ok := fileByID[extractedItem.ID]
		if !ok {
			missing = append(missing, extractedItem.ID)
			continue
		}
		var fields []string
		if item.Area != extractedItem.Area {
			fields = append(fields, fmt.Sprintf("area %q != %q", item.Area, extractedItem.Area))
		}
		if item.Detail != extractedItem.Detail {
			fields = append(fields, fmt.Sprintf("detail %q != %q", item.Detail, extractedItem.Detail))
		}
		if item.Source != extractedItem.Source {
			fields = append(fields, fmt.Sprintf("source %q != %q", item.Source, extractedItem.Source))
		}
		if len(fields) > 0 {
			changed = append(changed, extractedItem.ID+" ("+strings.Join(fields, "; ")+")")
		}
	}
	for id := range fileByID {
		if _, ok := seen[id]; !ok {
			stale = append(stale, id)
		}
	}

	sort.Strings(changed)
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		problems = append(problems, "extracted items missing from the inventory: "+strings.Join(missing, ", "))
	}
	if len(stale) > 0 {
		problems = append(problems, "stale inventory items no longer extracted: "+strings.Join(stale, ", "))
	}
	for _, change := range changed {
		problems = append(problems, "generated fields differ for "+change)
	}
	return problems
}

// validateClassifications checks the human-maintained fields of every item.
// knownScenarios is the set of scenario IDs declared by the scenario catalogs
// and registry maps each executable scenario ID to its owning test.
func validateClassifications(inv *inventory, knownScenarios map[string]struct{}, registry map[string]scenarioOwner) []string {
	validApplicability := make(map[applicability]struct{})
	for _, value := range applicabilityOrder() {
		validApplicability[value] = struct{}{}
	}

	var (
		invalid          []string
		missingRationale []string
		missingScenarios []string
		unclassified     []string
		unknownScenarios = make(map[string][]string)
		unregistered     = make(map[string][]string)
	)
	for _, item := range inv.Items {
		switch _, ok := validApplicability[item.Applicability]; {
		case !ok:
			invalid = append(invalid, fmt.Sprintf("%s (%q)", item.ID, item.Applicability))
		case item.Applicability == applicabilityUnclassified:
			unclassified = append(unclassified, item.ID)
		case item.Applicability == applicabilityProtocolVisible:
			if len(item.Scenarios) == 0 && strings.TrimSpace(item.Gap) == "" {
				missingScenarios = append(missingScenarios, item.ID)
			}
		default:
			if strings.TrimSpace(item.Rationale) == "" {
				missingRationale = append(missingRationale, item.ID)
			}
		}
		for _, scenario := range item.Scenarios {
			if _, ok := knownScenarios[scenario]; !ok {
				unknownScenarios[scenario] = append(unknownScenarios[scenario], item.ID)
			}
			if _, ok := registry[scenario]; !ok {
				unregistered[scenario] = append(unregistered[scenario], item.ID)
			}
		}
	}

	var problems []string
	if len(invalid) > 0 {
		problems = append(problems, "invalid applicability: "+strings.Join(invalid, ", "))
	}
	if len(unclassified) > 0 {
		problems = append(problems, "unclassified items (set applicability and rationale/scenarios): "+strings.Join(unclassified, ", "))
	}
	if len(missingScenarios) > 0 {
		problems = append(problems, "protocol_visible items without scenarios or a recorded gap: "+strings.Join(missingScenarios, ", "))
	}
	if len(missingRationale) > 0 {
		problems = append(problems, "non-protocol items without a rationale: "+strings.Join(missingRationale, ", "))
	}
	for _, scenario := range sortedKeys(unknownScenarios) {
		problems = append(problems, fmt.Sprintf("scenario %q is not declared in conformance/scenarios/*.json (referenced by %s)", scenario, strings.Join(unknownScenarios[scenario], ", ")))
	}
	for _, scenario := range sortedKeys(unregistered) {
		problems = append(problems, fmt.Sprintf("scenario %q has no owner in the harness scenario registry (referenced by %s)", scenario, strings.Join(unregistered[scenario], ", ")))
	}
	return problems
}

// renderMatrix renders the feature matrix from a fixed header and the
// inventory. Output is deterministic for a given input.
func renderMatrix(header string, inv *inventory, registry map[string]scenarioOwner) string {
	var sb strings.Builder
	sb.WriteString(strings.TrimRight(header, "\n"))
	sb.WriteString("\n")

	itemsByArea := make(map[string][]*inventoryItem)
	for _, item := range inv.Items {
		itemsByArea[item.Area] = append(itemsByArea[item.Area], item)
	}
	areas := areaInfos()
	knownAreas := make(map[string]struct{}, len(areas))
	for _, area := range areas {
		knownAreas[area.name] = struct{}{}
	}
	for _, name := range sortedKeys(itemsByArea) {
		if _, ok := knownAreas[name]; !ok {
			areas = append(areas, areaInfo{name: name})
		}
	}

	sb.WriteString("\n## Summary\n\n")
	sb.WriteString("| Area |")
	for _, value := range applicabilityOrder() {
		sb.WriteString(" `" + string(value) + "` |")
	}
	sb.WriteString(" Total |\n|---|")
	for range applicabilityOrder() {
		sb.WriteString("---:|")
	}
	sb.WriteString("---:|\n")
	for _, area := range areas {
		items := itemsByArea[area.name]
		if len(items) == 0 {
			continue
		}
		counts := make(map[applicability]int)
		for _, item := range items {
			counts[item.Applicability]++
		}
		sb.WriteString("| [`" + area.name + "`](#" + area.name + ") |")
		for _, value := range applicabilityOrder() {
			fmt.Fprintf(&sb, " %d |", counts[value])
		}
		fmt.Fprintf(&sb, " %d |\n", len(items))
	}

	for _, area := range areas {
		items := itemsByArea[area.name]
		if len(items) == 0 {
			continue
		}
		sb.WriteString("\n## " + area.name + "\n\n")
		if area.description != "" {
			sb.WriteString(area.description + "\n\n")
		}
		sb.WriteString("| Item | Applicability | Scenarios (owner test) | Notes |\n|---|---|---|---|\n")
		for _, item := range items {
			scenarios := make([]string, 0, len(item.Scenarios))
			for _, scenario := range item.Scenarios {
				owner := "unregistered"
				if binding, ok := registry[scenario]; ok {
					owner = binding.Owner
				}
				scenarios = append(scenarios, "`"+scenario+"` ("+owner+")")
			}
			if item.Gap != "" {
				scenarios = append(scenarios, "**Gap:** "+markdownCell(item.Gap))
			}
			fmt.Fprintf(&sb, "| `%s` | %s | %s | %s |\n",
				item.ID,
				item.Applicability,
				strings.Join(scenarios, "<br>"),
				markdownCell(item.Rationale),
			)
		}
	}
	return sb.String()
}

// markdownCell makes text safe for a single Markdown table cell.
func markdownCell(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	return strings.ReplaceAll(text, "|", `\|`)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedUnique(values []string) []string {
	sorted := slices.Clone(values)
	sort.Strings(sorted)
	return slices.Compact(sorted)
}

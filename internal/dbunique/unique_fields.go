package dbunique

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/rivertype"
)

var (
	// uniqueFieldsCache caches the unique fields for each JobArgs type. These are
	// global to ensure that each struct type's tags are only extracted once.
	uniqueFieldsCache = make(map[reflect.Type][]string) //nolint:gochecknoglobals
	cacheMutex        sync.RWMutex                      //nolint:gochecknoglobals
)

// extractUniqueValues extracts the raw JSON values of the specified keys from the JSON-encoded args.
func extractUniqueValues(encodedArgs []byte, uniqueKeys []string) []string {
	// Use GetManyBytes to retrieve multiple values at once
	results := gjson.GetManyBytes(encodedArgs, uniqueKeys...)

	uniqueValues := make([]string, len(results))
	for i, res := range results {
		if res.Exists() {
			uniqueValues[i] = res.Raw // Use Raw to get the JSON-encoded value
		} else {
			// Handle missing keys as "undefined" (they'll be skipped when building
			// the unique key). We don't want to use "null" here because the JSON may
			// actually contain "null" as a value.
			uniqueValues[i] = "undefined"
		}
	}

	return uniqueValues
}

// getSortedUniqueFields uses reflection to retrieve the JSON keys of fields
// marked with `river:"unique"` among potentially other comma-separated values.
// The return values are the JSON keys using the same logic as the `json` struct tag.
func getSortedUniqueFields(args rivertype.JobArgs) ([]string, error) {
	typ := reflect.TypeOf(args)

	// Handle pointer to struct
	if typ != nil && typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Ensure we're dealing with a struct
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %T", args)
	}

	var uniqueFields []string

	// Iterate over all fields
	for i := range typ.NumField() {
		field := typ.Field(i)

		// Check for `river:"unique"` tag, possibly among other comma-separated values
		if riverTag, ok := field.Tag.Lookup("river"); ok {
			// Split riverTag by comma
			tags := strings.Split(riverTag, ",")
			for _, tag := range tags {
				if strings.TrimSpace(tag) == "unique" {
					// Get the corresponding JSON key
					jsonTag := field.Tag.Get("json")
					if jsonTag == "" {
						// If no JSON tag, use the field name as-is
						uniqueFields = append(uniqueFields, field.Name)
					} else {
						// Handle cases like `json:"recipient,omitempty"`
						jsonKey := parseJSONTag(jsonTag)
						uniqueFields = append(uniqueFields, jsonKey)
					}
					break // No need to check other tags once "unique" is found
				}
			}
		}
	}

	// Sort the uniqueFields alphabetically for consistent ordering
	sort.Strings(uniqueFields)

	return uniqueFields, nil
}

// getSortedUniqueFieldsCached retrieves unique fields with caching to avoid
// extracting fields from the same struct type repeatedly.
func getSortedUniqueFieldsCached(args rivertype.JobArgs) ([]string, error) {
	typ := reflect.TypeOf(args)

	// Check cache first
	cacheMutex.RLock()
	if fields, ok := uniqueFieldsCache[typ]; ok {
		cacheMutex.RUnlock()
		return fields, nil
	}
	cacheMutex.RUnlock()

	// Not in cache; retrieve using reflection
	fields, err := getSortedUniqueFields(args)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cacheMutex.Lock()
	uniqueFieldsCache[typ] = fields
	cacheMutex.Unlock()

	return fields, nil
}

// parseJSONTag extracts the JSON key from the struct tag.
// It handles tags with options, e.g., `json:"recipient,omitempty"`.
func parseJSONTag(tag string) string {
	// Tags can be like "recipient,omitempty", so split by comma
	if commaIdx := strings.Index(tag, ","); commaIdx != -1 {
		return tag[:commaIdx]
	}
	return tag
}

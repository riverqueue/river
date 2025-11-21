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
func getSortedUniqueFields(typ reflect.Type, path []string) ([]string, error) {
	// Handle pointer to struct
	if typ != nil && typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Ensure we're dealing with a struct
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %T", typ.Name())
	}

	var uniqueFields []string

	// Iterate over all fields
	for i := range typ.NumField() {
		field := typ.Field(i)

		if !field.IsExported() {
			continue
		}

		var uniqueName string
		{
			// Get the corresponding JSON key
			jsonTag := field.Tag.Get("json")

			if jsonTag == "" {
				// If no JSON tag, use the field name as-is
				uniqueName = field.Name
			} else {
				// Handle cases like `json:"recipient,omitempty"`
				uniqueName = parseJSONTag(jsonTag)
			}
		}

		// Check for `river:"unique"` tag, possibly among other comma-separated values
		var hasUniqueTag bool
		if riverTag, ok := field.Tag.Lookup("river"); ok {
			tags := strings.Split(riverTag, ",")
			for _, tag := range tags {
				if strings.TrimSpace(tag) == "unique" {
					hasUniqueTag = true
				}
			}
		}

		if typeStructOrPointerToStruct(field.Type) {
			// Append the JSON to the path (all path segments sent down
			// recursively) unless we're looking at an anonymous struct, whose
			// fields will be let at the top level.
			fullPath := path
			if !field.Anonymous {
				fullPath = append(path, uniqueName) //nolint:gocritic
			}

			uniqueSubFields, err := getSortedUniqueFields(field.Type, fullPath)
			if err != nil {
				return nil, err
			}

			if len(uniqueSubFields) > 0 {
				uniqueFields = append(uniqueFields, uniqueSubFields...)
			} else if hasUniqueTag {
				// If a struct field is marked `river:"unique"`, use its entire
				// JSON serialization as a unique value. This may not be the
				// greatest idea practically, but keeping it in place for
				// backwards compatibility.
				uniqueFields = append(uniqueFields, strings.Join(append(path, uniqueName), "."))
			}

			continue
		}

		if hasUniqueTag {
			uniqueFields = append(uniqueFields, strings.Join(append(path, uniqueName), "."))
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
	fields, err := getSortedUniqueFields(reflect.TypeOf(args), nil)
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

func typeStructOrPointerToStruct(typ reflect.Type) bool {
	if typ.Kind() == reflect.Struct {
		return true
	}

	if typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct {
		return true
	}

	return false
}

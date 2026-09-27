package structtag

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/rivertype"
)

// ExtractValues extracts the raw JSON values of the specified keys from the
// JSON-encoded args. Keys are gjson paths like those returned by
// SortedFieldsWithTag.
func ExtractValues(encodedArgs []byte, uniqueKeys []string) []string {
	// Use GetManyBytes to retrieve multiple values at once
	results := gjson.GetManyBytes(encodedArgs, uniqueKeys...)

	uniqueValues := make([]string, len(results))
	for i, res := range results {
		if res.Exists() {
			uniqueValues[i] = res.Raw // Use Raw to get the JSON-encoded value
		} else {
			// Handle missing keys as "undefined" (they'll be skipped when
			// building the key). We don't want to use "null" here because the
			// JSON may actually contain "null" as a value.
			uniqueValues[i] = "undefined"
		}
	}

	return uniqueValues
}

// fieldPath is the location of a tagged field within a struct's JSON encoding.
type fieldPath struct {
	// path is a gjson/sjson path to the field, with each component escaped so
	// that JSON keys containing path syntax are treated literally.
	path string

	// sortKey is the unescaped components joined with `.`. Paths are sorted by
	// it so that fields keep the order they had before components were
	// escaped, which keeps unique keys stable for ordinary field names.
	sortKey string
}

func newFieldPath(components []string) fieldPath {
	escaped := make([]string, len(components))
	for i, component := range components {
		escaped[i] = escapePathComponent(component)
	}

	return fieldPath{
		path:    strings.Join(escaped, "."),
		sortKey: strings.Join(components, "."),
	}
}

type uniqueFieldCacheKey struct {
	typ      reflect.Type
	tagValue string
}

var (
	// uniqueFieldsCache caches the unique fields for each JobArgs type. These are
	// global to ensure that each struct type's tags are only extracted once.
	uniqueFieldsCache = make(map[uniqueFieldCacheKey][]string) //nolint:gochecknoglobals
	cacheMutex        sync.RWMutex                             //nolint:gochecknoglobals
)

// SortedFieldsWithTag retrieves unique fields with caching to avoid
// extracting fields from the same struct type repeatedly.
//
// Fields are returned as gjson/sjson paths suitable for use with ExtractValues
// and sjson. Each path component is escaped so a JSON key containing path
// syntax like `.`, `@`, or `*` addresses that key literally. Paths are sorted
// by their unescaped, dot-joined JSON keys.
func SortedFieldsWithTag(args rivertype.JobArgs, tagValue string) ([]string, error) {
	var (
		typ      = reflect.TypeOf(args)
		cacheKey = uniqueFieldCacheKey{typ: typ, tagValue: tagValue}
	)

	// Check cache first
	cacheMutex.RLock()
	if fields, ok := uniqueFieldsCache[cacheKey]; ok {
		cacheMutex.RUnlock()
		return fields, nil
	}
	cacheMutex.RUnlock()

	// Not in cache; retrieve using reflection
	fields, err := sortedFieldsWithTagUncached(typ, tagValue)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cacheMutex.Lock()
	uniqueFieldsCache[cacheKey] = fields
	cacheMutex.Unlock()

	return fields, nil
}

// sortedFieldsWithTagUncached uses reflection to retrieve the escaped JSON
// paths of fields marked with `river:"<tagValue>"`, sorted by their unescaped
// JSON keys.
func sortedFieldsWithTagUncached(typ reflect.Type, tagValue string) ([]string, error) {
	fieldPaths, err := fieldPathsWithTag(typ, tagValue, nil, make(map[reflect.Type]struct{}))
	if err != nil {
		return nil, err
	}

	// Sort by unescaped keys for consistent ordering that matches the order
	// used before path components were escaped. Break ties (possible when a
	// key containing `.` collides with a nested path) by escaped path.
	slices.SortFunc(fieldPaths, func(a, b fieldPath) int {
		return cmp.Or(strings.Compare(a.sortKey, b.sortKey), strings.Compare(a.path, b.path))
	})

	var fields []string
	for _, fieldPath := range fieldPaths {
		fields = append(fields, fieldPath.path)
	}
	return fields, nil
}

// fieldPathsWithTag uses reflection to retrieve the JSON paths of fields
// marked with `river:"<tagValue>"` among potentially other comma-separated
// values. Path components are the JSON keys using the same logic as the `json`
// struct tag. Results are unsorted.
//
// typesSeen should be a map passed through to make sure that recursive types
// don't cause a stack overflow.
func fieldPathsWithTag(typ reflect.Type, tagValue string, path []string, typesSeen map[reflect.Type]struct{}) ([]fieldPath, error) {
	// Handle pointer to struct
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	// Ensure we're dealing with a struct
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %T", typ.Name())
	}

	// Stop when encountering a recursive type. This has the effect of the
	// entire subfield's value being extracted by gjson, but this is about as
	// right of a way to handle it as any other I can think of.
	if _, ok := typesSeen[typ]; ok {
		return nil, nil
	}
	typesSeen[typ] = struct{}{}

	var uniqueFields []fieldPath

	// Iterate over all fields
	for field := range typ.Fields() {
		if !field.IsExported() {
			continue
		}

		// Get the corresponding JSON key
		uniqueName := parseJSONTag(field.Name, field.Tag.Get("json"))

		// Check for `river:"unique"` tag, possibly among other comma-separated values
		var hasUniqueTag bool
		if riverTag, ok := field.Tag.Lookup("river"); ok {
			tags := strings.SplitSeq(riverTag, ",")
			for tag := range tags {
				if strings.TrimSpace(tag) == tagValue {
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

			uniqueSubFields, err := fieldPathsWithTag(field.Type, tagValue, fullPath, typesSeen)
			if err != nil {
				return nil, err
			}

			if len(uniqueSubFields) > 0 {
				uniqueFields = append(uniqueFields, uniqueSubFields...)
			} else if hasUniqueTag {
				// If a struct field is marked `river:"<tagValue>"`, use its entire
				// JSON serialization as a unique value. This may not be the
				// greatest idea practically, but keeping it in place for
				// backwards compatibility.
				uniqueFields = append(uniqueFields, newFieldPath(append(path, uniqueName)))
			}

			continue
		}

		if hasUniqueTag {
			uniqueFields = append(uniqueFields, newFieldPath(append(path, uniqueName)))
		}
	}

	return uniqueFields, nil
}

// escapePathComponent escapes a JSON object key for use as a single component
// of a gjson or sjson path so that it addresses the key literally.
//
// gjson.Escape handles path separators, wildcards, modifiers, and queries.
// Also escape a leading colon so sjson doesn't strip it as an object-key
// directive. Both libraries unescape a backslash before any byte.
func escapePathComponent(key string) string {
	escaped := gjson.Escape(key)
	if strings.HasPrefix(escaped, ":") {
		return `\` + escaped
	}
	return escaped
}

// parseJSONTag returns the JSON key for a field with the given name and `json`
// struct tag. It handles tags with options, e.g., `json:"recipient,omitempty"`,
// and like encoding/json falls back to the field name when the tag has no name
// (e.g. `json:",omitempty"`).
//
// Preserve the historical handling of invalid JSON tag names; only the empty
// name falls back to the field name here.
func parseJSONTag(fieldName, tag string) string {
	// Tags can be like "recipient,omitempty", so split by comma
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return fieldName
	}
	return name
}

func typeStructOrPointerToStruct(typ reflect.Type) bool {
	if typ.Kind() == reflect.Struct {
		return true
	}

	if typ.Kind() == reflect.Pointer && typ.Elem().Kind() == reflect.Struct {
		return true
	}

	return false
}

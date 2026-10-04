package harness_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// schemaValidator validates JSON documents against the subset of JSON Schema
// 2020-12 that the conformance artifacts use. It deliberately rejects any
// keyword it does not implement, so a schema can never rely on a constraint
// that is silently ignored. References may point inside a document
// ("#/$defs/name") or at another schema file relative to the referencing
// document ("../schema/normalized-job.schema.json").
type schemaValidator struct {
	documents map[string]any
	mu        sync.Mutex
	patterns  map[string]*regexp.Regexp
}

func newSchemaValidator() *schemaValidator {
	return &schemaValidator{documents: make(map[string]any), patterns: make(map[string]*regexp.Regexp)}
}

// schemaAnnotations are keywords that carry no validation.
var schemaAnnotations = []string{"$defs", "$schema", "description", "title"} //nolint:gochecknoglobals // fixed keyword set

// decodeJSONWithNumbers decodes JSON keeping numbers as json.Number so large
// integers keep their exact value.
func decodeJSONWithNumbers(contents []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("trailing data after JSON value")
	}
	return value, nil
}

func (validator *schemaValidator) document(path string) (any, error) {
	validator.mu.Lock()
	defer validator.mu.Unlock()

	if document, ok := validator.documents[path]; ok {
		return document, nil
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	document, err := decodeJSONWithNumbers(contents)
	if err != nil {
		return nil, fmt.Errorf("decode schema %s: %w", path, err)
	}
	validator.documents[path] = document
	return document, nil
}

// validateFile validates value against the schema document at path, or a
// fragment of it such as "#/$defs/job".
func (validator *schemaValidator) validateFile(value any, path, fragment string) error {
	document, err := validator.document(path)
	if err != nil {
		return err
	}
	schema, err := resolvePointer(document, fragment)
	if err != nil {
		return fmt.Errorf("%s%s: %w", path, fragment, err)
	}
	return validator.validate(value, schema, path, "$")
}

func resolvePointer(document any, fragment string) (any, error) {
	fragment = strings.TrimPrefix(fragment, "#")
	current := document
	if fragment == "" {
		return current, nil
	}
	for token := range strings.SplitSeq(strings.TrimPrefix(fragment, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch container := current.(type) {
		case map[string]any:
			var ok bool
			if current, ok = container[token]; !ok {
				return nil, fmt.Errorf("pointer segment %q not found", token)
			}
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(container) {
				return nil, fmt.Errorf("pointer segment %q is not an index of a %d-item array", token, len(container))
			}
			current = container[index]
		default:
			return nil, fmt.Errorf("pointer segment %q does not address an object or array", token)
		}
	}
	return current, nil
}

//nolint:cyclop,gocognit,maintidx // One switch per supported keyword keeps the subset auditable.
func (validator *schemaValidator) validate(value, schemaValue any, documentPath, location string) error {
	switch schema := schemaValue.(type) {
	case bool:
		if !schema {
			return fmt.Errorf("%s: no value is allowed here", location)
		}
		return nil
	case map[string]any:
		keywords := make([]string, 0, len(schema))
		for keyword := range schema {
			keywords = append(keywords, keyword)
		}
		slices.Sort(keywords)
		for _, keyword := range keywords {
			argument := schema[keyword]
			var err error
			switch keyword {
			case "$ref":
				err = validator.validateRef(value, argument, documentPath, location)
			case "additionalProperties", "properties", "propertyNames":
				err = validator.validateObjectKeyword(value, schema, keyword, documentPath, location)
			case "allOf":
				alternatives, _ := argument.([]any)
				for _, alternative := range alternatives {
					if err = validator.validate(value, alternative, documentPath, location); err != nil {
						break
					}
				}
			case "if":
				// A value matching "if" must match "then"; otherwise "else".
				branch := "else"
				if validator.validate(value, argument, documentPath, location) == nil {
					branch = "then"
				}
				if branchSchema, ok := schema[branch]; ok {
					err = validator.validate(value, branchSchema, documentPath, location)
				}
			case "then", "else":
				// Evaluated with "if".
			case "anyOf", "oneOf":
				err = validator.validateAlternatives(value, keyword, argument, documentPath, location)
			case "const":
				if !jsonEqual(value, argument) {
					err = fmt.Errorf("%s: must equal %v", location, argument)
				}
			case "enum":
				options, _ := argument.([]any)
				if !slices.ContainsFunc(options, func(option any) bool { return jsonEqual(value, option) }) {
					err = fmt.Errorf("%s: %v is not one of %v", location, value, options)
				}
			case "exclusiveMinimum", "maximum", "minimum":
				err = validateBound(value, keyword, argument, location)
			case "format":
				err = validateFormat(value, argument, location)
			case "items":
				if array, ok := value.([]any); ok {
					for index, item := range array {
						if err = validator.validate(item, argument, documentPath, fmt.Sprintf("%s[%d]", location, index)); err != nil {
							break
						}
					}
				}
			case "minItems":
				if array, ok := value.([]any); ok && int64(len(array)) < schemaInteger(argument) {
					err = fmt.Errorf("%s: needs at least %v items", location, argument)
				}
			case "minLength":
				if text, ok := value.(string); ok && int64(len([]rune(text))) < schemaInteger(argument) {
					err = fmt.Errorf("%s: needs at least %v characters", location, argument)
				}
			case "minProperties":
				if object, ok := value.(map[string]any); ok && int64(len(object)) < schemaInteger(argument) {
					err = fmt.Errorf("%s: needs at least %v properties", location, argument)
				}
			case "pattern":
				err = validator.validatePattern(value, argument, location)
			case "required":
				if object, ok := value.(map[string]any); ok {
					names, _ := argument.([]any)
					for _, name := range names {
						if _, present := object[fmt.Sprint(name)]; !present {
							err = fmt.Errorf("%s: missing required property %q", location, name)
							break
						}
					}
				}
			case "type":
				err = validateType(value, argument, location)
			case "uniqueItems":
				if array, ok := value.([]any); ok && argument == true {
					for index := range array {
						for other := range index {
							if jsonEqual(array[index], array[other]) {
								err = fmt.Errorf("%s: items %d and %d are equal", location, other, index)
							}
						}
					}
				}
			default:
				if !slices.Contains(schemaAnnotations, keyword) {
					err = fmt.Errorf("%s: schema keyword %q is not supported by the harness validator", location, keyword)
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s: schema must be an object or boolean", location)
	}
}

func (validator *schemaValidator) validateRef(value, argument any, documentPath, location string) error {
	reference, ok := argument.(string)
	if !ok {
		return fmt.Errorf("%s: $ref must be a string", location)
	}
	target, fragment, _ := strings.Cut(reference, "#")
	path := documentPath
	if target != "" {
		path = filepath.Clean(filepath.Join(filepath.Dir(documentPath), target))
	}
	document, err := validator.document(path)
	if err != nil {
		return fmt.Errorf("%s: resolve %s: %w", location, reference, err)
	}
	schema, err := resolvePointer(document, fragment)
	if err != nil {
		return fmt.Errorf("%s: resolve %s: %w", location, reference, err)
	}
	return validator.validate(value, schema, path, location)
}

func (validator *schemaValidator) validateObjectKeyword(value any, schema map[string]any, keyword, documentPath, location string) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	properties, _ := schema["properties"].(map[string]any)
	names := make([]string, 0, len(object))
	for name := range object {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		child := location + "." + name
		switch keyword {
		case "additionalProperties":
			if _, declared := properties[name]; declared {
				continue
			}
			if schema[keyword] == false {
				return fmt.Errorf("%s: unknown property", child)
			}
			if err := validator.validate(object[name], schema[keyword], documentPath, child); err != nil {
				return err
			}
		case "properties":
			if propertySchema, declared := properties[name]; declared {
				if err := validator.validate(object[name], propertySchema, documentPath, child); err != nil {
					return err
				}
			}
		case "propertyNames":
			if err := validator.validate(name, schema[keyword], documentPath, child+" (name)"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (validator *schemaValidator) validateAlternatives(value any, keyword string, argument any, documentPath, location string) error {
	alternatives, _ := argument.([]any)
	matches := 0
	var failures []string
	for _, alternative := range alternatives {
		if err := validator.validate(value, alternative, documentPath, location); err != nil {
			failures = append(failures, err.Error())
			continue
		}
		matches++
	}
	switch {
	case matches == 0:
		return fmt.Errorf("%s: matches no %s alternative: %s", location, keyword, strings.Join(failures, "; "))
	case keyword == "oneOf" && matches > 1:
		return fmt.Errorf("%s: matches %d oneOf alternatives", location, matches)
	}
	return nil
}

func (validator *schemaValidator) validatePattern(value, argument any, location string) error {
	text, isString := value.(string)
	if !isString {
		return nil
	}
	pattern, _ := argument.(string)
	validator.mu.Lock()
	compiled, cached := validator.patterns[pattern]
	if !cached {
		var err error
		if compiled, err = regexp.Compile(pattern); err != nil {
			validator.mu.Unlock()
			return fmt.Errorf("%s: invalid pattern %q: %w", location, pattern, err)
		}
		validator.patterns[pattern] = compiled
	}
	validator.mu.Unlock()
	if !compiled.MatchString(text) {
		return fmt.Errorf("%s: %q does not match %q", location, text, pattern)
	}
	return nil
}

func validateType(value, argument any, location string) error {
	var names []string
	switch typed := argument.(type) {
	case string:
		names = []string{typed}
	case []any:
		names = make([]string, 0, len(typed))
		for _, name := range typed {
			names = append(names, fmt.Sprint(name))
		}
	}
	for _, name := range names {
		if jsonType(value) == name || (name == "number" && jsonType(value) == "integer") {
			return nil
		}
	}
	return fmt.Errorf("%s: %s is not of type %v", location, jsonType(value), names)
}

func jsonType(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case json.Number:
		if _, ok := new(big.Int).SetString(typed.String(), 10); ok {
			return "integer"
		}
		return "number"
	case float64:
		if typed == float64(int64(typed)) {
			return "integer"
		}
		return "number"
	}
	return fmt.Sprintf("%T", value)
}

func validateBound(value any, keyword string, argument any, location string) error {
	number, ok := value.(json.Number)
	if !ok {
		return nil
	}
	actual, _, err := big.ParseFloat(number.String(), 10, 256, big.ToNearestEven)
	if err != nil {
		return fmt.Errorf("%s: %w", location, err)
	}
	bound, _, err := big.ParseFloat(fmt.Sprint(argument), 10, 256, big.ToNearestEven)
	if err != nil {
		return fmt.Errorf("%s: invalid %s: %w", location, keyword, err)
	}
	comparison := actual.Cmp(bound)
	if (keyword == "minimum" && comparison < 0) || (keyword == "maximum" && comparison > 0) ||
		(keyword == "exclusiveMinimum" && comparison <= 0) {
		return fmt.Errorf("%s: %s violates %s %v", location, number, keyword, argument)
	}
	return nil
}

func validateFormat(value, argument any, location string) error {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	if argument != "date-time" {
		return fmt.Errorf("%s: format %v is not supported by the harness validator", location, argument)
	}
	if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
		return fmt.Errorf("%s: %q is not an RFC 3339 date-time", location, text)
	}
	return nil
}

func schemaInteger(argument any) int64 {
	number, _ := argument.(json.Number)
	value, _ := number.Int64()
	return value
}

func jsonEqual(first, second any) bool {
	firstBytes, firstErr := json.Marshal(first)
	secondBytes, secondErr := json.Marshal(second)
	return firstErr == nil && secondErr == nil && bytes.Equal(firstBytes, secondBytes)
}

func TestSchemaValidator(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		path      string
		validator *schemaValidator
	}

	setup := func(t *testing.T, schema string) *testBundle {
		t.Helper()

		path := filepath.Join(t.TempDir(), "schema.json")
		require.NoError(t, os.WriteFile(path, []byte(schema), 0o600))
		return &testBundle{path: path, validator: newSchemaValidator()}
	}
	validate := func(t *testing.T, bundle *testBundle, value string) error {
		t.Helper()

		decoded, err := decodeJSONWithNumbers([]byte(value))
		require.NoError(t, err)
		return bundle.validator.validateFile(decoded, bundle.path, "")
	}

	t.Run("AdditionalPropertiesRejectUnknownNames", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t, `{"additionalProperties": false, "properties": {"known": {"type": "string"}}, "type": "object"}`)
		require.NoError(t, validate(t, bundle, `{"known": "value"}`))
		require.ErrorContains(t, validate(t, bundle, `{"known": "value", "unknown": 1}`), "$.unknown: unknown property")
	})

	t.Run("ConditionalsAndAllOf", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t, `{"allOf": [{"if": {"properties": {"kind": {"const": "a"}}}, "then": {"required": ["a"]}, "else": {"required": ["b"]}}], "type": "object"}`)
		require.NoError(t, validate(t, bundle, `{"a": 1, "kind": "a"}`))
		require.NoError(t, validate(t, bundle, `{"b": 1, "kind": "c"}`))
		require.ErrorContains(t, validate(t, bundle, `{"kind": "a"}`), `missing required property "a"`)
		require.ErrorContains(t, validate(t, bundle, `{"kind": "c"}`), `missing required property "b"`)
	})

	t.Run("IntegersKeepExactValues", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t, `{"maximum": 18446744073709551615, "minimum": 0, "type": "integer"}`)
		require.NoError(t, validate(t, bundle, `18446744073709551615`))
		require.ErrorContains(t, validate(t, bundle, `18446744073709551616`), "maximum")
		require.ErrorContains(t, validate(t, bundle, `-1`), "minimum")
		require.ErrorContains(t, validate(t, bundle, `1.5`), "not of type")
	})

	t.Run("ReferencesResolveAcrossFiles", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t, `{"$defs": {"local": {"$ref": "other.json#/$defs/name"}}, "$ref": "#/$defs/local"}`)
		require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(bundle.path), "other.json"),
			[]byte(`{"$defs": {"name": {"minLength": 2, "type": "string"}}}`), 0o600))
		require.NoError(t, validate(t, bundle, `"ok"`))
		require.ErrorContains(t, validate(t, bundle, `"x"`), "at least 2 characters")
	})

	t.Run("RequiredAndEnum", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t, `{"properties": {"state": {"enum": ["a", "b"]}}, "required": ["state"], "type": "object"}`)
		require.NoError(t, validate(t, bundle, `{"state": "a"}`))
		require.ErrorContains(t, validate(t, bundle, `{}`), `missing required property "state"`)
		require.ErrorContains(t, validate(t, bundle, `{"state": "c"}`), "is not one of")
	})

	t.Run("UnsupportedKeywordsFail", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t, `{"maxLength": 3, "type": "string"}`)
		require.ErrorContains(t, validate(t, bundle, `"abc"`), `keyword "maxLength" is not supported`)
	})
}

package rivermysql

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/template"
)

// sqlc's MySQL parser does not support JSON_TABLE. Keep these predicate
// templates outside its input directory, alongside the other driver SQL.
//
//go:embed internal/dbfragment/json_contains.sql
var jsonContainsSQL string

// Parsed once and used without mutation for all metadata filters.
var jsonContainsTemplates = template.Must(template.New("json_contains").Parse(jsonContainsSQL)) //nolint:gochecknoglobals

// JSON_CONTAINS flattens nested arrays. Traverse each level explicitly to
// preserve Postgres containment boundaries, including whole object elements.
// Scalar subqueries avoid MySQL 8 semijoin rewrites losing nested correlations.
func jsonContains(source string, fragment json.RawMessage, bindFunc func(any) string, depth int) (string, error) {
	fragment = bytes.TrimSpace(fragment)
	values := map[string]string{"source": source}
	query := "scalar"
	switch fragment[0] {
	case '{', '[':
		alias := fmt.Sprintf("metadata_%d", depth)
		var children []string
		childFunc := func(query, param string, child json.RawMessage) error {
			predicate, err := jsonContains(alias+".value", child, bindFunc, depth+1)
			if err != nil {
				return err
			}
			var sql strings.Builder
			err = jsonContainsTemplates.ExecuteTemplate(&sql, query, map[string]string{
				"alias": alias, "param": param, "predicate": predicate, "source": source,
			})
			children = append(children, sql.String())
			return err
		}
		if fragment[0] == '{' {
			values["kind"] = "OBJECT"
			var object map[string]json.RawMessage
			if err := json.Unmarshal(fragment, &object); err != nil {
				return "", err
			}
			for _, key := range slices.Sorted(maps.Keys(object)) {
				encodedKey, err := json.Marshal(key)
				if err != nil {
					return "", err
				}
				if err := childFunc("member", bindFunc("$."+string(encodedKey)), object[key]); err != nil {
					return "", err
				}
			}
		} else {
			values["kind"] = "ARRAY"
			var array []json.RawMessage
			if err := json.Unmarshal(fragment, &array); err != nil {
				return "", err
			}
			for _, child := range array {
				if err := childFunc("element", "", child); err != nil {
					return "", err
				}
			}
		}
		values["children"] = "true"
		if len(children) > 0 {
			values["children"] = strings.Join(children, " AND ")
		}
		query = "container"
	case 'n':
		query = "null"
	default:
		values["param"] = bindFunc(string(fragment))
	}
	var sql strings.Builder
	err := jsonContainsTemplates.ExecuteTemplate(&sql, query, values)
	return sql.String(), err
}

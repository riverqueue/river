package rivermysql

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/template"

	"github.com/riverqueue/river/riverdriver/rivermysql/internal/dbsqlc"
)

// sqlc's MySQL parser does not support JSON_TABLE. Keep these predicate
// templates outside its input directory, alongside the other driver SQL.
//
//go:embed internal/dbfragment/json_contains.sql
var jsonContainsSQL string

// Parsed once and used without mutation for all metadata filters.
var jsonContainsTemplates = template.Must(template.New("json_contains").Parse(jsonContainsSQL)) //nolint:gochecknoglobals

// Merge under row locks before updating. JSON_MERGE_PATCH recursively merges
// objects and removes JSON nulls, while River replaces each supplied member.
// Read all metadata in one query to retain batching and preserve repeated IDs.
func jobMetadataMergeMany(ctx context.Context, dbtx templateReplaceWrapper, ids []int64, doMerge []bool, updates [][]byte) ([][]byte, error) {
	var mergeIDs []int64
	for i, id := range ids {
		if doMerge[i] {
			mergeIDs = append(mergeIDs, id)
		}
	}
	if len(mergeIDs) == 0 {
		return updates, nil
	}
	rows, err := dbsqlc.New().JobGetMetadataForUpdate(ctx, dbtx, mergeIDs)
	if err != nil {
		return nil, err
	}
	metadata := make(map[int64]json.RawMessage, len(rows))
	for _, row := range rows {
		metadata[row.ID] = json.RawMessage(row.Metadata)
	}
	merged := slices.Clone(updates)
	for i, id := range ids {
		value, exists := metadata[id]
		if !doMerge[i] || !exists {
			continue
		}
		merged[i], err = jsonMerge(value, updates[i])
		if err != nil {
			return nil, fmt.Errorf("merging job %d metadata: %w", id, err)
		}
		metadata[id] = merged[i]
	}
	return merged, nil
}

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

func jsonMerge(left, right json.RawMessage) ([]byte, error) {
	left, right = bytes.TrimSpace(left), bytes.TrimSpace(right)
	if !json.Valid(left) || !json.Valid(right) {
		return nil, errors.New("invalid JSON metadata")
	}
	if left[0] == '{' && right[0] == '{' {
		var original, updates map[string]json.RawMessage
		if err := json.Unmarshal(left, &original); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(right, &updates); err != nil {
			return nil, err
		}
		maps.Copy(original, updates)
		return json.Marshal(original)
	}
	values := make([]json.RawMessage, 0)
	for _, value := range []json.RawMessage{left, right} {
		if value[0] == '[' {
			var elements []json.RawMessage
			if err := json.Unmarshal(value, &elements); err != nil {
				return nil, err
			}
			values = append(values, elements...)
		} else {
			values = append(values, value)
		}
	}
	return json.Marshal(values)
}

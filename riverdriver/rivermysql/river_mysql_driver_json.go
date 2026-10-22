package rivermysql

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
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
	metadata := make(map[int64]map[string]json.RawMessage, len(rows))
	for _, row := range rows {
		value := make(map[string]json.RawMessage)
		if err := json.Unmarshal(row.Metadata, &value); err != nil {
			return nil, fmt.Errorf("decoding job %d metadata: %w", row.ID, err)
		}
		if value == nil {
			value = make(map[string]json.RawMessage)
		}
		metadata[row.ID] = value
	}
	merged := slices.Clone(updates)
	for i, id := range ids {
		value, exists := metadata[id]
		if !doMerge[i] || !exists {
			continue
		}
		var update map[string]json.RawMessage
		if err := json.Unmarshal(updates[i], &update); err != nil {
			return nil, fmt.Errorf("decoding job %d metadata update: %w", id, err)
		}
		maps.Copy(value, update)
		merged[i], err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding job %d metadata: %w", id, err)
		}
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

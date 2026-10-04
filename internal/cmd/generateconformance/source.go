package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// jsonField is one field of a Go struct's JSON encoding.
type jsonField struct {
	Name      string `json:"name"`
	OmitEmpty bool   `json:"omitempty"`
}

// sourceStructJSONFields returns a struct's JSON fields in declaration order
// by parsing its source file, so payload shapes of unexported notification
// structs are derived from Go rather than restated by hand.
func sourceStructJSONFields(path, typeName string) ([]jsonField, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var fields []jsonField
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok || spec.Name.Name != typeName {
			return true
		}
		structType, ok := spec.Type.(*ast.StructType)
		if !ok {
			return false
		}
		found = true
		for _, field := range structType.Fields.List {
			if field.Tag == nil {
				continue
			}
			tag, err := strconv.Unquote(field.Tag.Value)
			if err != nil {
				continue
			}
			name, options, _ := strings.Cut(reflect.StructTag(tag).Get("json"), ",")
			if name == "" || name == "-" {
				continue
			}
			fields = append(fields, jsonField{Name: name, OmitEmpty: slices.Contains(strings.Split(options, ","), "omitempty")})
		}
		return false
	})
	if !found {
		return nil, fmt.Errorf("struct %s not found in %s", typeName, path)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("struct %s in %s has no JSON fields", typeName, path)
	}
	return fields, nil
}

// sourceStringConstants returns the values of string constants declared with
// the named type in a source file, keyed by constant name.
func sourceStringConstants(path, typeName string) (map[string]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	constants := make(map[string]string)
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Values) != len(value.Names) {
				continue
			}
			identifier, ok := value.Type.(*ast.Ident)
			if !ok || identifier.Name != typeName {
				continue
			}
			for index, name := range value.Names {
				literal, ok := value.Values[index].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				unquoted, err := strconv.Unquote(literal.Value)
				if err != nil {
					return nil, err
				}
				constants[name.Name] = unquoted
			}
		}
	}
	if len(constants) == 0 {
		return nil, fmt.Errorf("no %s string constants in %s", typeName, path)
	}
	return constants, nil
}

var jsonBuildObjectPattern = regexp.MustCompile(`json_build_object\(([^)]*)\)`)

// sqlNotificationKeys returns the keys of the json_build_object payload that a
// named sqlc query passes to pg_notify.
func sqlNotificationKeys(path, queryName string) ([]string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	_, query, found := strings.Cut(string(contents), "-- name: "+queryName+" ")
	if !found {
		return nil, fmt.Errorf("query %s not found in %s", queryName, path)
	}
	query, _, _ = strings.Cut(query, "-- name: ")
	if !strings.Contains(query, "pg_notify(") {
		return nil, fmt.Errorf("query %s in %s sends no notification", queryName, path)
	}
	match := jsonBuildObjectPattern.FindStringSubmatch(query)
	if match == nil {
		return nil, fmt.Errorf("query %s in %s builds no JSON payload", queryName, path)
	}
	arguments := strings.Split(match[1], ",")
	if len(arguments)%2 != 0 {
		return nil, fmt.Errorf("query %s in %s has an odd json_build_object argument list", queryName, path)
	}
	keys := make([]string, 0, len(arguments)/2)
	for index := 0; index < len(arguments); index += 2 {
		key := strings.TrimSpace(arguments[index])
		if !strings.HasPrefix(key, "'") || !strings.HasSuffix(key, "'") {
			return nil, errors.New("json_build_object keys must be literals")
		}
		keys = append(keys, strings.Trim(key, "'"))
	}
	slices.Sort(keys)
	return keys, nil
}

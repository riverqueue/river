package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// metadataKeyCollector accumulates reserved metadata keys along with every
// mechanism and source that uses them.
type metadataKeyCollector struct {
	kinds   map[string]map[string]struct{}
	sources map[string]map[string]struct{}
}

func newMetadataKeyCollector() *metadataKeyCollector {
	return &metadataKeyCollector{
		kinds:   make(map[string]map[string]struct{}),
		sources: make(map[string]map[string]struct{}),
	}
}

func (c *metadataKeyCollector) add(key, kind, source string) {
	if c.kinds[key] == nil {
		c.kinds[key] = make(map[string]struct{})
		c.sources[key] = make(map[string]struct{})
	}
	c.kinds[key][kind] = struct{}{}
	c.sources[key][source] = struct{}{}
}

func (c *metadataKeyCollector) items() []*extractedItem {
	items := make([]*extractedItem, 0, len(c.kinds))
	for _, key := range sortedKeys(c.kinds) {
		items = append(items, &extractedItem{
			Area:   "metadata_key",
			Detail: strings.Join(sortedKeys(c.kinds[key]), ", "),
			ID:     "metadata_key." + key,
			Source: strings.Join(sortedKeys(c.sources[key]), ", "),
		})
	}
	return items
}

type metadataKeyUse struct {
	key    string
	kind   string
	source string
}

// sqlQuery is one named sqlc query.
type sqlQuery struct {
	body string
	name string
	path string
}

const (
	pgxDBSQLCDir    = "riverdriver/riverpgxv5/internal/dbsqlc"
	sqliteDBSQLCDir = "riverdriver/riversqlite/internal/dbsqlc"
)

// metadataScanRoots are the library directories scanned for metadata keys in
// Go code. The root package is scanned non-recursively because its
// subdirectories are separate packages covered elsewhere or not library code.
func metadataScanRoots() []struct {
	dir       string
	recursive bool
} {
	return []struct {
		dir       string
		recursive bool
	}{
		{dir: ".", recursive: false},
		{dir: "internal", recursive: true},
		{dir: "riverdriver", recursive: true},
		{dir: "riverlog", recursive: true},
		{dir: "rivershared", recursive: true},
		{dir: "rivertype", recursive: true},
	}
}

// metadataScanExcluded are directories below the scan roots that are not
// library code.
func metadataScanExcluded() map[string]struct{} {
	return map[string]struct{}{
		"internal/cmd":                {},
		"riverdriver/riverdrivertest": {},
	}
}

// extractMetadataKeys returns one item per reserved metadata key found in Go
// constants, Go metadata helper calls and struct tags, and driver SQL.
func extractMetadataKeys(root string) ([]*extractedItem, error) {
	files, err := metadataGoFiles(root)
	if err != nil {
		return nil, err
	}

	collector := newMetadataKeyCollector()

	// Constants are collected first so that helper calls referencing them by
	// name can be resolved to their values.
	constsByName := make(map[string][]string)
	numConsts := 0
	for _, file := range files {
		for _, constDecl := range fileStringConsts(file.file.Name.Name, file) {
			if !strings.Contains(constDecl.name, "MetadataKey") && !strings.Contains(constDecl.name, "metadataKey") {
				continue
			}
			collector.add(constDecl.value, "go:const", fmt.Sprintf("%s:%s.%s", file.path, constDecl.pkg, constDecl.name))
			qualified := constDecl.pkg + "." + constDecl.name
			constsByName[qualified] = append(constsByName[qualified], constDecl.value)
			numConsts++
		}
	}
	if numConsts == 0 {
		return nil, errors.New("no metadata key constants found")
	}

	numUses := 0
	for _, file := range files {
		uses, err := goMetadataKeyUses(file, constsByName)
		if err != nil {
			return nil, err
		}
		for _, use := range uses {
			collector.add(use.key, use.kind, use.source)
		}
		numUses += len(uses)
	}
	if numUses == 0 {
		return nil, errors.New("no metadata key uses found in Go helper calls or struct tags")
	}

	numSQL := 0
	for _, dir := range []string{pgxDBSQLCDir, sqliteDBSQLCDir} {
		queries, err := sqlQueries(root, dir)
		if err != nil {
			return nil, err
		}
		for _, query := range queries {
			uses, err := sqlMetadataKeyUses(query)
			if err != nil {
				return nil, err
			}
			for _, use := range uses {
				collector.add(use.key, use.kind, use.source)
			}
			numSQL += len(uses)
		}
	}
	if numSQL == 0 {
		return nil, errors.New("no metadata keys found in driver SQL")
	}

	return collector.items(), nil
}

// metadataGoFiles parses the non-test Go files under the metadata scan roots.
func metadataGoFiles(root string) ([]*goFile, error) {
	excluded := metadataScanExcluded()
	fset := token.NewFileSet()

	var files []*goFile
	for _, scanRoot := range metadataScanRoots() {
		start := filepath.Join(root, filepath.FromSlash(scanRoot.dir))
		err := filepath.WalkDir(start, func(filePath string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(root, filePath)
			if err != nil {
				return err
			}
			relPath = filepath.ToSlash(relPath)
			if entry.IsDir() {
				if filePath == start {
					return nil
				}
				name := entry.Name()
				if !scanRoot.recursive || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "node_modules" {
					return filepath.SkipDir
				}
				if _, ok := excluded[relPath]; ok {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(relPath, ".go") || strings.HasSuffix(relPath, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, filePath, nil, parser.SkipObjectResolution)
			if err != nil {
				return fmt.Errorf("parse %s: %w", relPath, err)
			}
			files = append(files, &goFile{file: file, path: relPath})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", scanRoot.dir, err)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	return files, nil
}

// goMetadataKeyUses finds metadata keys used with gjson/sjson helpers on
// metadata values, metadata update map indexes, and `river:` struct tags.
func goMetadataKeyUses(file *goFile, constsByName map[string][]string) ([]*metadataKeyUse, error) {
	pkgName := file.file.Name.Name

	resolveKey := func(expr ast.Expr) (string, bool, error) {
		if value, ok := stringLiteral(expr); ok {
			return value, true, nil
		}
		var qualified string
		switch typed := expr.(type) {
		case *ast.Ident:
			qualified = pkgName + "." + typed.Name
		case *ast.SelectorExpr:
			pkgIdent, ok := typed.X.(*ast.Ident)
			if !ok {
				return "", false, nil
			}
			qualified = pkgIdent.Name + "." + typed.Sel.Name
		default:
			return "", false, nil
		}
		if !strings.Contains(qualified, "MetadataKey") && !strings.Contains(qualified, "metadataKey") {
			return "", false, nil
		}
		values := constsByName[qualified]
		if len(values) != 1 {
			return "", false, fmt.Errorf("%s: cannot resolve metadata key constant %s", file.path, qualified)
		}
		return values[0], true, nil
	}

	var (
		inspectErr error
		uses       []*metadataKeyUse
	)
	inspect := func(symbol string, node ast.Node) {
		ast.Inspect(node, func(node ast.Node) bool {
			if inspectErr != nil {
				return false
			}
			switch typed := node.(type) {
			case *ast.CallExpr:
				selector, isSelector := typed.Fun.(*ast.SelectorExpr)
				if !isSelector || len(typed.Args) < 2 {
					return true
				}
				pkgIdent, isIdent := selector.X.(*ast.Ident)
				if !isIdent {
					return true
				}
				helper := pkgIdent.Name + "." + selector.Sel.Name
				switch helper {
				case "gjson.GetBytes", "sjson.DeleteBytes", "sjson.SetBytes", "sjson.SetRawBytes":
				default:
					return true
				}
				if !strings.Contains(strings.ToLower(types.ExprString(typed.Args[0])), "metadata") {
					return true
				}
				key, ok, err := resolveKey(typed.Args[1])
				if err != nil {
					inspectErr = err
					return false
				}
				if ok {
					uses = append(uses, &metadataKeyUse{key: key, kind: "go:" + helper, source: file.path + ":" + symbol})
				}
			case *ast.IndexExpr:
				if !strings.Contains(strings.ToLower(types.ExprString(typed.X)), "metadataupdates") {
					return true
				}
				key, ok, err := resolveKey(typed.Index)
				if err != nil {
					inspectErr = err
					return false
				}
				if ok {
					uses = append(uses, &metadataKeyUse{key: key, kind: "go:metadata_updates_index", source: file.path + ":" + symbol})
				}
			case *ast.Field:
				if typed.Tag == nil {
					return true
				}
				tag, err := strconv.Unquote(typed.Tag.Value)
				if err != nil {
					return true
				}
				jsonName, _, _ := strings.Cut(reflect.StructTag(tag).Get("json"), ",")
				if strings.HasPrefix(jsonName, "river:") {
					uses = append(uses, &metadataKeyUse{key: jsonName, kind: "go:json_tag", source: file.path + ":" + symbol})
				}
			}
			return true
		})
	}

	for _, decl := range file.file.Decls {
		switch typed := decl.(type) {
		case *ast.FuncDecl:
			symbol := pkgName + "." + typed.Name.Name
			if typed.Recv != nil {
				symbol = pkgName + "." + receiverTypeName(typed.Recv.List[0].Type) + "." + typed.Name.Name
			}
			inspect(symbol, typed)
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				symbol := pkgName
				if typeSpec, ok := spec.(*ast.TypeSpec); ok {
					symbol = pkgName + "." + typeSpec.Name.Name
				}
				inspect(symbol, spec)
			}
		}
	}
	if inspectErr != nil {
		return nil, inspectErr
	}
	return uses, nil
}

var (
	sqlJSONBBuildObjectPattern = regexp.MustCompile(`\bmetadata\s*(?:=|\|\|)\s*jsonb_build_object\s*\(`)
	sqlJSONBLiteralPattern     = regexp.MustCompile(`\bmetadata\s*\|\|\s*'(\{[^']*\})'\s*::\s*jsonb`)
	sqlJSONBPatchPattern       = regexp.MustCompile(`jsonb_patch\s*\(\s*json\s*\(\s*metadata\s*\)\s*,\s*json\s*\(\s*'(\{[^']*\})'`)
	sqlJSONBSetPathPattern     = regexp.MustCompile(`jsonb_set\s*\(\s*metadata\s*,\s*'\{([^}']+)\}'`)
	sqlJSONBSetSQLitePattern   = regexp.MustCompile(`jsonb_set\s*\(\s*metadata\s*,\s*'\$\.(?:"([^"']+)"|([A-Za-z0-9_:]+))'`)
	sqlNamePattern             = regexp.MustCompile(`(?m)^--\s*name:\s*(\w+)`)
	sqlPGNotifyPattern         = regexp.MustCompile(`\bpg_notify\s*\(`)
)

// sqlMetadataKeyUses finds metadata keys written by one query.
func sqlMetadataKeyUses(query *sqlQuery) ([]*metadataKeyUse, error) {
	source := query.path + ":" + query.name
	var uses []*metadataKeyUse

	for _, match := range sqlJSONBSetPathPattern.FindAllStringSubmatch(query.body, -1) {
		uses = append(uses, &metadataKeyUse{key: match[1], kind: "sql:jsonb_set", source: source})
	}
	for _, match := range sqlJSONBSetSQLitePattern.FindAllStringSubmatch(query.body, -1) {
		uses = append(uses, &metadataKeyUse{key: match[1] + match[2], kind: "sql:jsonb_set", source: source})
	}
	for _, loc := range sqlJSONBBuildObjectPattern.FindAllStringIndex(query.body, -1) {
		args, err := sqlCallArgs(query.body, loc[1]-1)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", source, err)
		}
		for i := 0; i < len(args); i += 2 {
			key, ok := sqlStringLiteral(args[i])
			if !ok {
				return nil, fmt.Errorf("%s: non-literal jsonb_build_object key %q", source, args[i])
			}
			uses = append(uses, &metadataKeyUse{key: key, kind: "sql:jsonb_build_object", source: source})
		}
	}
	for _, pattern := range []*regexp.Regexp{sqlJSONBLiteralPattern, sqlJSONBPatchPattern} {
		for _, match := range pattern.FindAllStringSubmatch(query.body, -1) {
			var object map[string]json.RawMessage
			if err := json.Unmarshal([]byte(match[1]), &object); err != nil {
				return nil, fmt.Errorf("%s: decode metadata literal %s: %w", source, match[1], err)
			}
			for _, key := range sortedKeys(object) {
				uses = append(uses, &metadataKeyUse{key: key, kind: "sql:json_literal", source: source})
			}
		}
	}
	return uses, nil
}

// extractSQLNotificationPayloads returns one item per `pg_notify` call whose
// payload is a `json_build_object` in the PostgreSQL driver queries.
func extractSQLNotificationPayloads(root string) ([]*extractedItem, error) {
	queries, err := sqlQueries(root, pgxDBSQLCDir)
	if err != nil {
		return nil, err
	}

	var items []*extractedItem
	for _, query := range queries {
		source := query.path + ":" + query.name
		var payloads []string
		for _, loc := range sqlPGNotifyPattern.FindAllStringIndex(query.body, -1) {
			notifyArgs, err := sqlCallArgs(query.body, loc[1]-1)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", source, err)
			}
			for _, notifyArg := range notifyArgs {
				start := strings.Index(notifyArg, "json_build_object(")
				if start < 0 {
					continue
				}
				objectArgs, err := sqlCallArgs(notifyArg, start+len("json_build_object"))
				if err != nil {
					return nil, fmt.Errorf("%s: %w", source, err)
				}
				if len(objectArgs)%2 != 0 {
					return nil, fmt.Errorf("%s: json_build_object has an odd number of arguments", source)
				}
				fields := make([]string, 0, len(objectArgs)/2)
				for i := 0; i < len(objectArgs); i += 2 {
					key, ok := sqlStringLiteral(objectArgs[i])
					if !ok {
						return nil, fmt.Errorf("%s: non-literal json_build_object key %q", source, objectArgs[i])
					}
					if value, ok := sqlStringLiteral(objectArgs[i+1]); ok {
						key += "=" + value
					}
					fields = append(fields, key)
				}
				sort.Strings(fields)
				payloads = append(payloads, strings.Join(fields, "; "))
			}
		}
		for i, payload := range payloads {
			id := "notification_payload.sql." + snakeCase(query.name)
			if len(payloads) > 1 {
				id += "." + strconv.Itoa(i+1)
			}
			items = append(items, &extractedItem{
				Area:   "notification_payload",
				Detail: payload,
				ID:     id,
				Source: source,
			})
		}
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("no pg_notify json_build_object payloads found in %s", pgxDBSQLCDir)
	}
	return items, nil
}

// sqlQueries splits every .sql file in dir into named sqlc queries, with
// whole-line comments removed from each body.
func sqlQueries(root, dir string) ([]*sqlQuery, error) {
	matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(dir), "*.sql"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", dir, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no SQL files found in %s", dir)
	}
	sort.Strings(matches)

	var queries []*sqlQuery
	for _, match := range matches {
		contents, err := os.ReadFile(match)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", match, err)
		}
		relPath := path.Join(dir, filepath.Base(match))
		text := string(contents)
		locs := sqlNamePattern.FindAllStringSubmatchIndex(text, -1)
		for i, loc := range locs {
			end := len(text)
			if i+1 < len(locs) {
				end = locs[i+1][0]
			}
			var body strings.Builder
			for line := range strings.SplitSeq(text[loc[1]:end], "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "--") {
					continue
				}
				body.WriteString(line + "\n")
			}
			queries = append(queries, &sqlQuery{
				body: body.String(),
				name: text[loc[2]:loc[3]],
				path: relPath,
			})
		}
	}
	return queries, nil
}

// sqlCallArgs splits the top-level arguments of the SQL call whose opening
// parenthesis is at openIndex, respecting nested parentheses and quotes.
func sqlCallArgs(text string, openIndex int) ([]string, error) {
	if openIndex >= len(text) || text[openIndex] != '(' {
		return nil, fmt.Errorf("expected '(' at offset %d", openIndex)
	}

	var (
		args     []string
		depth    = 0
		argStart = openIndex + 1
		quote    byte
	)
	for i := openIndex + 1; i < len(text); i++ {
		char := text[i]
		if quote != 0 {
			if char == quote {
				quote = 0
			}
			continue
		}
		switch char {
		case '\'', '"':
			quote = char
		case '(':
			depth++
		case ')':
			if depth == 0 {
				if arg := strings.TrimSpace(text[argStart:i]); arg != "" || len(args) > 0 {
					args = append(args, arg)
				}
				return args, nil
			}
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(text[argStart:i]))
				argStart = i + 1
			}
		}
	}
	return nil, fmt.Errorf("unterminated call starting at offset %d", openIndex)
}

// sqlStringLiteral returns the value of a single-quoted SQL string literal.
func sqlStringLiteral(expr string) (string, bool) {
	if len(expr) < 2 || expr[0] != '\'' || expr[len(expr)-1] != '\'' {
		return "", false
	}
	inner := expr[1 : len(expr)-1]
	if strings.Contains(strings.ReplaceAll(inner, "''", ""), "'") {
		return "", false
	}
	return strings.ReplaceAll(inner, "''", "'"), true
}

// snakeCase converts a Go-style identifier like JobCancel to job_cancel.
func snakeCase(name string) string {
	var sb strings.Builder
	for i, char := range name {
		if char >= 'A' && char <= 'Z' {
			if i > 0 {
				sb.WriteByte('_')
			}
			char += 'a' - 'A'
		}
		sb.WriteRune(char)
	}
	return sb.String()
}

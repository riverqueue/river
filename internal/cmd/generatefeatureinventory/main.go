// Command generatefeatureinventory maintains River's cross-language feature
// inventory, a drift gate between the Go implementation and the conformance
// program.
//
// It derives an inventory of Go-visible River features (configuration fields,
// insert options, client methods, job states, event kinds, reserved metadata
// keys, notification topics and payloads, driver and extension interfaces,
// migrations, and query parameter builders) from Go reflection, Go syntax
// trees, and driver SQL. Each derived item is merged into
// conformance/feature-inventory.json, where people classify it for
// cross-language compatibility and link it to executable conformance
// scenarios. conformance/feature-matrix.md is rendered from the result.
//
// Without flags the command rewrites both files, adding new items as
// "unclassified" and dropping items that no longer exist, then exits non-zero
// if any item still needs attention. With -check it writes nothing and fails
// if either file is out of date or any classification is incomplete, so CI
// fails whenever a Go feature is added or removed without being classified.
package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	harnessDir         = "conformance/harness"
	inventoryPath      = "conformance/feature-inventory.json"
	matrixPath         = "conformance/feature-matrix.md"
	scenarioCatalogDir = "conformance/scenarios"
)

//go:embed matrix_header.md
var matrixHeader string

func main() {
	check := flag.Bool("check", false, "check the inventory and matrix without writing")
	root := flag.String("root", ".", "repository root")
	flag.Parse()

	problems, err := run(*root, *check)
	if err != nil {
		fmt.Fprintln(os.Stderr, "generatefeatureinventory:", err)
		os.Exit(1)
	}
	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, "generatefeatureinventory: feature inventory needs attention:")
		for _, problem := range problems {
			fmt.Fprintln(os.Stderr, "  - "+problem)
		}
		if *check {
			fmt.Fprintln(os.Stderr, "Run `go run ./internal/cmd/generatefeatureinventory`, then classify any unclassified items in "+inventoryPath+".")
		}
		os.Exit(1)
	}
}

// run extracts the inventory from the repository at root and either checks or
// rewrites the checked-in files. It returns problems that need a developer's
// attention; an error means the command itself could not complete.
func run(root string, check bool) ([]string, error) {
	extracted, err := extractAll(root)
	if err != nil {
		return nil, err
	}
	knownScenarios, err := loadScenarioCatalogs(root)
	if err != nil {
		return nil, err
	}
	registry, err := loadScenarioRegistry(root)
	if err != nil {
		return nil, err
	}

	existingBytes, err := os.ReadFile(filepath.Join(root, inventoryPath))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read %s: %w", inventoryPath, err)
	}
	var existing *inventory
	if existingBytes != nil {
		if existing, err = decodeInventory(existingBytes); err != nil {
			return nil, fmt.Errorf("%s: %w", inventoryPath, err)
		}
	}

	if check {
		return checkFiles(root, existing, existingBytes, extracted, knownScenarios, registry)
	}

	merged, report := mergeInventory(existing, extracted)
	inventoryBytes, err := encodeInventory(merged)
	if err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(root, inventoryPath), inventoryBytes); err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(root, matrixPath), []byte(renderMatrix(matrixHeader, merged, registry))); err != nil {
		return nil, err
	}
	if len(report.Added) > 0 {
		fmt.Fprintln(os.Stderr, "added (unclassified): "+strings.Join(report.Added, ", "))
	}
	if len(report.Removed) > 0 {
		fmt.Fprintln(os.Stderr, "removed: "+strings.Join(report.Removed, ", "))
	}
	return validateClassifications(merged, knownScenarios, registry), nil
}

// checkFiles verifies the checked-in inventory and matrix without writing.
func checkFiles(root string, existing *inventory, existingBytes []byte, extracted []*extractedItem, knownScenarios map[string]struct{}, registry map[string]scenarioOwner) ([]string, error) {
	if existing == nil {
		return []string{inventoryPath + " does not exist"}, nil
	}

	problems := diffInventory(existing, extracted)
	problems = append(problems, validateClassifications(existing, knownScenarios, registry)...)

	canonical, err := encodeInventory(existing)
	if err != nil {
		return nil, err
	}
	sortedByID := sort.SliceIsSorted(existing.Items, func(i, j int) bool { return existing.Items[i].ID < existing.Items[j].ID })
	if !bytes.Equal(canonical, existingBytes) || !sortedByID || existing.Schema != inventorySchemaRef {
		problems = append(problems, inventoryPath+" is not in canonical form (sorted by ID, normalized formatting)")
	}

	// Render the matrix from the merged inventory so that a stale matrix is
	// reported even when the inventory itself also has problems.
	merged, _ := mergeInventory(existing, extracted)
	matrixBytes, err := os.ReadFile(filepath.Join(root, matrixPath))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read %s: %w", matrixPath, err)
	}
	if string(matrixBytes) != renderMatrix(matrixHeader, merged, registry) {
		problems = append(problems, matrixPath+" is out of date")
	}
	return problems, nil
}

// loadScenarioCatalogs returns every scenario ID declared by the scenario
// catalogs.
func loadScenarioCatalogs(root string) (map[string]struct{}, error) {
	matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(scenarioCatalogDir), "*.json"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", scenarioCatalogDir, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no scenario catalogs found in %s", scenarioCatalogDir)
	}

	scenarios := make(map[string]struct{})
	for _, match := range matches {
		contents, err := os.ReadFile(match)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", match, err)
		}
		var catalog struct {
			Scenarios []struct {
				Name string `json:"name"`
			} `json:"scenarios"`
		}
		if err := json.Unmarshal(contents, &catalog); err != nil {
			return nil, fmt.Errorf("decode %s: %w", match, err)
		}
		for _, scenario := range catalog.Scenarios {
			scenarios[scenario.Name] = struct{}{}
		}
	}
	return scenarios, nil
}

// loadScenarioRegistry parses the harness test files for the executable
// scenario registry.
func loadScenarioRegistry(root string) (map[string]scenarioOwner, error) {
	matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(harnessDir), "*_test.go"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", harnessDir, err)
	}
	sort.Strings(matches)

	sources := make(map[string][]byte, len(matches))
	for _, match := range matches {
		contents, err := os.ReadFile(match)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", match, err)
		}
		sources[path.Join(harnessDir, filepath.Base(match))] = contents
	}
	return parseScenarioRegistry(sources)
}

// parseScenarioRegistry extracts scenario bindings from Go sources keyed by
// path. It collects string constants from every file (for owner names) and
// every `map[string]scenarioBinding` composite literal whose entries use
// string keys.
func parseScenarioRegistry(sources map[string][]byte) (map[string]scenarioOwner, error) {
	fset := token.NewFileSet()
	files := make([]*goFile, 0, len(sources))
	for _, filePath := range sortedKeys(sources) {
		file, err := parser.ParseFile(fset, filePath, sources[filePath], parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", filePath, err)
		}
		files = append(files, &goFile{file: file, path: filePath})
	}

	constants := make(map[string]string)
	for _, file := range files {
		for _, constDecl := range fileStringConsts(file.file.Name.Name, file) {
			constants[constDecl.name] = constDecl.value
		}
	}

	var (
		inspectErr error
		registry   = make(map[string]scenarioOwner)
	)
	for _, file := range files {
		ast.Inspect(file.file, func(node ast.Node) bool {
			if inspectErr != nil {
				return false
			}
			compositeLit, ok := node.(*ast.CompositeLit)
			if !ok || !isScenarioBindingMap(compositeLit.Type) {
				return true
			}
			for _, elt := range compositeLit.Elts {
				keyValue, isKeyValue := elt.(*ast.KeyValueExpr)
				if !isKeyValue {
					continue
				}
				id, isString := stringLiteral(keyValue.Key)
				if !isString {
					continue
				}
				binding, err := parseScenarioBinding(keyValue.Value, constants)
				if err != nil {
					inspectErr = fmt.Errorf("%s: scenario %q: %w", file.path, id, err)
					return false
				}
				if _, exists := registry[id]; exists {
					inspectErr = fmt.Errorf("%s: scenario %q is registered more than once", file.path, id)
					return false
				}
				registry[id] = binding
			}
			return false
		})
		if inspectErr != nil {
			return nil, inspectErr
		}
	}
	if len(registry) == 0 {
		return nil, errors.New("no map[string]scenarioBinding registry entries found in " + harnessDir)
	}
	return registry, nil
}

// isScenarioBindingMap reports whether expr is `map[string]scenarioBinding`.
func isScenarioBindingMap(expr ast.Expr) bool {
	mapType, ok := expr.(*ast.MapType)
	if !ok {
		return false
	}
	key, keyOK := mapType.Key.(*ast.Ident)
	value, valueOK := mapType.Value.(*ast.Ident)
	return keyOK && valueOK && key.Name == "string" && value.Name == "scenarioBinding"
}

// parseScenarioBinding reads the owner and tier of one scenarioBinding
// literal. Owners may be string literals or string constants.
func parseScenarioBinding(expr ast.Expr, constants map[string]string) (scenarioOwner, error) {
	compositeLit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return scenarioOwner{}, errors.New("binding is not a composite literal")
	}

	var binding scenarioOwner
	for _, elt := range compositeLit.Elts {
		keyValue, isKeyValue := elt.(*ast.KeyValueExpr)
		if !isKeyValue {
			return scenarioOwner{}, errors.New("binding fields must be keyed")
		}
		field, isIdent := keyValue.Key.(*ast.Ident)
		if !isIdent {
			continue
		}
		value, isLiteral := stringLiteral(keyValue.Value)
		if !isLiteral {
			ident, isConstIdent := keyValue.Value.(*ast.Ident)
			if !isConstIdent {
				return scenarioOwner{}, fmt.Errorf("field %s is not a string literal or constant", field.Name)
			}
			var isKnown bool
			if value, isKnown = constants[ident.Name]; !isKnown {
				return scenarioOwner{}, fmt.Errorf("field %s references unknown constant %s", field.Name, ident.Name)
			}
		}
		switch field.Name {
		case "owner":
			binding.Owner = value
		case "tier":
			binding.Tier = value
		}
	}
	if binding.Owner == "" {
		return scenarioOwner{}, errors.New("binding has no owner")
	}
	return binding, nil
}

func writeFile(filePath string, contents []byte) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("create directory for %s: %w", filePath, err)
	}
	//nolint:gosec // Generated repository artifacts are intentionally world-readable.
	if err := os.WriteFile(filePath, contents, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", filePath, err)
	}
	return nil
}

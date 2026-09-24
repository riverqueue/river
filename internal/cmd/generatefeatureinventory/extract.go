package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// goFile is one parsed Go source file.
type goFile struct {
	file *ast.File
	path string // repository-relative, slash separated
}

// goPackage is the parsed non-test Go files of one directory.
type goPackage struct {
	dir   string // repository-relative, slash separated
	files []*goFile
	name  string
}

// parseGoPackage parses every non-test Go file directly inside dir, which is
// relative to root. It fails if the directory has no such files.
func parseGoPackage(root, dir string) (*goPackage, error) {
	entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(dir)))
	if err != nil {
		return nil, fmt.Errorf("read package directory %s: %w", dir, err)
	}

	pkg := &goPackage{dir: dir}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		relPath := path.Join(dir, name)
		file, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(relPath)), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", relPath, err)
		}
		if pkg.name == "" {
			pkg.name = file.Name.Name
		}
		pkg.files = append(pkg.files, &goFile{file: file, path: relPath})
	}
	if len(pkg.files) == 0 {
		return nil, fmt.Errorf("no Go files found in %s", dir)
	}
	return pkg, nil
}

// funcFile returns the file declaring top-level function name.
func (p *goPackage) funcFile(name string) (string, error) {
	for _, file := range p.files {
		for _, decl := range file.file.Decls {
			if funcDecl, ok := decl.(*ast.FuncDecl); ok && funcDecl.Recv == nil && funcDecl.Name.Name == name {
				return file.path, nil
			}
		}
	}
	return "", fmt.Errorf("function %s.%s not found in %s", p.name, name, p.dir)
}

// methodFile returns the file declaring method name on receiver type recv, or
// "" if the package does not declare it directly.
func (p *goPackage) methodFile(recv, name string) string {
	for _, file := range p.files {
		for _, decl := range file.file.Decls {
			funcDecl, ok := decl.(*ast.FuncDecl)
			if !ok || funcDecl.Recv == nil || funcDecl.Name.Name != name {
				continue
			}
			if receiverTypeName(funcDecl.Recv.List[0].Type) == recv {
				return file.path
			}
		}
	}
	return ""
}

// stringConsts returns every constant declared with a string literal value.
func (p *goPackage) stringConsts() []*stringConst {
	consts := make([]*stringConst, 0, len(p.files))
	for _, file := range p.files {
		consts = append(consts, fileStringConsts(p.name, file)...)
	}
	return consts
}

// typeSpec finds the declaration of type name.
func (p *goPackage) typeSpec(name string) (*ast.TypeSpec, *goFile, error) {
	for _, file := range p.files {
		for _, decl := range file.file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}
			for _, spec := range genDecl.Specs {
				if typeSpec := spec.(*ast.TypeSpec); typeSpec.Name.Name == name { //nolint:forcetypeassert // TYPE declarations only contain TypeSpecs
					return typeSpec, file, nil
				}
			}
		}
	}
	return nil, nil, fmt.Errorf("type %s.%s not found in %s", p.name, name, p.dir)
}

// stringConst is a constant declared with a string literal value.
type stringConst struct {
	name     string
	path     string
	pkg      string
	typeName string
	value    string
}

func fileStringConsts(pkgName string, file *goFile) []*stringConst {
	var consts []*stringConst
	for _, decl := range file.file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec := spec.(*ast.ValueSpec) //nolint:forcetypeassert // CONST declarations only contain ValueSpecs
			typeName := ""
			if ident, ok := valueSpec.Type.(*ast.Ident); ok {
				typeName = ident.Name
			}
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					continue
				}
				value, ok := stringLiteral(valueSpec.Values[i])
				if !ok {
					continue
				}
				consts = append(consts, &stringConst{
					name:     name.Name,
					path:     file.path,
					pkg:      pkgName,
					typeName: typeName,
					value:    value,
				})
			}
		}
	}
	return consts
}

// extractAll derives every inventory item from the repository at root. The
// result is sorted by ID and contains no duplicate IDs.
func extractAll(root string) ([]*extractedItem, error) {
	rootPkg, err := parseGoPackage(root, ".")
	if err != nil {
		return nil, err
	}

	extractors := []func() ([]*extractedItem, error){
		func() ([]*extractedItem, error) {
			return extractStructFields(rootPkg, "config", reflect.TypeFor[river.Config]())
		},
		func() ([]*extractedItem, error) {
			return extractStructFields(rootPkg, "insert_opts", reflect.TypeFor[river.InsertOpts]())
		},
		func() ([]*extractedItem, error) {
			return extractStructFields(rootPkg, "unique_opts", reflect.TypeFor[river.UniqueOpts]())
		},
		func() ([]*extractedItem, error) {
			return extractStructFields(rootPkg, "queue_config", reflect.TypeFor[river.QueueConfig]())
		},
		func() ([]*extractedItem, error) {
			return extractStructFields(rootPkg, "periodic_job_opts", reflect.TypeFor[river.PeriodicJobOpts]())
		},
		func() ([]*extractedItem, error) {
			return extractMethods(rootPkg, "client", reflect.TypeFor[*river.Client[pgx.Tx]]())
		},
		func() ([]*extractedItem, error) {
			return extractMethods(rootPkg, "job_list_params", reflect.TypeFor[*river.JobListParams]())
		},
		func() ([]*extractedItem, error) {
			return extractMethods(rootPkg, "job_delete_many_params", reflect.TypeFor[*river.JobDeleteManyParams]())
		},
		func() ([]*extractedItem, error) {
			return extractMethods(rootPkg, "queue_list_params", reflect.TypeFor[*river.QueueListParams]())
		},
		func() ([]*extractedItem, error) { return extractJobStates(root) },
		func() ([]*extractedItem, error) { return extractEventKinds(rootPkg) },
		func() ([]*extractedItem, error) { return extractMetadataKeys(root) },
		func() ([]*extractedItem, error) { return extractNotificationTopics(root) },
		func() ([]*extractedItem, error) { return extractNotificationPayloads(root, rootPkg) },
		func() ([]*extractedItem, error) { return extractDriverInterfaces(root) },
		func() ([]*extractedItem, error) { return extractExtensionInterfaces(root) },
		func() ([]*extractedItem, error) { return extractMigrations(root) },
	}

	var items []*extractedItem
	for _, extractor := range extractors {
		areaItems, err := extractor()
		if err != nil {
			return nil, err
		}
		if len(areaItems) == 0 {
			return nil, errors.New("an extractor produced no items; a source was probably renamed")
		}
		items = append(items, areaItems...)
	}

	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	for i := 1; i < len(items); i++ {
		if items[i].ID == items[i-1].ID {
			return nil, fmt.Errorf("duplicate extracted item ID %s (%s and %s)", items[i].ID, items[i-1].Source, items[i].Source)
		}
	}
	return items, nil
}

// extractStructFields returns one item per exported field of a struct type,
// located in pkg for the source reference.
func extractStructFields(pkg *goPackage, area string, structType reflect.Type) ([]*extractedItem, error) {
	_, file, err := pkg.typeSpec(structType.Name())
	if err != nil {
		return nil, err
	}

	var items []*extractedItem
	for field := range structType.Fields() {
		if !field.IsExported() {
			continue
		}
		items = append(items, &extractedItem{
			Area:   area,
			Detail: reflectTypeString(field.Type),
			ID:     area + "." + field.Name,
			Source: fmt.Sprintf("%s:%s.%s.%s", file.path, pkg.name, structType.Name(), field.Name),
		})
	}
	return items, nil
}

// extractMethods returns one item per exported method in the method set of a
// pointer type.
func extractMethods(pkg *goPackage, area string, ptrType reflect.Type) ([]*extractedItem, error) {
	elemName := genericBaseName(ptrType.Elem().Name())
	_, typeFile, err := pkg.typeSpec(elemName)
	if err != nil {
		return nil, err
	}

	items := make([]*extractedItem, 0, ptrType.NumMethod())
	for method := range ptrType.Methods() {
		file := pkg.methodFile(elemName, method.Name)
		if file == "" {
			file = typeFile.path
		}
		items = append(items, &extractedItem{
			Area:   area,
			Detail: reflectFuncSignature(method.Type, 1),
			ID:     area + "." + method.Name,
			Source: fmt.Sprintf("%s:%s.%s.%s", file, pkg.name, elemName, method.Name),
		})
	}
	return items, nil
}

func extractJobStates(root string) ([]*extractedItem, error) {
	pkg, err := parseGoPackage(root, "rivertype")
	if err != nil {
		return nil, err
	}
	if _, err := pkg.funcFile("JobStates"); err != nil {
		return nil, err
	}

	constsByValue := make(map[string]*stringConst)
	for _, constDecl := range pkg.stringConsts() {
		if constDecl.typeName == "JobState" {
			constsByValue[constDecl.value] = constDecl
		}
	}

	items := make([]*extractedItem, 0, len(rivertype.JobStates()))
	for _, state := range rivertype.JobStates() {
		constDecl, ok := constsByValue[string(state)]
		if !ok {
			return nil, fmt.Errorf("no rivertype.JobState constant declares %q", state)
		}
		items = append(items, &extractedItem{
			Area:   "job_state",
			Detail: constDecl.name,
			ID:     "job_state." + string(state),
			Source: fmt.Sprintf("%s:%s.%s", constDecl.path, pkg.name, constDecl.name),
		})
	}
	return items, nil
}

func extractEventKinds(rootPkg *goPackage) ([]*extractedItem, error) {
	return constItems(rootPkg, "event_kind", "event_kind.", func(constDecl *stringConst) bool {
		return constDecl.typeName == "EventKind" && strings.HasPrefix(constDecl.name, "EventKind")
	})
}

func extractNotificationTopics(root string) ([]*extractedItem, error) {
	pkg, err := parseGoPackage(root, "internal/notifier")
	if err != nil {
		return nil, err
	}
	return constItems(pkg, "notification_topic", "notification_topic.", func(constDecl *stringConst) bool {
		return constDecl.typeName == "NotificationTopic" && strings.HasPrefix(constDecl.name, "NotificationTopic")
	})
}

// constItems returns an item for every string constant in pkg matching
// include, identified by the constant's value.
func constItems(pkg *goPackage, area, idPrefix string, include func(*stringConst) bool) ([]*extractedItem, error) {
	var items []*extractedItem
	for _, constDecl := range pkg.stringConsts() {
		if !include(constDecl) {
			continue
		}
		items = append(items, &extractedItem{
			Area:   area,
			Detail: constDecl.name,
			ID:     idPrefix + constDecl.value,
			Source: fmt.Sprintf("%s:%s.%s", constDecl.path, pkg.name, constDecl.name),
		})
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("no %s constants found in %s", area, pkg.dir)
	}
	return items, nil
}

func extractNotificationPayloads(root string, rootPkg *goPackage) ([]*extractedItem, error) {
	leadershipPkg, err := parseGoPackage(root, "internal/leadership")
	if err != nil {
		return nil, err
	}

	var items []*extractedItem
	for _, payload := range []struct {
		id       string
		pkg      *goPackage
		typeName string
	}{
		{id: "notification_payload.control", pkg: rootPkg, typeName: "controlEventPayload"},
		{id: "notification_payload.insert", pkg: rootPkg, typeName: "insertPayload"},
		{id: "notification_payload.leadership", pkg: leadershipPkg, typeName: "DBNotification"},
	} {
		item, err := payloadStructItem(payload.pkg, payload.id, payload.typeName)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	controlActions, err := constItems(rootPkg, "notification_payload", "notification_payload.control.action.", func(constDecl *stringConst) bool {
		return constDecl.typeName == "controlAction"
	})
	if err != nil {
		return nil, err
	}
	leadershipActions, err := constItems(leadershipPkg, "notification_payload", "notification_payload.leadership.action.", func(constDecl *stringConst) bool {
		return constDecl.typeName == "DBNotificationKind"
	})
	if err != nil {
		return nil, err
	}
	sqlPayloads, err := extractSQLNotificationPayloads(root)
	if err != nil {
		return nil, err
	}

	items = append(items, controlActions...)
	items = append(items, leadershipActions...)
	return append(items, sqlPayloads...), nil
}

// payloadStructItem describes a JSON payload struct as its sorted JSON fields.
func payloadStructItem(pkg *goPackage, id, typeName string) (*extractedItem, error) {
	typeSpec, file, err := pkg.typeSpec(typeName)
	if err != nil {
		return nil, err
	}
	structType, ok := typeSpec.Type.(*ast.StructType)
	if !ok {
		return nil, fmt.Errorf("%s.%s is not a struct", pkg.name, typeName)
	}

	var fields []string
	for _, field := range structType.Fields.List {
		tagName, tagOptions := "", ""
		if field.Tag != nil {
			tag, err := strconv.Unquote(field.Tag.Value)
			if err != nil {
				return nil, fmt.Errorf("unquote tag in %s.%s: %w", pkg.name, typeName, err)
			}
			tagName, tagOptions, _ = strings.Cut(reflect.StructTag(tag).Get("json"), ",")
		}
		for _, name := range field.Names {
			if !name.IsExported() || tagName == "-" {
				continue
			}
			jsonName := tagName
			if jsonName == "" {
				jsonName = name.Name
			}
			description := jsonName + " " + types.ExprString(field.Type)
			if strings.Contains(","+tagOptions+",", ",omitempty,") {
				description += " omitempty"
			}
			fields = append(fields, description)
		}
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("%s.%s has no JSON fields", pkg.name, typeName)
	}
	sort.Strings(fields)

	return &extractedItem{
		Area:   "notification_payload",
		Detail: strings.Join(fields, "; "),
		ID:     id,
		Source: fmt.Sprintf("%s:%s.%s", file.path, pkg.name, typeName),
	}, nil
}

// extractDriverInterfaces returns one item per method or embedded interface
// of every exported interface in riverdriver.
func extractDriverInterfaces(root string) ([]*extractedItem, error) {
	pkg, err := parseGoPackage(root, "riverdriver")
	if err != nil {
		return nil, err
	}
	return interfaceItems(pkg, "driver", "driver.",
		[]string{"Driver", "Executor", "ExecutorJobCompletionConcurrency", "ExecutorTx", "Listener"},
		func(string) bool { return true },
	)
}

// extractExtensionInterfaces returns one item per method or embedded interface
// of the extension seam in riverpilot and the hook, middleware, and plugin
// interfaces in rivertype.
func extractExtensionInterfaces(root string) ([]*extractedItem, error) {
	pilotPkg, err := parseGoPackage(root, "rivershared/riverpilot")
	if err != nil {
		return nil, err
	}
	pilotItems, err := interfaceItems(pilotPkg, "extension", "extension.riverpilot.",
		[]string{"Pilot", "PilotJobCompletionConcurrency", "PilotJobRescuer", "PilotPeriodicJob"},
		func(string) bool { return true },
	)
	if err != nil {
		return nil, err
	}

	typePkg, err := parseGoPackage(root, "rivertype")
	if err != nil {
		return nil, err
	}
	typeItems, err := interfaceItems(typePkg, "extension", "extension.rivertype.",
		[]string{"Hook", "HookInsertBegin", "HookMetricEmit", "HookPeriodicJobsStart", "HookWorkBegin", "HookWorkEnd", "JobInsertMiddleware", "Middleware", "Plugin", "WorkerMiddleware"},
		func(name string) bool {
			return strings.HasPrefix(name, "Hook") || strings.HasSuffix(name, "Middleware") || name == "Plugin"
		},
	)
	if err != nil {
		return nil, err
	}

	return append(pilotItems, typeItems...), nil
}

// interfaceItems returns items for exported interfaces in pkg accepted by
// include. Every name in required must be present.
func interfaceItems(pkg *goPackage, area, idPrefix string, required []string, include func(string) bool) ([]*extractedItem, error) {
	var (
		found = make(map[string]struct{})
		items []*extractedItem
	)
	for _, file := range pkg.files {
		for _, decl := range file.file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}
			for _, spec := range genDecl.Specs {
				typeSpec := spec.(*ast.TypeSpec) //nolint:forcetypeassert // TYPE declarations only contain TypeSpecs
				interfaceType, ok := typeSpec.Type.(*ast.InterfaceType)
				if !ok || !typeSpec.Name.IsExported() || !include(typeSpec.Name.Name) {
					continue
				}
				found[typeSpec.Name.Name] = struct{}{}
				for _, method := range interfaceType.Methods.List {
					if len(method.Names) == 0 {
						embedded := types.ExprString(method.Type)
						items = append(items, &extractedItem{
							Area:   area,
							Detail: "embeds " + embedded,
							ID:     idPrefix + typeSpec.Name.Name + "." + embedded,
							Source: fmt.Sprintf("%s:%s.%s", file.path, pkg.name, typeSpec.Name.Name),
						})
						continue
					}
					for _, name := range method.Names {
						items = append(items, &extractedItem{
							Area:   area,
							Detail: types.ExprString(method.Type),
							ID:     idPrefix + typeSpec.Name.Name + "." + name.Name,
							Source: fmt.Sprintf("%s:%s.%s.%s", file.path, pkg.name, typeSpec.Name.Name, name.Name),
						})
					}
				}
			}
		}
	}
	for _, name := range required {
		if _, ok := found[name]; !ok {
			return nil, fmt.Errorf("interface %s.%s not found in %s", pkg.name, name, pkg.dir)
		}
	}
	return items, nil
}

var migrationFilePattern = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

// extractMigrations returns one item per main-line migration version for each
// backend, with a content digest so edits to a shipped migration are visible.
func extractMigrations(root string) ([]*extractedItem, error) {
	var items []*extractedItem
	for _, backend := range []struct {
		dir  string
		name string
	}{
		{dir: "riverdriver/riverpgxv5/migration/main", name: "postgres"},
		{dir: "riverdriver/riversqlite/migration/main", name: "sqlite"},
	} {
		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(backend.dir)))
		if err != nil {
			return nil, fmt.Errorf("read migrations %s: %w", backend.dir, err)
		}

		type migration struct {
			digests map[string]string
			name    string
		}
		migrations := make(map[string]*migration)
		for _, entry := range entries {
			match := migrationFilePattern.FindStringSubmatch(entry.Name())
			if entry.IsDir() || match == nil {
				continue
			}
			version, name, direction := match[1], match[2], match[3]
			contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(backend.dir), entry.Name()))
			if err != nil {
				return nil, fmt.Errorf("read migration %s/%s: %w", backend.dir, entry.Name(), err)
			}
			current, ok := migrations[version]
			if !ok {
				current = &migration{digests: make(map[string]string), name: name}
				migrations[version] = current
			}
			if current.name != name {
				return nil, fmt.Errorf("migration %s/%s has conflicting names %q and %q", backend.dir, version, current.name, name)
			}
			digest := sha256.Sum256(contents)
			current.digests[direction] = hex.EncodeToString(digest[:])[:12]
		}
		if len(migrations) == 0 {
			return nil, fmt.Errorf("no migrations found in %s", backend.dir)
		}

		for _, version := range sortedKeys(migrations) {
			current := migrations[version]
			var detail strings.Builder
			detail.WriteString(current.name)
			for _, direction := range []string{"up", "down"} {
				digest, ok := current.digests[direction]
				if !ok {
					return nil, fmt.Errorf("migration %s/%s_%s is missing its %s file", backend.dir, version, current.name, direction)
				}
				detail.WriteString(" " + direction + ":" + digest)
			}
			items = append(items, &extractedItem{
				Area:   "migration",
				Detail: detail.String(),
				ID:     "migration." + backend.name + "." + version,
				Source: backend.dir + "/" + version + "_" + current.name + ".{up,down}.sql",
			})
		}
	}
	return items, nil
}

// genericBaseName strips instantiation arguments from a reflected type name.
func genericBaseName(name string) string {
	base, _, _ := strings.Cut(name, "[")
	return base
}

// receiverTypeName returns the base type name of a method receiver.
func receiverTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(typed.X)
	case *ast.IndexExpr:
		return receiverTypeName(typed.X)
	case *ast.IndexListExpr:
		return receiverTypeName(typed.X)
	case *ast.Ident:
		return typed.Name
	}
	return ""
}

// reflectFuncSignature renders a function type, skipping the first skip
// parameters (such as a method receiver).
func reflectFuncSignature(funcType reflect.Type, skip int) string {
	params := make([]string, 0, funcType.NumIn())
	for i := skip; i < funcType.NumIn(); i++ {
		if funcType.IsVariadic() && i == funcType.NumIn()-1 {
			params = append(params, "..."+reflectTypeString(funcType.In(i).Elem()))
			continue
		}
		params = append(params, reflectTypeString(funcType.In(i)))
	}
	results := make([]string, 0, funcType.NumOut())
	for out := range funcType.Outs() {
		results = append(results, reflectTypeString(out))
	}

	signature := "func(" + strings.Join(params, ", ") + ")"
	switch len(results) {
	case 0:
	case 1:
		signature += " " + results[0]
	default:
		signature += " (" + strings.Join(results, ", ") + ")"
	}
	return signature
}

// reflectTypeString renders a reflected type, replacing the transaction type
// used to instantiate generic River types with its type parameter name.
func reflectTypeString(typ reflect.Type) string {
	return strings.NewReplacer("github.com/jackc/pgx/v5.Tx", "TTx", "pgx.Tx", "TTx").Replace(typ.String())
}

// stringLiteral returns the value of a Go string literal expression.
func stringLiteral(expr ast.Expr) (string, bool) {
	basicLit, ok := expr.(*ast.BasicLit)
	if !ok || basicLit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(basicLit.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

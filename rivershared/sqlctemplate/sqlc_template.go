// Package sqlctemplate provides a way of making arbitrary text replacement in
// sqlc queries which normally only allow parameters which are in places valid
// in a prepared statement. For example, it can be used to insert a schema name
// as a prefix to tables referenced in sqlc, which is otherwise impossible.
//
// Replacement is carried out from within invocations of sqlc's generated DBTX
// interface, after sqlc generated code runs, but before queries are executed.
// This is accomplished by implementing DBTX, calling Replacer.Run from within
// them, and injecting parameters in with WithReplacements (which is unfortunately
// the only way of injecting them).
//
// Templates are modeled as SQL comments so that they're still parseable as
// valid SQL. An example use of the basic /* TEMPLATE ... */ syntax:
//
//	-- name: JobCountByState :one
//	SELECT count(*)
//	FROM /* TEMPLATE: schema */river_job
//	WHERE state = @state;
//
// An open/close syntax is also available for when SQL is required before
// processing for the query to be valid. For example, a WHERE or ORDER BY clause
// can't be empty, so the SQL includes a sentinel value that's parseable which
// is then replaced later with template values:
//
//	-- name: JobList :many
//	SELECT *
//	FROM river_job
//	WHERE /* TEMPLATE_BEGIN: where_clause */ 1 /* TEMPLATE_END */
//	ORDER BY /* TEMPLATE_BEGIN: order_by_clause */ id /* TEMPLATE_END */
//	LIMIT @max::int;
//
// Be careful not to place a template on a line by itself because sqlc will
// strip any lines that start with a comment. For example, this does NOT work:
//
//	-- name: JobList :many
//	SELECT *
//	FROM river_job
//	/* TEMPLATE_BEGIN: where_clause */
//	LIMIT @max::int;
package sqlctemplate

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/riverqueue/river/rivershared/util/maputil"
)

type contextContainer struct {
	NamedArgs map[string]any
	Templates map[string]Replacement
}

type contextKey struct{}

// Replacement defines a replacement for a template value in some input SQL.
type Replacement struct {
	// Stable is whether the replacement value is expected to be stable for any
	// number of times Replacer.Run is called with the same given input SQL. If
	// all replacements are stable, then the output of Replacer.Run is cached so
	// that it doesn't have to be processed again. Replacements should be not be
	// stable if they depend on input parameters.
	Stable bool

	// Value is the value which the template should be replaced with. For a /*
	// TEMPLATE ... */ tag, replaces template and the comment containing it. For
	// a /* TEMPLATE_BEGIN ... */ ... /* TEMPLATE_END */ tag pair, replaces both
	// templates, comments, and the value between them.
	Value string
}

// Replacer replaces templates with template values. As an optimization, it
// contains an internal cache to short circuit SQL that has entirely stable
// template replacements and whose output is invariant of input parameters.
//
// The struct is written so that it's safe to use as a value and doesn't need to
// be initialized with a constructor. This lets it default to a usable instance
// on drivers that may themselves not be initialized.
type Replacer struct {
	cache   map[string]string
	cacheMu sync.RWMutex
}

var (
	templateBeginEndRE = regexp.MustCompile(`/\* TEMPLATE_BEGIN: (.*?) \*/ .*? /\* TEMPLATE_END \*/`)
	templateRE         = regexp.MustCompile(`/\* TEMPLATE: (.*?) \*/`)
)

// Run replaces any tempates in input SQL with values from context added via
// WithReplacements.
//
// args aren't used for replacements in the input SQL, but are needed to
// determine which placeholder number (e.g. $1, $2, $3, ...) we should start
// with to replace any template named args. The returned args value should then
// be used as query input as named args from context may have been added to it.
func (r *Replacer) Run(ctx context.Context, sql string, args []any) (string, []any) {
	sql, namedArgs, err := r.RunSafely(ctx, sql, args)
	if err != nil {
		panic(err)
	}
	return sql, namedArgs
}

// RunSafely is the same as Run, but returns an error in case of missing or
// extra templates.
func (r *Replacer) RunSafely(ctx context.Context, sql string, args []any) (string, []any, error) {
	// If nothing present in context, short circuit quickly.
	container, containerOK := ctx.Value(contextKey{}).(*contextContainer)
	if !containerOK {
		return sql, args, nil
	}

	r.cacheMu.RLock()
	var (
		cachedSQL   string
		cachedSQLOK bool
	)
	if r.cache != nil { // protect against map not initialized yet
		cachedSQL, cachedSQLOK = r.cache[sql]
	}
	r.cacheMu.RUnlock()

	// If all input templates were stable, the finished SQL will have been
	if cachedSQLOK {
		if len(container.NamedArgs) > 0 {
			args = append(args, maputil.Values(container.NamedArgs)...)
		}
		return cachedSQL, args, nil
	}

	if !strings.Contains(sql, "/* TEMPLATE") {
		return sql, args, nil
	}

	var (
		templatesExpected = maputil.Keys(container.Templates)
		templatesMissing  []string // not preallocated because we don't expect any missing parameters in the common case
	)

	replaceTemplate := func(sql string, templateRE *regexp.Regexp) string {
		return templateRE.ReplaceAllStringFunc(sql, func(templateStr string) string {
			// Really dumb, but Go doesn't provide any way to get submatches in a
			// function, so we have to match twice.
			//     https://github.com/golang/go/issues/5690
			matches := templateRE.FindStringSubmatch(templateStr)

			template := matches[1]

			if tmpl, ok := container.Templates[template]; ok {
				templatesExpected = slices.DeleteFunc(templatesExpected, func(p string) bool { return p == template })
				return tmpl.Value
			} else {
				templatesMissing = append(templatesMissing, template)
			}

			return templateStr
		})
	}

	updatedSQL := sql
	updatedSQL = replaceTemplate(updatedSQL, templateBeginEndRE)
	updatedSQL = replaceTemplate(updatedSQL, templateRE)

	if len(templatesExpected) > 0 {
		return "", nil, errors.New("sqlctemplate params present in context but missing in SQL: " + strings.Join(templatesExpected, ", "))
	}

	if len(templatesMissing) > 0 {
		return "", nil, errors.New("sqlctemplate params present in SQL but missing in context: " + strings.Join(templatesMissing, ", "))
	}

	if len(container.NamedArgs) > 0 {
		placeholderNum := len(args)
		for arg, val := range container.NamedArgs {
			placeholderNum++

			var (
				symbol      = "@" + arg
				symbolIndex = strings.Index(updatedSQL, symbol)
			)

			if symbolIndex == -1 {
				return "", nil, fmt.Errorf("sqltemplate expected to find named arg %q, but it wasn't present", symbol)
			}

			// ReplaceAll because an input parameter may appear multiple times.
			updatedSQL = strings.ReplaceAll(updatedSQL, symbol, "$"+strconv.Itoa(placeholderNum))
			args = append(args, val)
		}
	}

	for _, tmpl := range container.Templates {
		if !tmpl.Stable {
			return updatedSQL, args, nil
		}
	}

	r.cacheMu.Lock()
	if r.cache == nil {
		r.cache = make(map[string]string)
	}
	r.cache[sql] = updatedSQL
	r.cacheMu.Unlock()

	return updatedSQL, args, nil
}

// WithReplacements adds sqlctemplate templates to the given context (they go in
// context because it's the only way to get them down into the innards of sqlc).
// namedArgs can also be passed in to replace arguments found in
//
// If sqlctemplate params are already present in context, the two sets are
// merged, with the new params taking precedent.
func WithReplacements(ctx context.Context, templates map[string]Replacement, namedArgs map[string]any) context.Context {
	if container, ok := ctx.Value(contextKey{}).(*contextContainer); ok {
		for arg, val := range namedArgs {
			container.NamedArgs[arg] = val
		}
		for template, tmpl := range templates {
			container.Templates[template] = tmpl
		}
		return ctx
	}

	if namedArgs == nil {
		namedArgs = make(map[string]any)
	}

	return context.WithValue(ctx, contextKey{}, &contextContainer{
		NamedArgs: namedArgs,
		Templates: templates,
	})
}

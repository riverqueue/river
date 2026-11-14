// Command riverconformanceadapter is River Go's conformance adapter: the
// reference implementation of the contract in package protocol, which the
// harness runs against other implementations' adapters. One handler serves
// both Postgres and SQLite through River's generic client.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river/conformance/protocol"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
)

func main() {
	if err := run(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "River Go conformance adapter:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, input io.Reader, output io.Writer) error {
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("RIVER_CONFORMANCE_DATABASE_URL is required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	switch driver := os.Getenv("RIVER_CONFORMANCE_DRIVER"); driver {
	case "postgres":
		poolConfig, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			return fmt.Errorf("error parsing database URL: %w", err)
		}
		if name := os.Getenv("RIVER_CONFORMANCE_APPLICATION_NAME"); name != "" {
			poolConfig.ConnConfig.RuntimeParams["application_name"] = name
		}
		poolConfig.MaxConns = 10
		// Fault scenarios terminate this adapter's backends while they sit
		// idle in the pool. Checking liveness on acquire keeps a terminated
		// connection from failing the next request.
		poolConfig.ShouldPing = func(context.Context, pgxpool.ShouldPingParams) bool { return true }

		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("error opening database: %w", err)
		}
		defer pool.Close()

		return serve(ctx, input, output, newServer(driver, riverpgxv5.New(pool), logger, &txFuncs[pgx.Tx]{
			begin:    func(ctx context.Context) (pgx.Tx, error) { return pool.Begin(ctx) },
			commit:   func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) },
			rollback: func(ctx context.Context, tx pgx.Tx) error { return tx.Rollback(ctx) },
		}))

	case "sqlite":
		// Pragmas go in the DSN so every pooled connection gets them. The busy
		// timeout comes first because another adapter may be switching the
		// same new database to WAL at the same moment.
		separator := "?"
		if strings.Contains(databaseURL, "?") {
			separator = "&"
		}
		db, err := sql.Open("sqlite", databaseURL+separator+
			"_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
		if err != nil {
			return fmt.Errorf("error opening database: %w", err)
		}
		defer db.Close()
		db.SetMaxOpenConns(1)

		return serve(ctx, input, output, newServer(driver, riversqlite.New(db), logger, &txFuncs[*sql.Tx]{
			begin:    func(ctx context.Context) (*sql.Tx, error) { return db.BeginTx(ctx, nil) },
			commit:   func(ctx context.Context, tx *sql.Tx) error { return tx.Commit() },
			rollback: func(ctx context.Context, tx *sql.Tx) error { return tx.Rollback() },
		}))

	default:
		return fmt.Errorf("unsupported RIVER_CONFORMANCE_DRIVER %q", driver)
	}
}

// handler handles one decoded request.
type handler interface {
	handle(ctx context.Context, method string, params json.RawMessage) (any, error)
	shutdown(ctx context.Context)
}

// serve answers requests from in, one per line, until in closes.
func serve(ctx context.Context, input io.Reader, output io.Writer, handler handler) error {
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		handler.shutdown(ctx)
	}()

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)

	for scanner.Scan() {
		response := respond(ctx, handler, scanner.Bytes())
		if err := encoder.Encode(response); err != nil {
			return fmt.Errorf("error writing response: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading requests: %w", err)
	}
	return nil
}

func respond(ctx context.Context, handler handler, line []byte) *protocol.Response {
	response := &protocol.Response{JSONRPC: "2.0"}

	var request protocol.Request
	if err := json.Unmarshal(line, &request); err != nil {
		response.Error = &protocol.Error{Code: protocol.CodeParseError, Message: err.Error()}
		return response
	}
	response.ID = request.ID
	if request.JSONRPC != "2.0" {
		response.Error = &protocol.Error{Code: protocol.CodeInvalidRequest, Message: "jsonrpc must be 2.0"}
		return response
	}

	result, err := handler.handle(ctx, request.Method, request.Params)
	if err != nil {
		response.Error = toProtocolError(err)
		return response
	}

	if result == nil {
		result = struct{}{}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		response.Error = &protocol.Error{Code: protocol.CodeInternal, Message: err.Error()}
		return response
	}
	response.Result = encoded
	return response
}

// codedError is an error with a protocol error code.
type codedError struct {
	code int
	err  error
}

func (e *codedError) Error() string { return e.err.Error() }

func (e *codedError) Unwrap() error { return e.err }

func invalidParams(err error) error { return &codedError{code: protocol.CodeInvalidParams, err: err} }

func notFound(err error) error { return &codedError{code: protocol.CodeNotFound, err: err} }

// toProtocolError maps an error to its protocol error. Errors without a code
// come from River, which rejected the request or failed to complete it.
func toProtocolError(err error) *protocol.Error {
	if coded, ok := errors.AsType[*codedError](err); ok {
		return &protocol.Error{Code: coded.code, Message: err.Error()}
	}
	return &protocol.Error{Code: protocol.CodeRejected, Message: err.Error()}
}

// decodeParams decodes params strictly: unknown fields are invalid.
func decodeParams(params json.RawMessage, target any) error {
	if len(params) == 0 || string(params) == "null" {
		params = json.RawMessage("{}")
	}
	decoder := json.NewDecoder(strings.NewReader(string(params)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return invalidParams(err)
	}
	return nil
}

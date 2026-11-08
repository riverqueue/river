package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river/riverdriver/rivermysql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
)

func runDatabase(ctx context.Context, backend string, cfg config, sample *trial) error {
	// Only drop objects successfully created by this invocation. Never truncate
	// a caller's tables, even when using a shared local development server.
	name := "river_bench_" + strings.ToLower(rand.Text())
	switch backend {
	case "mysql":
		return runMySQL(ctx, cfg, name, sample)
	case "postgres":
		return runPostgres(ctx, cfg, name, sample)
	case "sqlite":
		return runSQLite(ctx, cfg, sample)
	default:
		return fmt.Errorf("unknown backend: %s", backend)
	}
}

func runMySQL(ctx context.Context, cfg config, name string, sample *trial) (err error) {
	connConfig, err := mysql.ParseDSN(cmp.Or(os.Getenv("RIVER_BENCH_MYSQL_DSN"), "root@tcp(127.0.0.1:3306)/"))
	if err != nil {
		return err
	}
	connConfig.DBName = ""
	connConfig.Loc = time.UTC
	connConfig.MultiStatements = true
	connConfig.ParseTime = true
	if connConfig.Params == nil {
		connConfig.Params = make(map[string]string)
	}
	connConfig.Params["time_zone"] = "'+00:00'"
	connector, err := mysql.NewConnector(connConfig)
	if err != nil {
		return err
	}
	admin := sql.OpenDB(connector)
	defer func() { err = errors.Join(err, admin.Close()) }()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, dropErr := admin.ExecContext(cleanupCtx, "DROP DATABASE `"+name+"`")
		err = errors.Join(err, dropErr)
	}()
	connConfig.DBName = name
	connector, err = mysql.NewConnector(connConfig)
	if err != nil {
		return err
	}
	pool := sql.OpenDB(connector)
	defer func() { err = errors.Join(err, pool.Close()) }()
	pool.SetMaxOpenConns(cfg.Connections)
	pool.SetMaxIdleConns(cfg.Connections)
	sample.Settings, err = readSettings(pool.QueryRowContext(ctx,
		"SELECT VERSION(), @@innodb_flush_log_at_trx_commit, @@sync_binlog, @@log_bin, @@innodb_buffer_pool_size"),
		"version", "innodb_flush_log_at_trx_commit", "sync_binlog", "log_bin", "innodb_buffer_pool_size")
	if err != nil {
		return err
	}
	sample.Settings["interpolate_params"] = strconv.FormatBool(connConfig.InterpolateParams)
	return measure(ctx, rivermysql.New(pool), "", cfg, sample)
}

func runPostgres(ctx context.Context, cfg config, name string, sample *trial) (err error) {
	url := cmp.Or(os.Getenv("RIVER_BENCH_POSTGRES_URL"), "postgres://localhost/postgres")
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		err = errors.Join(err, admin.Close(cleanupCtx))
	}()
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, dropErr := admin.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE")
		err = errors.Join(err, dropErr)
	}()
	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}
	poolConfig.MaxConns = int32(cfg.Connections) //nolint:gosec // Small, user-selected benchmark pool size.
	poolConfig.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()
	sample.Settings, err = readSettings(pool.QueryRow(ctx,
		"SELECT current_setting('server_version'), current_setting('fsync'), current_setting('synchronous_commit'), current_setting('full_page_writes'), current_setting('shared_buffers')"),
		"version", "fsync", "synchronous_commit", "full_page_writes", "shared_buffers")
	if err != nil {
		return err
	}
	return measure(ctx, riverpgxv5.New(pool), name, cfg, sample)
}

func runSQLite(ctx context.Context, cfg config, sample *trial) (err error) {
	dir, err := os.MkdirTemp("", "river-bench-sqlite-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	pool, err := sql.Open("sqlite", filepath.Join(dir, "bench.sqlite3"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, pool.Close()) }()
	pool.SetMaxOpenConns(1)
	if _, err := pool.ExecContext(ctx, "PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000;"); err != nil {
		return err
	}
	sample.Settings, err = readSettings(pool.QueryRowContext(ctx,
		"SELECT sqlite_version(), (SELECT journal_mode FROM pragma_journal_mode), (SELECT synchronous FROM pragma_synchronous), (SELECT cache_size FROM pragma_cache_size), (SELECT page_size FROM pragma_page_size), (SELECT fullfsync FROM pragma_fullfsync)"),
		"version", "journal_mode", "synchronous", "cache_size", "page_size", "fullfsync")
	if err != nil {
		return err
	}
	return measure(ctx, riversqlite.New(pool), "", cfg, sample)
}

func readSettings(row interface{ Scan(dest ...any) error }, names ...string) (map[string]string, error) {
	values := make([]string, len(names))
	args := make([]any, len(names))
	for i := range values {
		args[i] = &values[i]
	}
	if err := row.Scan(args...); err != nil {
		return nil, err
	}
	settings := make(map[string]string, len(names))
	for i, name := range names {
		settings[name] = values[i]
	}
	return settings, nil
}

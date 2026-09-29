//go:build foundationdb

package riverfdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

var _ riverdriver.Driver[fdb.Transaction] = (*Driver)(nil)

// Driver implements River's driver interface using FoundationDB transactions.
type Driver struct {
	db     fdb.Database
	prefix []byte
}

// New returns a driver using an open database and an exclusive key prefix.
// The prefix is copied and must be nonempty, at most 1,024 bytes, and outside
// FoundationDB's system keyspace. The caller owns the database's lifetime.
func New(db fdb.Database, prefix []byte) (*Driver, error) {
	if db == (fdb.Database{}) {
		return nil, errors.New("riverfdb: database must be open")
	}
	if len(prefix) == 0 || len(prefix) > 1024 || prefix[0] == 0xff {
		return nil, errors.New("riverfdb: prefix must contain 1–1024 bytes and not start with 0xff")
	}
	return &Driver{db: db, prefix: bytes.Clone(prefix)}, nil
}

func (d *Driver) ArgPlaceholder() string { return "" }
func (d *Driver) DatabaseName() string   { return riverdriver.DatabaseNameFoundationDB }

func (d *Driver) GetExecutor() riverdriver.Executor { return &executor{driver: d} }
func (d *Driver) GetListener(params *riverdriver.GetListenenerParams) riverdriver.Listener {
	return &Listener{driver: d, schema: params.Schema}
}
func (d *Driver) GetMigrationDefaultLines() []string              { return nil }
func (d *Driver) GetMigrationFS(string) fs.FS                     { return nil }
func (d *Driver) GetMigrationLines() []string                     { return nil }
func (d *Driver) GetMigrationTruncateTables(string, int) []string { return nil }
func (d *Driver) PoolIsSet() bool                                 { return true }
func (d *Driver) PoolSet(any) error                               { return riverdriver.ErrNotImplemented }

func (d *Driver) SQLFragmentColumnContainsAll(string, string, []string) (string, any, error) {
	return "", nil, unsupported("SQL job filters")
}

func (d *Driver) SQLFragmentColumnContainsAny(string, string, []string) (string, any, error) {
	return "", nil, unsupported("SQL job filters")
}

func (d *Driver) SQLFragmentColumnIn(string, any) (string, any, error) {
	return "", nil, unsupported("SQL job filters")
}
func (d *Driver) SupportsListener() bool       { return true }
func (d *Driver) SupportsListenNotify() bool   { return true }
func (d *Driver) TimePrecision() time.Duration { return time.Nanosecond }
func (d *Driver) UnwrapExecutor(tx fdb.Transaction) riverdriver.ExecutorTx {
	return &executorTx{executor: executor{driver: d, tx: &tx}}
}

func (d *Driver) UnwrapTx(execTx riverdriver.ExecutorTx) fdb.Transaction {
	return *execTx.(*executorTx).tx //nolint:forcetypeassert
}

func (d *Driver) key(schema string, parts ...tuple.TupleElement) fdb.Key {
	return append(bytes.Clone(d.prefix), append(tuple.Tuple{1, schema}, parts...).Pack()...)
}

type executor struct {
	unsupportedExecutor

	driver *Driver
	tx     *fdb.Transaction
}

func (e *executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if e.tx != nil {
		return nil, unsupported("nested transactions")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := e.driver.db.CreateTransaction()
	if err != nil {
		return nil, err
	}
	return &executorTx{
		executor:       executor{driver: e.driver, tx: &tx},
		stopCancelFunc: context.AfterFunc(ctx, tx.Cancel),
	}, nil
}

func (e *executor) InitDriver(ctx context.Context) error { return e.Ping(ctx) }

func (e *executor) Ping(ctx context.Context) error {
	_, err := transact(ctx, e, func(tx fdb.Transaction) (int64, error) {
		return tx.GetReadVersion().Get()
	})
	return err
}

type executorTx struct {
	executor

	stopCancelFunc func() bool
}

func (e *executorTx) Commit(ctx context.Context) error {
	if e.stopCancelFunc != nil {
		defer e.stopCancelFunc()
	}
	if err := ctx.Err(); err != nil {
		e.tx.Cancel()
		return err
	}
	stopCancelFunc := context.AfterFunc(ctx, e.tx.Cancel)
	defer stopCancelFunc()
	err := e.tx.Commit().Get()
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (e *executorTx) Rollback(ctx context.Context) error {
	if e.stopCancelFunc != nil {
		e.stopCancelFunc()
	}
	e.tx.Cancel()
	return nil
}

// transact only replays operations whose entire transaction belongs to River.
// User transactions and Begin/Commit transactions must be retried by their owner.
func transact[T any](ctx context.Context, exec *executor, operationFunc func(fdb.Transaction) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if exec.tx != nil {
		stopCancelFunc := context.AfterFunc(ctx, exec.tx.Cancel)
		defer stopCancelFunc()
		result, err := operationFunc(*exec.tx)
		if err != nil {
			// Match an SQL transaction's failure semantics: a failed batch
			// must not leave partial mutations that its caller can commit.
			if !errors.Is(err, rivertype.ErrNotFound) {
				exec.tx.Cancel()
			}
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return zero, err
		}
		return result, nil
	}
	tx, err := exec.driver.db.CreateTransaction()
	if err != nil {
		return zero, err
	}
	defer tx.Cancel()
	stopCancelFunc := context.AfterFunc(ctx, tx.Cancel)
	defer stopCancelFunc()
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		result, err := operationFunc(tx)
		if err == nil {
			err = tx.Commit().Get()
		}
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		var fdbErr fdb.Error
		if !errors.As(err, &fdbErr) {
			return zero, err
		}
		// A lost commit response may mean this operation succeeded. Replaying
		// an insert or claim could affect a second job; surface the ambiguity
		// to the caller instead. Conflict errors remain safe to retry.
		if fdbErr.Code == 1021 { // commit_unknown_result
			return zero, err
		}
		retry := tx.OnError(fdbErr)
		stopRetryCancelFunc := context.AfterFunc(ctx, retry.Cancel)
		err = retry.Get()
		stopRetryCancelFunc()
		if err != nil {
			if ctx.Err() != nil {
				return zero, ctx.Err()
			}
			return zero, err
		}
	}
}

func unsupported(operation string) error {
	return fmt.Errorf("riverfdb: %s: %w", operation, riverdriver.ErrNotImplemented)
}

package sharedtx

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SharedTx can be used to wrap a test transaction in cases where multiple
// callers may want to access it concurrently during the course of a single test
// case. Normally this is not allowed and an access will error with "conn busy"
// if another caller is already using it.
//
// This is a test-only construct because in non-test environments an executor is
// a full connection pool which can support concurrent access without trouble.
// Many test cases use single test transactions, and that's where code written
// to use a connection pool can become problematic.
//
// SharedTx uses a channel for synchronization and does *not* guarantee FIFO
// ordering for callers.
//
// Avoid using SharedTx if possible because while it works, problems
// encountered by use of concurrent accesses will be more difficult to debug
// than otherwise, so it's better to not go there at all if it can be avoided.
type SharedTx struct {
	inner pgx.Tx
	wait  chan struct{}
}

func NewSharedTx(tx pgx.Tx) *SharedTx {
	wait := make(chan struct{}, 1)

	// Send one initial signal in so that the first caller is able to acquire a
	// lock.
	wait <- struct{}{}

	return &SharedTx{
		inner: tx,
		wait:  wait,
	}
}

func (e *SharedTx) Begin(ctx context.Context) (pgx.Tx, error) {
	e.lock()
	// no unlock until transaction commit/rollback

	tx, err := e.inner.Begin(ctx)
	if err != nil {
		e.unlock()
		return nil, err
	}

	// shared tx is unlocked when the transaction is committed or rolled back
	return &SharedSubTx{sharedTxDerivative{sharedTx: e}, tx}, nil
}

func (e *SharedTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	e.lock()
	defer e.unlock()

	return e.inner.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (e *SharedTx) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	e.lock()
	defer e.unlock()

	return e.inner.Exec(ctx, query, args...)
}

func (e *SharedTx) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	e.lock()
	// no unlock until rows close or return on error condition

	rows, err := e.inner.Query(ctx, query, args...)
	if err != nil {
		e.unlock()
		return nil, err
	}

	// executor is unlocked when rows are closed
	return &SharedTxRows{sharedTxDerivative{sharedTx: e}, rows}, nil
}

func (e *SharedTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e.lock()
	// no unlock until row scan

	row := e.inner.QueryRow(ctx, query, args...)

	// executor is unlocked when row is scanned
	return &SharedTxRow{sharedTxDerivative{sharedTx: e}, row}
}

//
// These are implemented so SharedTx can satisfy pgx.Tx.
//
// Conn intentionally returns nil (instead of panicking) because some callers
// perform capability/config probes through Conn() and can safely handle nil.
// SharedTx does not expose a stable underlying conn pointer, so nil is the
// correct "unavailable" signal for probes.
//
// The rest stay panic-only because they are behavioral operations that should
// not be used on SharedTx directly.
//

func (e *SharedTx) Conn() *pgx.Conn                  { return nil }
func (e *SharedTx) Commit(ctx context.Context) error { panic("not implemented") }
func (e *SharedTx) LargeObjects() pgx.LargeObjects   { panic("not implemented") }
func (e *SharedTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	panic("not implemented")
}
func (e *SharedTx) Rollback(ctx context.Context) error { panic("not implemented") }
func (e *SharedTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	panic("not implemented")
}

func (e *SharedTx) lock() {
	select {
	case <-e.wait:
		// success

	case <-time.After(5 * time.Second):
		panic("sharedtx: Timed out trying to acquire lock on SharedTx")
	}
}

func (e *SharedTx) unlock() {
	select {
	case e.wait <- struct{}{}:
	default:
		panic("wait channel was already unlocked which should not happen; BUG!")
	}
}

type sharedTxDerivative struct {
	once     sync.Once // used to guarantee executor only unlocked once
	sharedTx *SharedTx
}

func (d *sharedTxDerivative) unlockParent() {
	d.once.Do(func() {
		d.sharedTx.unlock()
	})
}

// SharedTxRow wraps a pgx.Row such that it unlocks SharedExecutor when
// the row finishes scanning.
type SharedTxRow struct {
	sharedTxDerivative

	innerRow pgx.Row
}

func (r *SharedTxRow) Scan(dest ...any) error {
	defer r.unlockParent()
	return r.innerRow.Scan(dest...)
}

// SharedTxRows wraps a pgx.Rows such that it unlocks SharedExecutor when
// the rows are closed.
type SharedTxRows struct {
	sharedTxDerivative

	innerRows pgx.Rows
}

func (r *SharedTxRows) Close() {
	defer r.unlockParent()
	r.innerRows.Close()
}

//
// All of these are simple pass throughs.
//

func (r *SharedTxRows) CommandTag() pgconn.CommandTag { return r.innerRows.CommandTag() }
func (r *SharedTxRows) Conn() *pgx.Conn               { return nil }
func (r *SharedTxRows) Err() error                    { return r.innerRows.Err() }
func (r *SharedTxRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.innerRows.FieldDescriptions()
}
func (r *SharedTxRows) Next() bool             { return r.innerRows.Next() }
func (r *SharedTxRows) RawValues() [][]byte    { return r.innerRows.RawValues() }
func (r *SharedTxRows) Scan(dest ...any) error { return r.innerRows.Scan(dest...) }
func (r *SharedTxRows) Values() ([]any, error) { return r.innerRows.Values() }

// SharedSubTx wraps a pgx.Tx such that it unlocks SharedTx when it commits or
// rolls back.
type SharedSubTx struct {
	sharedTxDerivative

	inner pgx.Tx
}

func (tx *SharedSubTx) Begin(ctx context.Context) (pgx.Tx, error) {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.Begin(ctx)
}

func (tx *SharedSubTx) Conn() *pgx.Conn {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.Conn()
}

func (tx *SharedSubTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (tx *SharedSubTx) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.Exec(ctx, query, args...)
}

func (tx *SharedSubTx) LargeObjects() pgx.LargeObjects {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.LargeObjects()
}

func (tx *SharedSubTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.Prepare(ctx, name, sql)
}

func (tx *SharedSubTx) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.Query(ctx, query, args...)
}

func (tx *SharedSubTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.QueryRow(ctx, query, args...)
}

func (tx *SharedSubTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	// pass through because we're already holding the lock at the executor level
	return tx.inner.SendBatch(ctx, b)
}

func (tx *SharedSubTx) Commit(ctx context.Context) error {
	defer tx.unlockParent()
	return tx.inner.Commit(ctx)
}

func (tx *SharedSubTx) Rollback(ctx context.Context) error {
	defer tx.unlockParent()
	return tx.inner.Rollback(ctx)
}

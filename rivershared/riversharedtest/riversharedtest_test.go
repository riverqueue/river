package riversharedtest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestDBPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	pool1 := DBPool(ctx, t)
	_, err := pool1.Exec(ctx, "SELECT 1")
	require.NoError(t, err)

	// Both pools should be exactly the same object.
	pool2 := DBPool(ctx, t)
	require.Equal(t, pool1, pool2)
}

func TestTestTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type PoolOrTx interface {
		Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	}

	checkTestTable := func(ctx context.Context, poolOrTx PoolOrTx) error {
		_, err := poolOrTx.Exec(ctx, "SELECT * FROM river_shared_test_tx_table")
		return err
	}

	// Test cleanups are invoked in the order of last added, first called. When
	// TestTx is called below it adds a cleanup, so we want to make sure that
	// this cleanup, which checks that the database remains pristine, is invoked
	// after the TestTx cleanup, so we add it first.
	t.Cleanup(func() {
		// Tests may inherit context from `t.Context()` which is cancelled after
		// tests run and before calling clean up. We need a non-cancelled
		// context to issue rollback here, so use a bit of a bludgeon to do so
		// with `context.WithoutCancel()`.
		ctx := context.WithoutCancel(ctx)

		err := checkTestTable(ctx, DBPool(ctx, t))
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
	})

	tx := TestTx(ctx, t)

	_, err := tx.Exec(ctx, "CREATE TABLE river_shared_test_tx_table (id bigint)")
	require.NoError(t, err)

	err = checkTestTable(ctx, tx)
	require.NoError(t, err)
}

func TestWaitOrTimeout(t *testing.T) {
	t.Parallel()

	// Inject a few extra numbers to make sure we pick only one.
	numChan := make(chan int, 5)
	for i := range 5 {
		numChan <- i
	}

	num := WaitOrTimeout(t, numChan)
	require.Equal(t, 0, num)
}

func TestWaitOrTimeoutN(t *testing.T) {
	t.Parallel()

	// Inject a few extra numbers to make sure we pick the right number.
	numChan := make(chan int, 5)
	for i := range 5 {
		numChan <- i
	}

	nums := WaitOrTimeoutN(t, numChan, 3)
	require.Equal(t, []int{0, 1, 2}, nums)
}

func TestTimeStub(t *testing.T) {
	t.Parallel()

	t.Run("BasicUsage", func(t *testing.T) {
		t.Parallel()

		initialTime := time.Now().UTC()

		timeStub := &TimeStub{}

		timeStub.StubNowUTC(initialTime)
		require.Equal(t, initialTime, timeStub.NowUTC())

		newTime := timeStub.StubNowUTC(initialTime.Add(1 * time.Second))
		require.Equal(t, newTime, timeStub.NowUTC())
	})

	t.Run("Stress", func(t *testing.T) {
		t.Parallel()

		timeStub := &TimeStub{}

		for range 10 {
			go func() {
				for range 50 {
					timeStub.StubNowUTC(time.Now().UTC())
					_ = timeStub.NowUTC()
				}
			}()
		}
	})
}

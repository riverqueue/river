package riversharedtest

import (
	"context"
	"testing"
	"time"

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

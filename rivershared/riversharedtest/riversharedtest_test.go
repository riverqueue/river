package riversharedtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitOrTimeout(t *testing.T) {
	t.Parallel()

	// Inject a few extra numbers to make sure we pick only one.
	numChan := make(chan int, 5)
	for i := 0; i < 5; i++ {
		numChan <- i
	}

	num := WaitOrTimeout(t, numChan)
	require.Equal(t, 0, num)
}

func TestWaitOrTimeoutN(t *testing.T) {
	t.Parallel()

	// Inject a few extra numbers to make sure we pick the right number.
	numChan := make(chan int, 5)
	for i := 0; i < 5; i++ {
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

		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					timeStub.StubNowUTC(time.Now().UTC())
					_ = timeStub.NowUTC()
				}
			}()
		}
	})
}

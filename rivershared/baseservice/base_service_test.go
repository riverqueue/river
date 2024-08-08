package baseservice

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivershared/util/randutil"
)

func TestInit(t *testing.T) {
	t.Parallel()

	archetype := archetype()

	myService := Init(archetype, &MyService{})
	require.NotNil(t, myService.Logger)
	require.Equal(t, "MyService", myService.Name)
	require.WithinDuration(t, time.Now().UTC(), myService.Time.NowUTC(), 2*time.Second)
}

func TestBaseService_CancellableSleep(t *testing.T) {
	t.Parallel()

	testCancellableSleep := func(t *testing.T, startSleepFunc func(ctx context.Context, myService *MyService) <-chan struct{}) {
		t.Helper()

		ctx := context.Background()

		archetype := archetype()
		myService := Init(archetype, &MyService{})

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)

		sleepDone := startSleepFunc(ctx, myService)

		// Wait a very nominal amount of time just to make sure that some sleep is
		// actually happening.
		select {
		case <-sleepDone:
			require.FailNow(t, "Sleep returned sooner than expected")
		case <-time.After(50 * time.Millisecond):
		}

		cancel()

		select {
		case <-sleepDone:
		case <-time.After(50 * time.Millisecond):
			require.FailNow(t, "Timed out waiting for sleep to finish after cancel")
		}
	}

	// Starts sleep for sleep functions that don't return a channel, returning a
	// channel that's closed when sleep finishes.
	startSleep := func(sleepFunc func()) <-chan struct{} {
		sleepDone := make(chan struct{})
		go func() {
			defer close(sleepDone)
			sleepFunc()
		}()
		return sleepDone
	}

	t.Run("CancellableSleep", func(t *testing.T) {
		t.Parallel()

		testCancellableSleep(t, func(ctx context.Context, myService *MyService) <-chan struct{} {
			return startSleep(func() {
				myService.CancellableSleep(ctx, 5*time.Second)
			})
		})
	})

	t.Run("CancellableSleepC", func(t *testing.T) {
		t.Parallel()

		testCancellableSleep(t, func(ctx context.Context, myService *MyService) <-chan struct{} {
			return myService.CancellableSleepC(ctx, 5*time.Second)
		})
	})

	t.Run("CancellableSleepRandomBetween", func(t *testing.T) {
		t.Parallel()

		testCancellableSleep(t, func(ctx context.Context, myService *MyService) <-chan struct{} {
			return startSleep(func() {
				myService.CancellableSleepRandomBetween(ctx, 5*time.Second, 10*time.Second)
			})
		})
	})

	t.Run("CancellableSleepRandomBetweenC", func(t *testing.T) {
		t.Parallel()

		testCancellableSleep(t, func(ctx context.Context, myService *MyService) <-chan struct{} {
			return myService.CancellableSleepRandomBetweenC(ctx, 5*time.Second, 10*time.Second)
		})
	})
}

func TestBaseService_ExponentialBackoff(t *testing.T) {
	t.Parallel()

	archetype := archetype()
	myService := Init(archetype, &MyService{})

	require.InDelta(t, 1.0, myService.ExponentialBackoff(1, MaxAttemptsBeforeResetDefault).Seconds(), 1.0*0.1)
	require.InDelta(t, 2.0, myService.ExponentialBackoff(2, MaxAttemptsBeforeResetDefault).Seconds(), 2.0*0.1)
	require.InDelta(t, 4.0, myService.ExponentialBackoff(3, MaxAttemptsBeforeResetDefault).Seconds(), 4.0*0.1)
	require.InDelta(t, 8.0, myService.ExponentialBackoff(4, MaxAttemptsBeforeResetDefault).Seconds(), 8.0*0.1)
	require.InDelta(t, 16.0, myService.ExponentialBackoff(5, MaxAttemptsBeforeResetDefault).Seconds(), 16.0*0.1)
	require.InDelta(t, 32.0, myService.ExponentialBackoff(6, MaxAttemptsBeforeResetDefault).Seconds(), 32.0*0.1)
}

func TestBaseService_exponentialBackoffSecondsWithoutJitter(t *testing.T) {
	t.Parallel()

	archetype := archetype()
	myService := Init(archetype, &MyService{})

	require.Equal(t, 1, int(myService.exponentialBackoffSecondsWithoutJitter(1, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 2, int(myService.exponentialBackoffSecondsWithoutJitter(2, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 4, int(myService.exponentialBackoffSecondsWithoutJitter(3, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 8, int(myService.exponentialBackoffSecondsWithoutJitter(4, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 16, int(myService.exponentialBackoffSecondsWithoutJitter(5, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 32, int(myService.exponentialBackoffSecondsWithoutJitter(6, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 64, int(myService.exponentialBackoffSecondsWithoutJitter(7, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 128, int(myService.exponentialBackoffSecondsWithoutJitter(8, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 256, int(myService.exponentialBackoffSecondsWithoutJitter(9, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 512, int(myService.exponentialBackoffSecondsWithoutJitter(10, MaxAttemptsBeforeResetDefault)))
	require.Equal(t, 1, int(myService.exponentialBackoffSecondsWithoutJitter(11, MaxAttemptsBeforeResetDefault))) // resets
}

type MyService struct {
	BaseService
}

func archetype() *Archetype {
	return &Archetype{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		Rand:   randutil.NewCryptoSeededConcurrentSafeRand(),
		Time:   &UnStubbableTimeGenerator{},
	}
}

func TestSimplifyLogName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "NotGeneric", simplifyLogName("NotGeneric"))

	// Simplified for use during debugging. Real generics will tend to have
	// fully qualified paths and not look like this.
	require.Equal(t, "Simple[int]", simplifyLogName("Simple[int]"))
	require.Equal(t, "Simple[*int]", simplifyLogName("Simple[*int]"))
	require.Equal(t, "Simple[[]int]", simplifyLogName("Simple[[]int]"))
	require.Equal(t, "Simple[[]*int]", simplifyLogName("Simple[[]*int]"))

	// More realistic examples.
	require.Equal(t, "QueryCacher[dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
	require.Equal(t, "QueryCacher[*dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[*github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
	require.Equal(t, "QueryCacher[[]dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[[]github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
	require.Equal(t, "QueryCacher[[]*dbsqlc.JobCountByStateRow]", simplifyLogName("QueryCacher[[]*github.com/riverqueue/riverui/internal/dbsqlc.JobCountByStateRow]"))
}

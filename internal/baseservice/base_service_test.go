package baseservice

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/util/randutil"
)

func TestArchetype_WithSleepDisabled(t *testing.T) {
	t.Parallel()

	archetype := (&Archetype{}).WithSleepDisabled()
	require.True(t, archetype.DisableSleep)
}

func TestInit(t *testing.T) {
	t.Parallel()

	archetype := archetype()

	myService := Init(archetype, &MyService{})
	require.True(t, myService.DisableSleep)
	require.NotNil(t, myService.Logger)
	require.Equal(t, "MyService", myService.Name)
	require.WithinDuration(t, time.Now().UTC(), myService.TimeNowUTC(), 2*time.Second)
}

func TestBaseService_CancellableSleep(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	archetype := archetype()
	myService := Init(archetype, &MyService{})

	// A deadline
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "Test case took too long to run (sleep statements should return immediately)")
		}
	}()

	// Returns immediately because context is cancelled.
	{
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		myService.CancellableSleep(ctx, 15*time.Second)
	}

	// Returns immediately because `DisableSleep` flag is on.
	myService.DisableSleep = true
	myService.CancellableSleep(ctx, 15*time.Second)
}

type MyService struct {
	BaseService
}

func archetype() *Archetype {
	return &Archetype{
		DisableSleep: true,
		Logger:       slog.New(slog.NewTextHandler(os.Stdout, nil)),
		Rand:         randutil.NewCryptoSeededConcurrentSafeRand(),
		TimeNowUTC:   func() time.Time { return time.Now().UTC() },
	}
}

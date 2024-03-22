package startstoptest

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/riverinternaltest"
)

type MyService struct {
	startstop.BaseStartStop
	logger   *slog.Logger
	startErr error
}

func (s *MyService) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if s.startErr != nil {
		stopped()
		return s.startErr
	}

	go func() {
		started()
		defer stopped()

		s.logger.InfoContext(ctx, "Service started")
		defer s.logger.InfoContext(ctx, "Service stopped")

		<-ctx.Done()
	}()

	return nil
}

func TestStress(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	Stress(ctx, t, &MyService{logger: riverinternaltest.Logger(t)})
}

func TestStressErr(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	startErr := errors.New("error returned on start")

	StressErr(ctx, t, &MyService{logger: riverinternaltest.Logger(t), startErr: startErr}, startErr)

	mockT := newMockTestingT(t)
	StressErr(ctx, mockT, &MyService{logger: riverinternaltest.Logger(t), startErr: errors.New("different error")}, startErr)
	require.True(t, mockT.failed.Load())
}

type mockTestingT struct {
	failed atomic.Bool
	tb     testing.TB
}

func newMockTestingT(tb testing.TB) *mockTestingT {
	tb.Helper()
	return &mockTestingT{tb: tb}
}

func (t *mockTestingT) Errorf(format string, args ...interface{}) {}
func (t *mockTestingT) FailNow()                                  { t.failed.Store(true) }
func (t *mockTestingT) Helper()                                   { t.tb.Helper() }

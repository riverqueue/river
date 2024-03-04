package startstoptest

import (
	"context"
	"log/slog"
	"testing"

	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/riverinternaltest"
)

type MyService struct {
	startstop.BaseStartStop
	logger *slog.Logger
}

func (s *MyService) Start(ctx context.Context) error {
	ctx, shouldStart, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	go func() {
		defer close(stopped)

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

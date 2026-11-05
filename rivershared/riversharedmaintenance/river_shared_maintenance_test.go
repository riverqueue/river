package riversharedmaintenance

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestQueueMaintainerServiceBase_StaggerStart(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name            string
		cancelled       bool
		staggerDisabled bool
	}{
		{name: "Cancelled", cancelled: true},
		{name: "CancelledWithStaggerDisabled", cancelled: true, staggerDisabled: true},
		{name: "StaggerDisabled", staggerDisabled: true},
		{name: "StaggerEnabled"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				svc := &QueueMaintainerServiceBase{}
				svc.StaggerStartupDisable(testCase.staggerDisabled)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if testCase.cancelled {
					cancel()
				}

				require.Equal(t, !testCase.cancelled, svc.StaggerStart(ctx))
			})
		})
	}
}

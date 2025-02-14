package river

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
)

func TestClientDriverPlugin(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		pluginDriver *TestDriverWithPlugin
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		pluginDriver := newDriverWithPlugin(t, riverinternaltest.TestDB(ctx, t))

		client, err := NewClient(pluginDriver, newTestConfig(t, nil))
		require.NoError(t, err)

		return client, &testBundle{
			pluginDriver: pluginDriver,
		}
	}

	t.Run("InitCalled", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		require.True(t, bundle.pluginDriver.initCalled)
	})
}

var _ driverPlugin[pgx.Tx] = &TestDriverWithPlugin{}

type TestDriverWithPlugin struct {
	*riverpgxv5.Driver
	initCalled bool
	pilot      riverpilot.Pilot
}

func newDriverWithPlugin(t *testing.T, dbPool *pgxpool.Pool) *TestDriverWithPlugin {
	t.Helper()

	return &TestDriverWithPlugin{
		Driver: riverpgxv5.New(dbPool),
	}
}

func (d *TestDriverWithPlugin) PluginInit(archetype *baseservice.Archetype) {
	d.initCalled = true
}

func (d *TestDriverWithPlugin) PluginPilot() riverpilot.Pilot {
	if !d.initCalled {
		panic("expected PluginInit to be called before this function")
	}

	return d.pilot
}

func TestClientPilotPlugin(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		pluginDriver *TestDriverWithPlugin
		pluginPilot  *TestPilotWithPlugin
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		pluginDriver := newDriverWithPlugin(t, riverinternaltest.TestDB(ctx, t))
		pluginPilot := newPilotWithPlugin(t)
		pluginDriver.pilot = pluginPilot

		client, err := NewClient(pluginDriver, newTestConfig(t, nil))
		require.NoError(t, err)

		return client, &testBundle{
			pluginDriver: pluginDriver,
			pluginPilot:  pluginPilot,
		}
	}

	t.Run("ServicesStart", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		riversharedtest.WaitOrTimeout(t, startstop.WaitAllStartedC(
			bundle.pluginPilot.maintenanceService,
			bundle.pluginPilot.service,
		))
	})
}

var _ pilotPlugin = &TestPilotWithPlugin{}

type TestPilotWithPlugin struct {
	riverpilot.StandardPilot
	maintenanceService startstop.Service
	service            startstop.Service
}

func newPilotWithPlugin(t *testing.T) *TestPilotWithPlugin {
	t.Helper()

	newService := func(name string) startstop.Service {
		return startstop.StartStopFunc(func(ctx context.Context, shouldStart bool, started, stopped func()) error {
			if !shouldStart {
				return nil
			}

			go func() {
				started()
				defer stopped() // this defer should come first so it's last out

				t.Logf("Test service started: %s", name)

				<-ctx.Done()
			}()

			return nil
		})
	}

	return &TestPilotWithPlugin{
		StandardPilot:      riverpilot.StandardPilot{},
		maintenanceService: newService("maintenance service"),
		service:            newService("other service"),
	}
}

func (d *TestPilotWithPlugin) PluginMaintenanceServices() []startstop.Service {
	return []startstop.Service{d.maintenanceService}
}

func (d *TestPilotWithPlugin) PluginServices() []startstop.Service {
	return []startstop.Service{d.service}
}

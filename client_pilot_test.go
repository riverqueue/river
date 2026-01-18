package river

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

type pilotSpy struct {
	riverpilot.StandardPilot

	jobCancelCalls                atomic.Int64
	jobCleanerQueuesExcludedCalls atomic.Int64
	jobInsertManyCalls            atomic.Int64
	jobRetryCalls                 atomic.Int64
	pilotInitCalls                atomic.Int64

	testSignals pilotSpyTestSignals
}

type pilotSpyTestSignals struct {
	JobGetAvailable          testsignal.TestSignal[struct{}]
	JobSetStateIfRunningMany testsignal.TestSignal[struct{}]
	PeriodicJobGetAll        testsignal.TestSignal[struct{}]
	PeriodicJobKeepAlive     testsignal.TestSignal[struct{}]
	PeriodicJobUpsertMany    testsignal.TestSignal[struct{}]
	PilotInit                testsignal.TestSignal[struct{}]
	ProducerInit             testsignal.TestSignal[struct{}]
	ProducerKeepAlive        testsignal.TestSignal[struct{}]
	ProducerShutdown         testsignal.TestSignal[struct{}]
	QueueMetadataChanged     testsignal.TestSignal[struct{}]
}

func (ts *pilotSpyTestSignals) Init(tb testutil.TestingTB) {
	ts.JobGetAvailable.Init(tb)
	ts.JobSetStateIfRunningMany.Init(tb)
	ts.PeriodicJobGetAll.Init(tb)
	ts.PeriodicJobKeepAlive.Init(tb)
	ts.PeriodicJobUpsertMany.Init(tb)
	ts.PilotInit.Init(tb)
	ts.ProducerInit.Init(tb)
	ts.ProducerKeepAlive.Init(tb)
	ts.ProducerShutdown.Init(tb)
	ts.QueueMetadataChanged.Init(tb)
}

func (p *pilotSpy) JobCancel(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	p.jobCancelCalls.Add(1)
	return p.StandardPilot.JobCancel(ctx, exec, params)
}

func (p *pilotSpy) JobCleanerQueuesExcluded() []string {
	p.jobCleanerQueuesExcludedCalls.Add(1)
	return p.StandardPilot.JobCleanerQueuesExcluded()
}

func (p *pilotSpy) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state riverpilot.ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	p.testSignals.JobGetAvailable.Signal(struct{}{})
	return p.StandardPilot.JobGetAvailable(ctx, exec, state, params)
}

func (p *pilotSpy) JobInsertMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	p.jobInsertManyCalls.Add(1)
	return p.StandardPilot.JobInsertMany(ctx, exec, params)
}

func (p *pilotSpy) JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	p.jobRetryCalls.Add(1)
	return p.StandardPilot.JobRetry(ctx, exec, params)
}

func (p *pilotSpy) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	p.testSignals.JobSetStateIfRunningMany.Signal(struct{}{})
	return p.StandardPilot.JobSetStateIfRunningMany(ctx, exec, params)
}

func (p *pilotSpy) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
	p.testSignals.PeriodicJobGetAll.Signal(struct{}{})
	return p.StandardPilot.PeriodicJobGetAll(ctx, exec, params)
}

func (p *pilotSpy) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
	p.testSignals.PeriodicJobKeepAlive.Signal(struct{}{})
	return p.StandardPilot.PeriodicJobKeepAliveAndReap(ctx, exec, params)
}

func (p *pilotSpy) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
	p.testSignals.PeriodicJobUpsertMany.Signal(struct{}{})
	return p.StandardPilot.PeriodicJobUpsertMany(ctx, exec, params)
}

func (p *pilotSpy) PilotInit(archetype *baseservice.Archetype, params *riverpilot.PilotInitParams) {
	p.pilotInitCalls.Add(1)
	p.testSignals.PilotInit.Signal(struct{}{})
	p.StandardPilot.PilotInit(archetype, params)
}

func (p *pilotSpy) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *riverpilot.ProducerInitParams) (int64, riverpilot.ProducerState, error) {
	p.testSignals.ProducerInit.Signal(struct{}{})
	return p.StandardPilot.ProducerInit(ctx, exec, params)
}

func (p *pilotSpy) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	p.testSignals.ProducerKeepAlive.Signal(struct{}{})
	return p.StandardPilot.ProducerKeepAlive(ctx, exec, params)
}

func (p *pilotSpy) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *riverpilot.ProducerShutdownParams) error {
	p.testSignals.ProducerShutdown.Signal(struct{}{})
	return p.StandardPilot.ProducerShutdown(ctx, exec, params)
}

func (p *pilotSpy) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *riverpilot.QueueMetadataChangedParams) error {
	p.testSignals.QueueMetadataChanged.Signal(struct{}{})
	return p.StandardPilot.QueueMetadataChanged(ctx, exec, params)
}

func Test_Client_PilotUsage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T, configMutate func(*Config)) (*Client[pgx.Tx], *pilotSpy) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		if configMutate != nil {
			configMutate(config)
		}

		pilot := &pilotSpy{}
		pluginDriver := newDriverWithPlugin(t, dbPool)
		pluginDriver.pilot = pilot

		client, err := NewClient(pluginDriver, config)
		require.NoError(t, err)

		return client, pilot
	}

	withClientTx := func(t *testing.T, client *Client[pgx.Tx], callback func(tx pgx.Tx)) {
		t.Helper()

		exec := client.Driver().GetExecutor()
		execTx, err := exec.Begin(ctx)
		require.NoError(t, err)

		committed := false
		t.Cleanup(func() {
			if !committed {
				_ = execTx.Rollback(ctx)
			}
		})

		tx := client.Driver().UnwrapTx(execTx)
		callback(tx)

		require.NoError(t, execTx.Commit(ctx))
		committed = true
	}

	t.Run("InitUsesPilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)
		require.NotNil(t, client)
		require.Equal(t, int64(1), pilot.jobCleanerQueuesExcludedCalls.Load())
		require.Equal(t, int64(1), pilot.pilotInitCalls.Load())
	})

	t.Run("JobCancelUsesPilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Now().Add(5 * time.Minute),
		})
		require.NoError(t, err)

		_, err = client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, int64(1), pilot.jobCancelCalls.Load())
	})

	t.Run("JobCancelTxUsesPilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Now().Add(5 * time.Minute),
		})
		require.NoError(t, err)

		withClientTx(t, client, func(tx pgx.Tx) {
			_, err = client.JobCancelTx(ctx, tx, insertRes.Job.ID)
			require.NoError(t, err)
			require.Equal(t, int64(1), pilot.jobCancelCalls.Load())
		})
	})

	t.Run("JobInsertManyUsesPilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)

		_, err := client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), pilot.jobInsertManyCalls.Load())
	})

	t.Run("JobRetryUsesPilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Now().Add(5 * time.Minute),
		})
		require.NoError(t, err)

		_, err = client.JobRetry(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, int64(1), pilot.jobRetryCalls.Load())
	})

	t.Run("JobRetryTxUsesPilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Now().Add(5 * time.Minute),
		})
		require.NoError(t, err)

		withClientTx(t, client, func(tx pgx.Tx) {
			_, err = client.JobRetryTx(ctx, tx, insertRes.Job.ID)
			require.NoError(t, err)
			require.Equal(t, int64(1), pilot.jobRetryCalls.Load())
		})
	})

	t.Run("PeriodicJobsUsePilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, func(config *Config) {
			config.PeriodicJobs = []*PeriodicJob{
				NewPeriodicJob(PeriodicInterval(time.Second), func() (JobArgs, *InsertOpts) {
					return noOpArgs{}, nil
				}, &PeriodicJobOpts{
					ID:         "pilot_periodic_job",
					RunOnStart: true,
				}),
			}
		})

		client.testSignals.Init(t)
		pilot.testSignals.Init(t)

		startClient(ctx, t, client)
		client.testSignals.electedLeader.WaitOrTimeout()

		pilot.testSignals.PeriodicJobGetAll.WaitOrTimeout()
		pilot.testSignals.PeriodicJobUpsertMany.WaitOrTimeout()
		pilot.testSignals.PeriodicJobKeepAlive.WaitOrTimeout()
	})

	t.Run("ProducerAndCompleterUsePilot", func(t *testing.T) {
		t.Parallel()

		client, pilot := setup(t, nil)

		jobDone := make(chan struct{})

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			close(jobDone)
			return nil
		}))

		pilot.testSignals.Init(t)

		require.NoError(t, client.Start(ctx))

		stopOnce := sync.Once{}
		stopClient := func() {
			stopOnce.Do(func() {
				stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				require.NoError(t, client.Stop(stopCtx))
			})
		}
		t.Cleanup(stopClient)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, jobDone)
		require.NotZero(t, insertRes.Job.ID)

		pilot.testSignals.JobGetAvailable.WaitOrTimeout()
		pilot.testSignals.JobSetStateIfRunningMany.WaitOrTimeout()
		pilot.testSignals.ProducerInit.WaitOrTimeout()
		pilot.testSignals.ProducerKeepAlive.WaitOrTimeout()
		pilot.testSignals.QueueMetadataChanged.WaitOrTimeout()

		stopClient()

		pilot.testSignals.ProducerShutdown.WaitOrTimeout()
	})
}

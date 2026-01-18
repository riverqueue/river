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
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

type pilotSpy struct {
	riverpilot.StandardPilot

	jobCancelCalls                atomic.Int64
	jobCleanerQueuesExcludedCalls atomic.Int64
	jobGetAvailableCalls          atomic.Int64
	jobInsertManyCalls            atomic.Int64
	jobRetryCalls                 atomic.Int64
	jobSetStateIfRunningManyCalls atomic.Int64
	periodicJobGetAllCalls        atomic.Int64
	periodicJobKeepAliveCalls     atomic.Int64
	periodicJobUpsertManyCalls    atomic.Int64
	pilotInitCalls                atomic.Int64
	producerInitCalls             atomic.Int64
	producerKeepAliveCalls        atomic.Int64
	producerShutdownCalls         atomic.Int64
	queueMetadataChangedCalls     atomic.Int64

	jobGetAvailableCh          chan struct{}
	jobSetStateIfRunningManyCh chan struct{}
	periodicJobGetAllCh        chan struct{}
	periodicJobKeepAliveCh     chan struct{}
	periodicJobUpsertManyCh    chan struct{}
	pilotInitCh                chan struct{}
	producerInitCh             chan struct{}
	producerKeepAliveCh        chan struct{}
	producerShutdownCh         chan struct{}
	queueMetadataChangedCh     chan struct{}
}

func newPilotSpy() *pilotSpy {
	return &pilotSpy{
		jobGetAvailableCh:          make(chan struct{}, 1),
		jobSetStateIfRunningManyCh: make(chan struct{}, 1),
		periodicJobGetAllCh:        make(chan struct{}, 1),
		periodicJobKeepAliveCh:     make(chan struct{}, 1),
		periodicJobUpsertManyCh:    make(chan struct{}, 1),
		pilotInitCh:                make(chan struct{}, 1),
		producerInitCh:             make(chan struct{}, 1),
		producerKeepAliveCh:        make(chan struct{}, 1),
		producerShutdownCh:         make(chan struct{}, 1),
		queueMetadataChangedCh:     make(chan struct{}, 1),
	}
}

func (p *pilotSpy) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
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
	p.jobGetAvailableCalls.Add(1)
	p.signal(p.jobGetAvailableCh)
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
	p.jobSetStateIfRunningManyCalls.Add(1)
	p.signal(p.jobSetStateIfRunningManyCh)
	return p.StandardPilot.JobSetStateIfRunningMany(ctx, exec, params)
}

func (p *pilotSpy) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
	p.periodicJobGetAllCalls.Add(1)
	p.signal(p.periodicJobGetAllCh)
	return p.StandardPilot.PeriodicJobGetAll(ctx, exec, params)
}

func (p *pilotSpy) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
	p.periodicJobKeepAliveCalls.Add(1)
	p.signal(p.periodicJobKeepAliveCh)
	return p.StandardPilot.PeriodicJobKeepAliveAndReap(ctx, exec, params)
}

func (p *pilotSpy) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
	p.periodicJobUpsertManyCalls.Add(1)
	p.signal(p.periodicJobUpsertManyCh)
	return p.StandardPilot.PeriodicJobUpsertMany(ctx, exec, params)
}

func (p *pilotSpy) PilotInit(archetype *baseservice.Archetype, params *riverpilot.PilotInitParams) {
	p.pilotInitCalls.Add(1)
	p.signal(p.pilotInitCh)
	p.StandardPilot.PilotInit(archetype, params)
}

func (p *pilotSpy) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *riverpilot.ProducerInitParams) (int64, riverpilot.ProducerState, error) {
	p.producerInitCalls.Add(1)
	p.signal(p.producerInitCh)
	return p.StandardPilot.ProducerInit(ctx, exec, params)
}

func (p *pilotSpy) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	p.producerKeepAliveCalls.Add(1)
	p.signal(p.producerKeepAliveCh)
	return p.StandardPilot.ProducerKeepAlive(ctx, exec, params)
}

func (p *pilotSpy) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *riverpilot.ProducerShutdownParams) error {
	p.producerShutdownCalls.Add(1)
	p.signal(p.producerShutdownCh)
	return p.StandardPilot.ProducerShutdown(ctx, exec, params)
}

func (p *pilotSpy) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *riverpilot.QueueMetadataChangedParams) error {
	p.queueMetadataChangedCalls.Add(1)
	p.signal(p.queueMetadataChangedCh)
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

		pilot := newPilotSpy()
		pluginDriver := newDriverWithPlugin(t, dbPool)
		pluginDriver.pilot = pilot

		client, err := NewClient(pluginDriver, config)
		require.NoError(t, err)

		return client, pilot
	}

	t.Run("InitUsesPilot", func(t *testing.T) {
		client, pilot := setup(t, nil)
		require.NotNil(t, client)
		require.Equal(t, int64(1), pilot.jobCleanerQueuesExcludedCalls.Load())
		require.Equal(t, int64(1), pilot.pilotInitCalls.Load())
	})

	t.Run("JobInsertManyUsesPilot", func(t *testing.T) {
		client, pilot := setup(t, nil)

		_, err := client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), pilot.jobInsertManyCalls.Load())
	})

	t.Run("JobCancelUsesPilot", func(t *testing.T) {
		client, pilot := setup(t, nil)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Now().Add(5 * time.Minute),
		})
		require.NoError(t, err)

		_, err = client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, int64(1), pilot.jobCancelCalls.Load())
	})

	t.Run("JobRetryUsesPilot", func(t *testing.T) {
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
		client, pilot := setup(t, nil)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Now().Add(5 * time.Minute),
		})
		require.NoError(t, err)

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

		_, err = client.JobRetryTx(ctx, tx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, int64(1), pilot.jobRetryCalls.Load())

		require.NoError(t, execTx.Commit(ctx))
		committed = true
	})

	t.Run("PeriodicJobsUsePilot", func(t *testing.T) {
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

		startClient(ctx, t, client)
		client.testSignals.electedLeader.WaitOrTimeout()

		riversharedtest.WaitOrTimeout(t, pilot.periodicJobGetAllCh)
		riversharedtest.WaitOrTimeout(t, pilot.periodicJobUpsertManyCh)
		riversharedtest.WaitOrTimeout(t, pilot.periodicJobKeepAliveCh)
	})

	t.Run("ProducerAndCompleterUsePilot", func(t *testing.T) {
		client, pilot := setup(t, nil)

		jobDone := make(chan struct{})

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			close(jobDone)
			return nil
		}))

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

		riversharedtest.WaitOrTimeout(t, pilot.jobGetAvailableCh)
		riversharedtest.WaitOrTimeout(t, pilot.jobSetStateIfRunningManyCh)
		riversharedtest.WaitOrTimeout(t, pilot.producerInitCh)
		riversharedtest.WaitOrTimeout(t, pilot.producerKeepAliveCh)
		riversharedtest.WaitOrTimeout(t, pilot.queueMetadataChangedCh)

		stopClient()

		riversharedtest.WaitOrTimeout(t, pilot.producerShutdownCh)
	})
}

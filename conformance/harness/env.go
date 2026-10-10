package harness

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// Env is the environment of one scenario run: a database of its own, and
// the reference and candidate adapters connected to it.
type Env struct {
	// Candidate is the adapter of the implementation under test.
	Candidate *Adapter

	// DB is the scenario's database.
	DB *Database

	// Driver is DriverPostgres or DriverSQLite.
	Driver string

	// Reference is the adapter of the reference implementation, River Go
	// unless RIVER_CONFORMANCE_REFERENCE says otherwise.
	Reference *Adapter

	adapters int
	config   *config
	opts     *EnvOpts
}

// Another sets up another environment like this one, with a database of its
// own, for scenarios that compare what implementations write into
// databases that start out the same.
func (e *Env) Another(t *testing.T) *Env {
	t.Helper()

	// It's part of the same scenario, which already holds a slot.
	return newEnvWithoutSlot(t, e.config, e.Driver, e.opts)
}

// EnvOpts adjust a scenario's environment.
type EnvOpts struct {
	// Drivers limits the scenario to these drivers.
	Drivers []string

	// NoMigrate leaves the database unmigrated.
	NoMigrate bool

	// SearchPath follows the scenario's schema in the search path of
	// adapters on Postgres.
	SearchPath []string

	// Setup prepares the database before adapters connect.
	Setup func(t *testing.T, db *Database)
}

// StartAdapter starts another adapter of implementation, such as a process
// to kill.
func (e *Env) StartAdapter(t *testing.T, implementation *Implementation) *Adapter {
	t.Helper()

	e.adapters++
	return startAdapter(t, implementation, e.DB, "", fmt.Sprintf("%s adapter %d", implementation.Name, e.adapters))
}

// StartAdapterURL starts another adapter of implementation that connects
// through databaseURL, such as a fault proxy's.
func (e *Env) StartAdapterURL(t *testing.T, implementation *Implementation, databaseURL string) *Adapter {
	t.Helper()

	e.adapters++
	return startAdapter(t, implementation, e.DB, databaseURL, fmt.Sprintf("%s adapter %d", implementation.Name, e.adapters))
}

// config is the harness's configuration from the environment.
type config struct {
	candidate *Implementation
	drivers   []string
	nightly   bool
	peer      *Implementation
	reference *Implementation
}

// loadConfig reads the configuration, skipping the test when conformance
// isn't enabled.
func loadConfig(t *testing.T) *config {
	t.Helper()

	candidateName := os.Getenv("RIVER_CONFORMANCE")
	if candidateName == "" {
		t.Skip("set RIVER_CONFORMANCE to the implementation to test (go, rust, or js) to run conformance scenarios")
	}
	candidate, err := lookupImplementation(candidateName)
	require.NoError(t, err)
	reference, err := lookupImplementation(cmp.Or(os.Getenv("RIVER_CONFORMANCE_REFERENCE"), "go"))
	require.NoError(t, err)
	drivers := strings.Split(cmp.Or(os.Getenv("RIVER_CONFORMANCE_DRIVERS"), DriverPostgres+","+DriverSQLite), ",")
	for _, driver := range drivers {
		require.Contains(t, []string{DriverPostgres, DriverSQLite}, driver, "RIVER_CONFORMANCE_DRIVERS")
	}
	var peer *Implementation
	if name := os.Getenv("RIVER_CONFORMANCE_PEER"); name != "" {
		peer, err = lookupImplementation(name)
		require.NoError(t, err)
	}
	return &config{
		candidate: candidate,
		peer:      peer,
		drivers:   drivers,
		nightly:   os.Getenv("RIVER_CONFORMANCE_NIGHTLY") != "",
		reference: reference,
	}
}

// RequireNightly skips a test outside the nightly tier.
func RequireNightly(t *testing.T) {
	t.Helper()

	if !loadConfig(t).nightly {
		t.Skip("set RIVER_CONFORMANCE_NIGHTLY=1 to run nightly scenarios")
	}
}

// RequirePeer returns the third implementation of a multi-engine fleet,
// skipping the test when RIVER_CONFORMANCE_PEER doesn't name one.
func RequirePeer(t *testing.T) *Implementation {
	t.Helper()

	peer := loadConfig(t).peer
	if peer == nil {
		t.Skip("set RIVER_CONFORMANCE_PEER to a third implementation to run multi-engine scenarios")
	}
	return peer
}

// postgresURL is the Postgres database scenarios create their schemas in.
func postgresURL() string {
	return cmp.Or(os.Getenv("TEST_DATABASE_URL"), "postgres://localhost:5432/river_test?sslmode=disable")
}

// envSlots bounds how many scenario environments run at once, so that their
// adapters' connections fit Postgres's default connection limit.
var envSlots = make(chan struct{}, 8) //nolint:gochecknoglobals // shared by every scenario in the process

// scenariosRun counts started scenario environments, which Main requires to
// be positive when conformance is enabled.
var scenariosRun atomic.Int64 //nolint:gochecknoglobals // shared by every scenario in the process

// newEnv sets up a scenario environment on driver.
func newEnv(t *testing.T, config *config, driver string, opts *EnvOpts) *Env {
	t.Helper()

	envSlots <- struct{}{}
	t.Cleanup(func() { <-envSlots })
	scenariosRun.Add(1)

	return newEnvWithoutSlot(t, config, driver, opts)
}

func newEnvWithoutSlot(t *testing.T, config *config, driver string, opts *EnvOpts) *Env {
	t.Helper()

	db := newDatabase(t, driver, opts.SearchPath)
	env := &Env{DB: db, Driver: driver, config: config, opts: opts}
	if opts.Setup != nil {
		opts.Setup(t, db)
	}
	env.Reference = startAdapter(t, config.reference, db, "", "reference "+config.reference.Name)
	env.Candidate = startAdapter(t, config.candidate, db, "", "candidate "+config.candidate.Name)
	if !opts.NoMigrate {
		env.Reference.Migrate(t, protocol.MigrateParams{})
	}
	return env
}

// EachDriver runs scenarioFunc as a parallel subtest for each enabled
// driver, each in a fresh environment.
func EachDriver(t *testing.T, opts *EnvOpts, scenarioFunc func(t *testing.T, env *Env)) {
	t.Helper()

	eachDriver(t, opts, func(t *testing.T, newEnvFunc func(t *testing.T) *Env) {
		t.Helper()

		scenarioFunc(t, newEnvFunc(t))
	})
}

// EachDirection runs scenarioFunc as a parallel subtest for each enabled
// driver and both orders of the reference and candidate, each in a fresh
// environment. A two-party scenario then proves its property with each
// implementation in each role.
func EachDirection(t *testing.T, opts *EnvOpts, scenarioFunc func(t *testing.T, env *Env, first, second *Adapter)) {
	t.Helper()

	eachDriver(t, opts, func(t *testing.T, newEnvFunc func(t *testing.T) *Env) {
		t.Helper()

		t.Run("reference_first", func(t *testing.T) {
			t.Parallel()

			env := newEnvFunc(t)
			scenarioFunc(t, env, env.Reference, env.Candidate)
		})
		t.Run("candidate_first", func(t *testing.T) {
			t.Parallel()

			env := newEnvFunc(t)
			scenarioFunc(t, env, env.Candidate, env.Reference)
		})
	})
}

func eachDriver(t *testing.T, opts *EnvOpts, driverFunc func(t *testing.T, newEnvFunc func(t *testing.T) *Env)) {
	t.Helper()

	if opts == nil {
		opts = &EnvOpts{}
	}
	config := loadConfig(t)
	for _, driver := range config.drivers {
		if opts.Drivers != nil && !slices.Contains(opts.Drivers, driver) {
			continue
		}
		t.Run(driver, func(t *testing.T) {
			t.Parallel()

			driverFunc(t, func(t *testing.T) *Env {
				t.Helper()

				return newEnv(t, config, driver, opts)
			})
		})
	}
}

// Main runs a conformance test package and removes the adapters it built.
func Main(m *testing.M) {
	buildDir, err := os.MkdirTemp("", "river-conformance-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, implementation := range knownImplementations {
		implementation.buildDir = buildDir
	}

	code := m.Run()
	_ = os.RemoveAll(buildDir)

	// `go test -run` succeeds when its pattern matches nothing, so an
	// enabled run must have run something.
	if code == 0 && os.Getenv("RIVER_CONFORMANCE") != "" && scenariosRun.Load() == 0 {
		fmt.Fprintln(os.Stderr, "RIVER_CONFORMANCE is set but no conformance scenario ran; check the -run pattern")
		code = 1
	}
	os.Exit(code)
}

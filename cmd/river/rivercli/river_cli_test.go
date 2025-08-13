package rivercli

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"net/url"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/cmd/river/riverbench"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

type DriverProcurerStub struct {
	getBenchmarkerStub func(config *riverbench.Config) BenchmarkerInterface
	getMigratorStub    func(config *rivermigrate.Config) (MigratorInterface, error)
	initPgxV5Stub      func(pool *pgxpool.Pool)
	queryRowStub       func(ctx context.Context, sql string, args ...any) riverdriver.Row
}

func (p *DriverProcurerStub) GetBenchmarker(config *riverbench.Config) BenchmarkerInterface {
	if p.getBenchmarkerStub == nil {
		panic("GetBenchmarker is not stubbed")
	}

	return p.getBenchmarkerStub(config)
}

func (p *DriverProcurerStub) GetMigrator(config *rivermigrate.Config) (MigratorInterface, error) {
	if p.getMigratorStub == nil {
		panic("GetMigrator is not stubbed")
	}

	return p.getMigratorStub(config)
}

func (p *DriverProcurerStub) InitPgxV5(pool *pgxpool.Pool) {
	if p.initPgxV5Stub == nil {
		panic("InitPgxV5 is not stubbed")
	}

	p.initPgxV5Stub(pool)
}

func (p *DriverProcurerStub) QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row {
	if p.queryRowStub == nil {
		panic("QueryRow is not stubbed")
	}

	return p.queryRowStub(ctx, sql, args...)
}

type MigratorStub struct {
	allVersionsStub      func() []rivermigrate.Migration
	existingVersionsStub func(ctx context.Context) ([]rivermigrate.Migration, error)
	getVersionStub       func(version int) (rivermigrate.Migration, error)
	migrateStub          func(ctx context.Context, direction rivermigrate.Direction, opts *rivermigrate.MigrateOpts) (*rivermigrate.MigrateResult, error)
	validateStub         func(ctx context.Context) (*rivermigrate.ValidateResult, error)
}

func (m *MigratorStub) AllVersions() []rivermigrate.Migration {
	if m.allVersionsStub == nil {
		panic("AllVersions is not stubbed")
	}

	return m.allVersionsStub()
}

func (m *MigratorStub) ExistingVersions(ctx context.Context) ([]rivermigrate.Migration, error) {
	if m.allVersionsStub == nil {
		panic("ExistingVersions is not stubbed")
	}

	return m.existingVersionsStub(ctx)
}

func (m *MigratorStub) GetVersion(version int) (rivermigrate.Migration, error) {
	if m.getVersionStub == nil {
		panic("GetVersion is not stubbed")
	}

	return m.getVersionStub(version)
}

func (m *MigratorStub) Migrate(ctx context.Context, direction rivermigrate.Direction, opts *rivermigrate.MigrateOpts) (*rivermigrate.MigrateResult, error) {
	if m.migrateStub == nil {
		panic("Migrate is not stubbed")
	}

	return m.migrateStub(ctx, direction, opts)
}

func (m *MigratorStub) Validate(ctx context.Context) (*rivermigrate.ValidateResult, error) {
	if m.validateStub == nil {
		panic("Validate is not stubbed")
	}

	return m.validateStub(ctx)
}

var (
	testMigration01 = rivermigrate.Migration{Name: "1st migration", SQLDown: "SELECT 'down 1' FROM /* TEMPLATE: schema */river_table", SQLUp: "SELECT 'up 1' FROM /* TEMPLATE: schema */river_table", Version: 1} //nolint:gochecknoglobals
	testMigration02 = rivermigrate.Migration{Name: "2nd migration", SQLDown: "SELECT 'down 2' FROM /* TEMPLATE: schema */river_table", SQLUp: "SELECT 'up 2' FROM /* TEMPLATE: schema */river_table", Version: 2} //nolint:gochecknoglobals
	testMigration03 = rivermigrate.Migration{Name: "3rd migration", SQLDown: "SELECT 'down 3' FROM /* TEMPLATE: schema */river_table", SQLUp: "SELECT 'up 3' FROM /* TEMPLATE: schema */river_table", Version: 3} //nolint:gochecknoglobals

	testMigrationAll = []rivermigrate.Migration{testMigration01, testMigration02, testMigration03} //nolint:gochecknoglobals
)

// High level integration tests that operate on the Cobra command directly. This
// isn't always appropriate because there's no way to inject a test transaction.
func TestBaseCommandSetIntegration(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		out *bytes.Buffer
	}

	setup := func(t *testing.T) (*cobra.Command, *testBundle) {
		t.Helper()

		cli := NewCLI(&Config{
			Name: "River",
		})

		var out bytes.Buffer
		cli.SetOut(&out)

		return cli.BaseCommandSet(), &testBundle{
			out: &out,
		}
	}

	t.Run("DebugVerboseMutuallyExclusive", func(t *testing.T) {
		t.Parallel()

		cmd, _ := setup(t)

		cmd.SetArgs([]string{"--debug", "--verbose"})
		require.EqualError(t, cmd.Execute(), "if any flags in the group [debug verbose] are set none of the others can be; [debug verbose] were all set")
	})

	t.Run("DatabaseURLWithInvalidPrefix", func(t *testing.T) {
		t.Parallel()

		cmd, _ := setup(t)

		cmd.SetArgs([]string{"migrate-down", "--database-url", "post://"})
		require.EqualError(t, cmd.Execute(), "unsupported database URL (`post://`); try one with a `postgres://`, `postgresql://`, or `sqlite://` scheme/prefix")
	})

	t.Run("MissingDatabaseURLAndPGEnv", func(t *testing.T) {
		t.Parallel()

		cmd, _ := setup(t)

		cmd.SetArgs([]string{"migrate-down"})
		require.EqualError(t, cmd.Execute(), "either PG* env vars or --database-url must be set")
	})

	t.Run("VersionFlag", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		cmd.SetArgs([]string{"--version"})
		require.NoError(t, cmd.Execute())

		buildInfo, _ := debug.ReadBuildInfo()

		// `devel` on 1.25, `unknown` on versions previous to that
		require.Regexp(t, strings.TrimSpace(fmt.Sprintf(`
River version \((devel|unknown)\)
Built with %s
		`, buildInfo.GoVersion)), strings.TrimSpace(bundle.out.String()))
	})

	t.Run("VersionSubcommand", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		cmd.SetArgs([]string{"version"})
		require.NoError(t, cmd.Execute())

		buildInfo, _ := debug.ReadBuildInfo()

		// `devel` on 1.25, `unknown` on versions previous to that
		require.Regexp(t, strings.TrimSpace(fmt.Sprintf(`
River version \((devel|unknown)\)
Built with %s
		`, buildInfo.GoVersion)), strings.TrimSpace(bundle.out.String()))
	})
}

// Same as the above, but non-parallel so tests can use `t.Setenv`. Separated
// out into its own test block so that we don't have to mark the entire block
// above as non-parallel because a few tests can't be made parallel.
func TestBaseCommandSetNonParallel(t *testing.T) {
	ctx := context.Background()

	type testBundle struct {
		out *bytes.Buffer
	}

	setup := func(t *testing.T) (*cobra.Command, *testBundle) {
		t.Helper()

		cli := NewCLI(&Config{
			Name: "River",
		})

		var out bytes.Buffer
		cli.SetOut(&out)

		return cli.BaseCommandSet(), &testBundle{
			out: &out,
		}
	}

	t.Run("PGEnvWithoutDatabaseURL", func(t *testing.T) {
		cmd, _ := setup(t)

		testDatabaseURL := riversharedtest.TestDatabaseURL()

		config, err := pgxpool.ParseConfig(testDatabaseURL)
		require.NoError(t, err)

		dbPool, err := pgxpool.NewWithConfig(ctx, config)
		require.NoError(t, err)

		var (
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		t.Setenv("TEST_DATABASE_URL", "")

		parsedURL, err := url.Parse(testDatabaseURL)
		require.NoError(t, err)

		t.Setenv("PGDATABASE", parsedURL.Path[1:])
		t.Setenv("PGHOST", parsedURL.Hostname())
		pass, _ := parsedURL.User.Password()
		t.Setenv("PGPASSWORD", pass)
		t.Setenv("PGPORT", cmp.Or(parsedURL.Port(), "5432"))
		t.Setenv("PGSSLMODE", parsedURL.Query().Get("sslmode"))
		t.Setenv("PGUSER", parsedURL.User.Username())

		cmd.SetArgs([]string{"migrate-up", "--schema", schema})
		require.NoError(t, cmd.Execute())
	})
}

func TestBaseCommandSetDriverProcurerPgxV5(t *testing.T) {
	t.Parallel()

	calledStub := false

	migratorStub := &MigratorStub{}
	migratorStub.allVersionsStub = func() []rivermigrate.Migration { return []rivermigrate.Migration{testMigration01} }
	migratorStub.getVersionStub = func(version int) (rivermigrate.Migration, error) {
		calledStub = true
		if version == 1 {
			return testMigration01, nil
		}

		return rivermigrate.Migration{}, fmt.Errorf("unknown version: %d", version)
	}
	migratorStub.existingVersionsStub = func(ctx context.Context) ([]rivermigrate.Migration, error) { return nil, nil }

	cli := NewCLI(&Config{
		DriverProcurer: &DriverProcurerStub{
			getMigratorStub: func(config *rivermigrate.Config) (MigratorInterface, error) {
				calledStub = true
				return migratorStub, nil
			},
			initPgxV5Stub: func(pool *pgxpool.Pool) {
				calledStub = true
			},
		},
		Name: "River",
	})

	var out bytes.Buffer
	cli.SetOut(&out)

	cmd := cli.BaseCommandSet()
	cmd.SetArgs([]string{"migrate-get", "--up", "--version", "1"})
	require.NoError(t, cmd.Execute())

	require.True(t, calledStub)

	require.Equal(t, strings.TrimSpace(`
-- River main migration 001 [up]
SELECT 'up 1' FROM river_table
		`), strings.TrimSpace(out.String()))
}

func TestMigrateGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		migratorStub *MigratorStub
		out          *bytes.Buffer
	}

	setup := func(t *testing.T) (*migrateGet, *testBundle) {
		t.Helper()

		cmd, out := withCommandBase(t, &migrateGet{})

		migratorStub := &MigratorStub{}
		migratorStub.allVersionsStub = func() []rivermigrate.Migration { return testMigrationAll }
		migratorStub.getVersionStub = func(version int) (rivermigrate.Migration, error) {
			switch version {
			case 1:
				return testMigration01, nil
			case 2:
				return testMigration02, nil
			case 3:
				return testMigration03, nil
			}
			return rivermigrate.Migration{}, fmt.Errorf("unknown version: %d", version)
		}
		migratorStub.existingVersionsStub = func(ctx context.Context) ([]rivermigrate.Migration, error) { return nil, nil }

		cmd.GetCommandBase().DriverProcurer = &DriverProcurerStub{
			getMigratorStub: func(config *rivermigrate.Config) (MigratorInterface, error) { return migratorStub, nil },
		}

		return cmd, &testBundle{
			out:          out,
			migratorStub: migratorStub,
		}
	}

	t.Run("DownMigration", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		_, err := runCommand(ctx, t, cmd, &migrateGetOpts{Down: true, Version: []int{1}})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
-- River main migration 001 [down]
SELECT 'down 1' FROM river_table
		`), strings.TrimSpace(bundle.out.String()))
	})

	t.Run("UpMigration", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		_, err := runCommand(ctx, t, cmd, &migrateGetOpts{Up: true, Version: []int{1}})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
-- River main migration 001 [up]
SELECT 'up 1' FROM river_table
		`), strings.TrimSpace(bundle.out.String()))
	})

	t.Run("MultipleMigrations", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		_, err := runCommand(ctx, t, cmd, &migrateGetOpts{Up: true, Version: []int{1, 2, 3}})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
-- River main migration 001 [up]
SELECT 'up 1' FROM river_table

-- River main migration 002 [up]
SELECT 'up 2' FROM river_table

-- River main migration 003 [up]
SELECT 'up 3' FROM river_table
		`), strings.TrimSpace(bundle.out.String()))
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		_, err := runCommand(ctx, t, cmd, &migrateGetOpts{Schema: "custom_schema", Up: true, Version: []int{1}})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
-- River main migration 001 [up]
SELECT 'up 1' FROM custom_schema.river_table
		`), strings.TrimSpace(bundle.out.String()))
	})
}

func TestMigrateList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		migratorStub *MigratorStub
		out          *bytes.Buffer
	}

	setup := func(t *testing.T) (*migrateList, *testBundle) {
		t.Helper()

		cmd, out := withCommandBase(t, &migrateList{})

		migratorStub := &MigratorStub{}
		migratorStub.allVersionsStub = func() []rivermigrate.Migration { return testMigrationAll }
		migratorStub.existingVersionsStub = func(ctx context.Context) ([]rivermigrate.Migration, error) { return nil, nil }

		cmd.GetCommandBase().DriverProcurer = &DriverProcurerStub{
			getMigratorStub: func(config *rivermigrate.Config) (MigratorInterface, error) { return migratorStub, nil },
		}

		return cmd, &testBundle{
			out:          out,
			migratorStub: migratorStub,
		}
	}

	t.Run("NoExistingMigrations", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		_, err := runCommand(ctx, t, cmd, &migrateListOpts{})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
001 1st migration
002 2nd migration
003 3rd migration
		`), strings.TrimSpace(bundle.out.String()))
	})

	t.Run("WithExistingMigrations", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		bundle.migratorStub.existingVersionsStub = func(ctx context.Context) ([]rivermigrate.Migration, error) {
			return []rivermigrate.Migration{testMigration01, testMigration02}, nil
		}

		_, err := runCommand(ctx, t, cmd, &migrateListOpts{})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
  001 1st migration
* 002 2nd migration
  003 3rd migration
		`), strings.TrimSpace(bundle.out.String()))
	})
}

func TestVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		buf *bytes.Buffer
	}

	setup := func(t *testing.T) (*version, *testBundle) {
		t.Helper()

		cmd, buf := withCommandBase(t, &version{})

		return cmd, &testBundle{
			buf: buf,
		}
	}

	t.Run("PrintsVersion", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		_, err := runCommand(ctx, t, cmd, &versionOpts{Name: "River"})
		require.NoError(t, err)

		buildInfo, _ := debug.ReadBuildInfo()

		// `devel` on 1.25, `unknown` on versions previous to that
		require.Regexp(t, strings.TrimSpace(fmt.Sprintf(`
River version \((devel|unknown)\)
Built with %s
		`, buildInfo.GoVersion)), strings.TrimSpace(bundle.buf.String()))
	})
}

// runCommand runs a CLI command while doing some additional niceties like
// validating options.
func runCommand[TCommand Command[TOpts], TOpts CommandOpts](ctx context.Context, t *testing.T, cmd TCommand, opts TOpts) (bool, error) {
	t.Helper()

	require.NoError(t, opts.Validate())

	return cmd.Run(ctx, opts)
}

func withCommandBase[TCommand Command[TOpts], TOpts CommandOpts](t *testing.T, cmd TCommand) (TCommand, *bytes.Buffer) {
	t.Helper()

	var out bytes.Buffer
	cmd.SetCommandBase(&CommandBase{
		DriverProcurer: &DriverProcurerStub{},
		Logger:         riversharedtest.Logger(t),
		Out:            &out,
	})
	return cmd, &out
}

func TestMigrationComment(t *testing.T) {
	t.Parallel()

	require.Equal(t, "-- River main migration 001 [down]", migrationComment("main", 1, rivermigrate.DirectionDown))
	require.Equal(t, "-- River main migration 002 [up]", migrationComment("main", 2, rivermigrate.DirectionUp))
}

func TestRoundDuration(t *testing.T) {
	t.Parallel()

	mustParseDuration := func(s string) time.Duration {
		d, err := time.ParseDuration(s)
		require.NoError(t, err)
		return d
	}

	require.Equal(t, "1.33µs", roundDuration(mustParseDuration("1.332µs")).String())
	require.Equal(t, "765.62µs", roundDuration(mustParseDuration("765.625µs")).String())
	require.Equal(t, "4.42ms", roundDuration(mustParseDuration("4.422125ms")).String())
	require.Equal(t, "13.28ms", roundDuration(mustParseDuration("13.280834ms")).String())
	require.Equal(t, "234.91ms", roundDuration(mustParseDuration("234.91075ms")).String())
	require.Equal(t, "3.93s", roundDuration(mustParseDuration("3.937042s")).String())
	require.Equal(t, "34.04s", roundDuration(mustParseDuration("34.042234s")).String())
	require.Equal(t, "2m34.04s", roundDuration(mustParseDuration("2m34.042234s")).String())
}

func TestTargetVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, 1, targetVersionTranslateDefault(1))
	require.Equal(t, 2, targetVersionTranslateDefault(2))
	require.Equal(t, 3, targetVersionTranslateDefault(3))

	require.Equal(t, 0, targetVersionTranslateDefault(-2))
	require.Equal(t, -1, targetVersionTranslateDefault(-1))
	require.Equal(t, -1, targetVersionTranslateDefault(0))
}

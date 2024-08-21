package rivercli

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

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
	if m.allVersionsStub == nil {
		panic("GetVersion is not stubbed")
	}

	return m.getVersionStub(version)
}

func (m *MigratorStub) Migrate(ctx context.Context, direction rivermigrate.Direction, opts *rivermigrate.MigrateOpts) (*rivermigrate.MigrateResult, error) {
	if m.allVersionsStub == nil {
		panic("Migrate is not stubbed")
	}

	return m.migrateStub(ctx, direction, opts)
}

func (m *MigratorStub) Validate(ctx context.Context) (*rivermigrate.ValidateResult, error) {
	if m.allVersionsStub == nil {
		panic("Validate is not stubbed")
	}

	return m.validateStub(ctx)
}

var (
	testMigration01 = rivermigrate.Migration{Name: "1st migration", SQLDown: "SELECT 1", SQLUp: "SELECT 1", Version: 1} //nolint:gochecknoglobals
	testMigration02 = rivermigrate.Migration{Name: "2nd migration", SQLDown: "SELECT 1", SQLUp: "SELECT 1", Version: 2} //nolint:gochecknoglobals
	testMigration03 = rivermigrate.Migration{Name: "3rd migration", SQLDown: "SELECT 1", SQLUp: "SELECT 1", Version: 3} //nolint:gochecknoglobals

	testMigrationAll = []rivermigrate.Migration{testMigration01, testMigration02, testMigration03} //nolint:gochecknoglobals
)

type TestDriverProcurer struct{}

func (p *TestDriverProcurer) ProcurePgxV5(pool *pgxpool.Pool) riverdriver.Driver[pgx.Tx] {
	return riverpgxv5.New(pool)
}

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
			DriverProcurer: &TestDriverProcurer{},
			Name:           "River",
		})

		var out bytes.Buffer
		cli.SetOut(&out)

		return cli.BaseCommandSet(), &testBundle{
			out: &out,
		}
	}

	t.Run("DebugVerboseMutallyExclusive", func(t *testing.T) {
		t.Parallel()

		cmd, _ := setup(t)

		cmd.SetArgs([]string{"--debug", "--verbose"})
		require.EqualError(t, cmd.Execute(), `if any flags in the group [debug verbose] are set none of the others can be; [debug verbose] were all set`)
	})

	t.Run("MigrateDownMissingDatabaseURL", func(t *testing.T) {
		t.Parallel()

		cmd, _ := setup(t)

		cmd.SetArgs([]string{"migrate-down"})
		require.EqualError(t, cmd.Execute(), `required flag(s) "database-url" not set`)
	})

	t.Run("VersionFlag", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		cmd.SetArgs([]string{"--version"})
		require.NoError(t, cmd.Execute())

		buildInfo, _ := debug.ReadBuildInfo()

		require.Equal(t, strings.TrimSpace(fmt.Sprintf(`
River version (unknown)
Built with %s
		`, buildInfo.GoVersion)), strings.TrimSpace(bundle.out.String()))
	})

	t.Run("VersionSubcommand", func(t *testing.T) {
		t.Parallel()

		cmd, bundle := setup(t)

		cmd.SetArgs([]string{"version"})
		require.NoError(t, cmd.Execute())

		buildInfo, _ := debug.ReadBuildInfo()

		require.Equal(t, strings.TrimSpace(fmt.Sprintf(`
River version (unknown)
Built with %s
		`, buildInfo.GoVersion)), strings.TrimSpace(bundle.out.String()))
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

		cmd.GetCommandBase().GetMigrator = func(config *rivermigrate.Config) MigratorInterface { return migratorStub }

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

		require.Equal(t, strings.TrimSpace(fmt.Sprintf(`
River version (unknown)
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
		Logger: riversharedtest.Logger(t),
		Out:    &out,

		GetMigrator: func(config *rivermigrate.Config) MigratorInterface { return &MigratorStub{} },
	})
	return cmd, &out
}

func TestMigrationComment(t *testing.T) {
	t.Parallel()

	require.Equal(t, "-- River migration 001 [down]", migrationComment(1, rivermigrate.DirectionDown))
	require.Equal(t, "-- River migration 002 [up]", migrationComment(2, rivermigrate.DirectionUp))
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

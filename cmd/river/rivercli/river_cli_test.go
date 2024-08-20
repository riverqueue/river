package rivercli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

func TestMigrateList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		buf          *bytes.Buffer
		migratorStub *MigratorStub
	}

	setup := func(t *testing.T) (*migrateList, *testBundle) {
		t.Helper()

		cmd, buf := withMigrateBase(t, &migrateList{})

		migratorStub := &MigratorStub{}
		migratorStub.allVersionsStub = func() []rivermigrate.Migration { return testMigrationAll }
		migratorStub.existingVersionsStub = func(ctx context.Context) ([]rivermigrate.Migration, error) { return nil, nil }

		cmd.GetCommandBase().GetMigrator = func(config *rivermigrate.Config) MigratorInterface { return migratorStub }

		return cmd, &testBundle{
			buf:          buf,
			migratorStub: migratorStub,
		}
	}

	t.Run("NoExistingMigrations", func(t *testing.T) {
		t.Parallel()

		migrateList, bundle := setup(t)

		_, err := migrateList.Run(ctx, &migrateListOpts{})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
001 1st migration
002 2nd migration
003 3rd migration
		`), strings.TrimSpace(bundle.buf.String()))
	})

	t.Run("WithExistingMigrations", func(t *testing.T) {
		t.Parallel()

		migrateList, bundle := setup(t)

		bundle.migratorStub.existingVersionsStub = func(ctx context.Context) ([]rivermigrate.Migration, error) {
			return []rivermigrate.Migration{testMigration01, testMigration02}, nil
		}

		_, err := migrateList.Run(ctx, &migrateListOpts{})
		require.NoError(t, err)

		require.Equal(t, strings.TrimSpace(`
  001 1st migration
* 002 2nd migration
  003 3rd migration
		`), strings.TrimSpace(bundle.buf.String()))
	})
}

func withMigrateBase[TCommand Command[TOpts], TOpts CommandOpts](t *testing.T, cmd TCommand) (TCommand, *bytes.Buffer) {
	t.Helper()

	var buf bytes.Buffer
	cmd.SetCommandBase(&CommandBase{
		Logger: riversharedtest.Logger(t),
		Out:    &buf,

		GetMigrator: func(config *rivermigrate.Config) MigratorInterface { return &MigratorStub{} },
	})
	return cmd, &buf
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

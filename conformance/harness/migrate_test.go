package harness

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// versionRange returns the versions from first to last.
func versionRange(first, last int) []int {
	versions := []int{}
	for version := first; version <= last; version++ {
		versions = append(versions, version)
	}
	return versions
}

// createSchema creates a Postgres schema the scenario drops when it ends.
func createSchema(t *testing.T, env *Env, schema string) {
	t.Helper()

	env.DB.Exec(t, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	t.Cleanup(func() { env.DB.Exec(t, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE") })
}

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestMigrate(t *testing.T) {
	t.Parallel()

	// One implementation migrates a schema other than the default, and the
	// other inserts and works jobs in it. Both accept the longest schema
	// name River supports and reject a longer one, and a migrated schema
	// whose name has capitals is seen as migrated rather than folded to
	// lowercase.
	t.Run("CustomSchema", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, &EnvOpts{Drivers: []string{DriverPostgres}, NoMigrate: true}, func(t *testing.T, env *Env, migrator, worker *Adapter) {
			schema := env.DB.Schema + "_custom"
			createSchema(t, env, schema)
			migrator.Migrate(t, protocol.MigrateParams{Schema: schema})

			inserted := worker.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{echo("custom schema", protocol.BehaviorComplete)}, Schema: schema})[0].Job
			list := func(adapter *Adapter) []protocol.Job {
				return adapter.List(t, protocol.ListParams{IDs: []int64{inserted.ID}, Schema: schema}).Jobs
			}
			require.Equal(t, []protocol.Job{inserted}, list(migrator))

			worker.Start(t, protocol.StartParams{ClientID: "custom-schema", Schema: schema})
			WaitFor(t, "the job in the custom schema completing", workWait, func() bool { return list(migrator)[0].State == "completed" })
			worker.Stop(t, protocol.StopParams{})
			require.Equal(t, list(worker), list(migrator))

			longest := env.DB.Schema + strings.Repeat("s", 46-len(env.DB.Schema))
			createSchema(t, env, longest)
			migrator.Migrate(t, protocol.MigrateParams{Schema: longest})
			require.Positive(t, worker.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{echo("longest schema", protocol.BehaviorComplete)}, Schema: longest})[0].Job.ID)
			err := worker.Call(protocol.MethodInsert, &protocol.InsertParams{
				Jobs: []protocol.InsertJob{echo("schema too long", protocol.BehaviorComplete)}, Schema: longest + "s",
			}, nil)
			RequireErrorCode(t, err, protocol.CodeRejected)

			mixedCase := "MixedCase" + env.DB.Schema
			createSchema(t, env, mixedCase)
			require.NotEmpty(t, migrator.Migrate(t, protocol.MigrateParams{Schema: mixedCase}))
			require.Empty(t, worker.Migrate(t, protocol.MigrateParams{Schema: mixedCase}))
		})
	})

	// For every version, one implementation migrates a database to it and
	// the other upgrades it to the latest, works on it, and migrates it back
	// down; each must see the versions the other applied.
	t.Run("History", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, &EnvOpts{NoMigrate: true}, func(t *testing.T, env *Env, initializer, upgrader *Adapter) {
			latest := len(env.Reference.Migrate(t, protocol.MigrateParams{}))
			require.Positive(t, latest)
			env.Reference.Migrate(t, protocol.MigrateParams{Direction: "down", TargetVersion: new(-1)})

			for version := 1; version <= latest; version++ {
				// On Postgres each version gets a schema of its own, since an
				// adapter's cached statements can't outlive a table rebuilt
				// under them.
				var schema string
				if env.Driver == DriverPostgres {
					schema = fmt.Sprintf("%s_v%d", env.DB.Schema, version)
					createSchema(t, env, schema)
				}
				down := protocol.MigrateParams{Direction: "down", Schema: schema, TargetVersion: new(-1)}
				migrateUp := protocol.MigrateParams{Schema: schema}

				require.Equal(t, versionRange(1, version), initializer.Migrate(t, protocol.MigrateParams{Schema: schema, TargetVersion: &version}))
				require.Equal(t, versionRange(1, version), env.DB.MigrationVersions(t, schema))

				require.Equal(t, versionRange(version+1, latest), upgrader.Migrate(t, migrateUp), "upgrading from %d", version)
				inserted := upgrader.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{echo("historical migration", protocol.BehaviorComplete)}, Schema: schema})[0].Job
				require.Equal(t, []protocol.Job{inserted}, initializer.List(t, protocol.ListParams{IDs: []int64{inserted.ID}, Schema: schema}).Jobs)

				initializer.Migrate(t, protocol.MigrateParams{Direction: "down", Schema: schema, TargetVersion: &version})
				require.Equal(t, versionRange(1, version), env.DB.MigrationVersions(t, schema))
				require.Equal(t, versionRange(version+1, latest), upgrader.Migrate(t, migrateUp), "upgrading again from %d", version)
				upgrader.Migrate(t, down)
				require.Empty(t, env.DB.MigrationVersions(t, schema))
			}
		})
	})

	// One implementation rebuilds the schema from nothing and the other's
	// runtime works on it.
	t.Run("MigratorThenRuntime", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, migrator, runtime *Adapter) {
			migrator.Migrate(t, protocol.MigrateParams{Direction: "down", TargetVersion: new(-1)})
			require.Empty(t, env.DB.MigrationVersions(t, ""))
			applied := migrator.Migrate(t, protocol.MigrateParams{})
			require.Equal(t, versionRange(1, len(applied)), applied)

			inserted := runtime.InsertJob(t, echo("runtime on another migrator's schema", protocol.BehaviorComplete))
			requireWorkedOnceBy(t, workOne(t, env, runtime, "migrated-runtime", inserted.ID), "migrated-runtime")
		})
	})
}

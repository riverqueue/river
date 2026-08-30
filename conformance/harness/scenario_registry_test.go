package harness_test

const (
	scenarioOwnerMixed         = "TestMixedConformance"
	scenarioOwnerPerformance   = "TestPerformanceGate"
	scenarioOwnerSQLiteRuntime = "TestMixedSQLiteRuntimeConformance"
	scenarioOwnerSQLiteStorage = "TestMixedSQLiteConformance"
	scenarioOwnerSoak          = "TestMixedSoak"
)

type scenarioBinding struct {
	owner   string
	profile string
	tier    string
}

// scenarioRegistry is the executable source of truth for conformance
// scenarios. Each owning test must report every bound scenario as passed before
// it returns successfully; artifact validation separately requires core.json to
// contain this exact set with matching tiers.
var scenarioRegistry = map[string]scenarioBinding{ //nolint:gochecknoglobals // shared executable catalog
	"adapter_handshake_and_capabilities":                {owner: scenarioOwnerMixed, tier: "codec"},
	"barrier_wait_and_release":                          {owner: scenarioOwnerMixed, tier: "runtime"},
	"bulk_delete_safety":                                {owner: scenarioOwnerMixed, tier: "storage"},
	"candidate_insert_reference_work":                   {owner: scenarioOwnerMixed, tier: "mixed"},
	"candidate_migrator_reference_runtime":              {owner: scenarioOwnerMixed, tier: "storage"},
	"completion_batching":                               {owner: scenarioOwnerMixed, tier: "performance"},
	"cooperative_remote_cancellation":                   {owner: scenarioOwnerMixed, tier: "runtime"},
	"copy_from_both_implementations":                    {owner: scenarioOwnerMixed, tier: "storage"},
	"cross_language_unique_conflict":                    {owner: scenarioOwnerMixed, tier: "codec"},
	"custom_schema_candidate_migrate_reference_work":    {owner: scenarioOwnerMixed, tier: "mixed"},
	"custom_schema_reference_migrate_candidate_work":    {owner: scenarioOwnerMixed, tier: "mixed"},
	"deterministic_retry_clock_rng":                     {owner: scenarioOwnerMixed, tier: "codec"},
	"differential_job_crud":                             {owner: scenarioOwnerMixed, tier: "storage"},
	"differential_job_list_filters_and_cursors":         {owner: scenarioOwnerMixed, tier: "storage"},
	"differential_queue_crud":                           {owner: scenarioOwnerMixed, tier: "storage"},
	"dynamic_queue_add_reconfigure_remove":              {owner: scenarioOwnerMixed, tier: "runtime"},
	"error_handler_cancel_override":                     {owner: scenarioOwnerMixed, tier: "runtime"},
	"extension_hook_middleware_order":                   {owner: scenarioOwnerMixed, tier: "runtime"},
	"external_terminal_completion_race":                 {owner: scenarioOwnerMixed, tier: "mixed"},
	"historical_migration_down_up":                      {owner: scenarioOwnerMixed, tier: "storage"},
	"ignored_cancellation_hard_abort":                   {owner: scenarioOwnerMixed, tier: "chaos"},
	"job_row_round_trip_all_fields":                     {owner: scenarioOwnerMixed, tier: "codec"},
	"listener_backend_disconnect_reconnect":             {owner: scenarioOwnerMixed, tier: "chaos"},
	"lost_notification_poll_recovery":                   {owner: scenarioOwnerMixed, tier: "chaos"},
	"mixed_connection_pool_bound":                       {owner: scenarioOwnerSoak, tier: "performance"},
	"mixed_leader_failover_both_directions":             {owner: scenarioOwnerMixed, tier: "mixed"},
	"mixed_request_resign_terms":                        {owner: scenarioOwnerMixed, tier: "mixed"},
	"mixed_skip_locked_competition":                     {owner: scenarioOwnerMixed, tier: "mixed"},
	"mixed_soak":                                        {owner: scenarioOwnerSoak, tier: "performance"},
	"mixed_unknown_kind_error":                          {owner: scenarioOwnerMixed, tier: "mixed"},
	"notification_only_wakeups":                         {owner: scenarioOwnerMixed, tier: "mixed"},
	"panic_attempt_trace":                               {owner: scenarioOwnerMixed, tier: "runtime"},
	"pause_resume_notification":                         {owner: scenarioOwnerMixed, tier: "mixed"},
	"periodic_run_on_start":                             {owner: scenarioOwnerMixed, tier: "runtime"},
	"process_kill_restart_and_rescue":                   {owner: scenarioOwnerMixed, tier: "chaos"},
	"reference_insert_candidate_work":                   {owner: scenarioOwnerMixed, tier: "mixed"},
	"reference_migrator_candidate_runtime":              {owner: scenarioOwnerMixed, tier: "storage"},
	"refetched_attempt_cancellation":                    {owner: scenarioOwnerMixed, tier: "runtime"},
	"release_enqueue_performance":                       {owner: scenarioOwnerPerformance, tier: "performance"},
	"release_mixed_performance":                         {owner: scenarioOwnerPerformance, tier: "performance"},
	"release_worker_performance":                        {owner: scenarioOwnerPerformance, tier: "performance"},
	"remote_cancel_notification":                        {owner: scenarioOwnerMixed, tier: "mixed"},
	"remote_queue_subscription_events":                  {owner: scenarioOwnerMixed, tier: "mixed"},
	"resumable_retry":                                   {owner: scenarioOwnerMixed, tier: "runtime"},
	"single_implementation_worker_outcomes":             {owner: scenarioOwnerMixed, tier: "runtime"},
	"snooze_once_metadata_transition":                   {owner: scenarioOwnerMixed, tier: "runtime"},
	"timeout_cancellation":                              {owner: scenarioOwnerMixed, tier: "runtime"},
	"transaction_abort_rollback_visibility":             {owner: scenarioOwnerMixed, tier: "storage"},
	"transaction_commit_visibility":                     {owner: scenarioOwnerMixed, tier: "storage"},
	"transaction_rollback_visibility":                   {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_batch_insertion":                     {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_completion":                          {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_cross_language_cancel":               {owner: scenarioOwnerMixed, tier: "mixed"},
	"transactional_crud_commit_rollback":                {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_fast_batch_insertion":                {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_insert_notification_commit_only":     {owner: scenarioOwnerMixed, tier: "mixed"},
	"transactional_queue_operations":                    {owner: scenarioOwnerMixed, tier: "storage"},
	"typed_batch_insertion":                             {owner: scenarioOwnerMixed, tier: "storage"},
	"unique_hash_goldens":                               {owner: scenarioOwnerMixed, tier: "codec"},
	"sqlite_batch_atomicity":                            {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "storage"},
	"sqlite_deterministic_retry_unique":                 {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "codec"},
	"sqlite_insert_get_unique_cross_language":           {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "mixed"},
	"sqlite_job_crud":                                   {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "storage"},
	"sqlite_migration_cross_language":                   {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "storage"},
	"sqlite_profile_handshake":                          {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "codec"},
	"sqlite_timestamp_rounding_ordering":                {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "codec"},
	"sqlite_transactions":                               {owner: scenarioOwnerSQLiteStorage, profile: "portable-storage-v1", tier: "storage"},
	"sqlite_runtime_attempted_by_ordering":              {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_competing_workers":                  {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_cross_language_work":                {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_extensions_resumable_subscriptions": {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "runtime"},
	"sqlite_runtime_external_terminal_completion_race":  {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_leadership_failover":                {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "runtime"},
	"sqlite_runtime_lifecycle_shutdown":                 {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "runtime"},
	"sqlite_runtime_notification_wakeups":               {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_periodic_scheduler":                 {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "runtime"},
	"sqlite_runtime_poll_only_recovery":                 {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "runtime"},
	"sqlite_runtime_profile_handshake":                  {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "codec"},
	"sqlite_runtime_queue_crud_reconfigure_pause":       {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_remote_cancellation":                {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_remote_queue_subscription_events":   {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_transactional_notification":         {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
	"sqlite_runtime_unknown_kind_error":                 {owner: scenarioOwnerSQLiteRuntime, profile: "sqlite-runtime-v1", tier: "mixed"},
}

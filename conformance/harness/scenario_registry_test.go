package harness_test

const (
	scenarioOwnerMixed = "TestMixedConformance"
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
	"adapter_handshake_and_capabilities":             {owner: scenarioOwnerMixed, tier: "codec"},
	"barrier_wait_and_release":                       {owner: scenarioOwnerMixed, tier: "runtime"},
	"bulk_delete_safety":                             {owner: scenarioOwnerMixed, tier: "storage"},
	"candidate_insert_reference_work":                {owner: scenarioOwnerMixed, tier: "mixed"},
	"candidate_migrator_reference_runtime":           {owner: scenarioOwnerMixed, tier: "storage"},
	"candidate_process_kill_reference_rescue":        {owner: scenarioOwnerMixed, tier: "chaos"},
	"claim_order":                                    {owner: scenarioOwnerMixed, tier: "mixed"},
	"clock_boundary_scheduling":                      {owner: scenarioOwnerMixed, tier: "mixed"},
	"completion_batching":                            {owner: scenarioOwnerMixed, tier: "performance"},
	"cross_language_cancel_retry_race":               {owner: scenarioOwnerMixed, tier: "mixed"},
	"cross_language_unique_conflict":                 {owner: scenarioOwnerMixed, tier: "codec"},
	"custom_schema_candidate_migrate_reference_work": {owner: scenarioOwnerMixed, tier: "mixed"},
	"custom_schema_reference_migrate_candidate_work": {owner: scenarioOwnerMixed, tier: "mixed"},
	"differential_job_crud":                          {owner: scenarioOwnerMixed, tier: "storage"},
	"differential_job_list_filters_and_cursors":      {owner: scenarioOwnerMixed, tier: "storage"},
	"differential_queue_crud":                        {owner: scenarioOwnerMixed, tier: "storage"},
	"dynamic_queue_add_reconfigure_remove":           {owner: scenarioOwnerMixed, tier: "runtime"},
	"error_handler_cancel_override":                  {owner: scenarioOwnerMixed, tier: "runtime"},
	"exhausted_job_retry":                            {owner: scenarioOwnerMixed, tier: "mixed"},
	"extension_hook_middleware_order":                {owner: scenarioOwnerMixed, tier: "runtime"},
	"external_terminal_completion_race":              {owner: scenarioOwnerMixed, tier: "mixed"},
	"historical_migration_down_up":                   {owner: scenarioOwnerMixed, tier: "storage"},
	"job_cleaner_queue_filters":                      {owner: scenarioOwnerMixed, tier: "storage"},
	"job_list_cursor_interchange":                    {owner: scenarioOwnerMixed, tier: "storage"},
	"job_row_round_trip_all_fields":                  {owner: scenarioOwnerMixed, tier: "codec"},
	"panic_attempt_trace":                            {owner: scenarioOwnerMixed, tier: "runtime"},
	"periodic_run_on_start":                          {owner: scenarioOwnerMixed, tier: "runtime"},
	"periodic_unique_cross_engine":                   {owner: scenarioOwnerMixed, tier: "runtime"},
	"pool_pressure_completion":                       {owner: scenarioOwnerMixed, tier: "performance"},
	"reference_insert_candidate_work":                {owner: scenarioOwnerMixed, tier: "mixed"},
	"reference_migrator_candidate_runtime":           {owner: scenarioOwnerMixed, tier: "storage"},
	"reference_process_kill_candidate_rescue":        {owner: scenarioOwnerMixed, tier: "chaos"},
	"refetched_attempt_cancellation":                 {owner: scenarioOwnerMixed, tier: "runtime"},
	"resumable_cross_engine_cursor":                  {owner: scenarioOwnerMixed, tier: "mixed"},
	"resumable_retry":                                {owner: scenarioOwnerMixed, tier: "runtime"},
	"resumable_validation":                           {owner: scenarioOwnerMixed, tier: "runtime"},
	"scheduler_unique_conflict_discard":              {owner: scenarioOwnerMixed, tier: "mixed"},
	"single_implementation_worker_outcomes":          {owner: scenarioOwnerMixed, tier: "runtime"},
	"snooze_once_metadata_transition":                {owner: scenarioOwnerMixed, tier: "runtime"},
	"stuck_job_detection":                            {owner: scenarioOwnerMixed, tier: "runtime"},
	"timeout_cancellation":                           {owner: scenarioOwnerMixed, tier: "runtime"},
	"transaction_abort_rollback_visibility":          {owner: scenarioOwnerMixed, tier: "storage"},
	"transaction_commit_visibility":                  {owner: scenarioOwnerMixed, tier: "storage"},
	"transaction_rollback_visibility":                {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_batch_insertion":                  {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_completion":                       {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_cross_language_cancel":            {owner: scenarioOwnerMixed, tier: "mixed"},
	"transactional_crud_commit_rollback":             {owner: scenarioOwnerMixed, tier: "storage"},
	"transactional_queue_operations":                 {owner: scenarioOwnerMixed, tier: "storage"},
	"typed_batch_insertion":                          {owner: scenarioOwnerMixed, tier: "storage"},
	"unique_column_bytes":                            {owner: scenarioOwnerMixed, tier: "codec"},
	"unique_skip_keeps_existing_kind":                {owner: scenarioOwnerMixed, tier: "storage"},
	"unsafe_int64_job_ids_rpc_list_cursors":          {owner: scenarioOwnerMixed, tier: "codec"},
}

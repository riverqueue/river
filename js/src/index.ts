/// <reference lib="esnext.temporal" preserve="true" />
// The public declarations use the global `Temporal` types. The preserved
// reference loads them for TypeScript consumers without a `lib` setting.

export { Client } from "./client.js";
export type {
  CheckedInsertManyItems,
  ClientConstructor,
  InsertClient,
  InsertManyItem,
  InsertManyResults,
  InsertResult,
  JobOperations,
  QueueOperations,
  TransactionOptions,
} from "./client.js";
export { cron } from "./cron.js";
export type { CronOptions, CronSchedule } from "./cron.js";
export type {
  ClientDriver,
  DriverCapability,
  JobListOrderBy,
  LeaderTerm,
  RegisteredTransaction,
  RiverTransactionRegistry,
  SortDirection,
} from "./driver.js";
export {
  BackendMismatchError,
  ConfigurationError,
  DatabaseOperationError,
  ExtensionError,
  isRetryableError,
  JobAbortedError,
  JobAttemptFinishedError,
  JobCancelledError,
  JobRunningError,
  JobStuckError,
  JobTimeoutError,
  LifecycleError,
  MigrationError,
  PayloadValidationError,
  RIVER_ERROR_CODE,
  RiverError,
  SubscriptionLagError,
  TransactionScopeError,
  UnknownJobKindError,
  UnsupportedCapabilityError,
  ValidationError,
} from "./errors.js";
export type {
  DatabaseOperationErrorOptions,
  MigrationErrorOptions,
  PayloadValidationPhase,
  RiverErrorCode,
  RiverErrorOptions,
  RiverErrorSubclassOptions,
  TransactionScopeErrorReason,
} from "./errors.js";
export { EventSubscription } from "./events.js";
export type {
  EventLoopDelayEvent,
  JobCancelledEvent,
  JobCompletedEvent,
  JobEvent,
  JobEventKind,
  JobFailedEvent,
  JobInterruptedEvent,
  JobRaceEvent,
  JobSnoozedEvent,
  JobStartedEvent,
  JobStuckEvent,
  LeaderEvent,
  MaintenanceFailedEvent,
  MaintenanceSucceededEvent,
  QueueEvent,
  QueueEventKind,
  QueueRemovedEvent,
  RiverEvent,
  RiverEventBase,
  RiverEventKind,
  SubscribedEvent,
  SubscribeOptions,
  SubscriptionLagEvent,
} from "./events.js";
export type {
  ErrorHandlerContext,
  ErrorHandlerResult,
  InsertContext,
  InsertMiddleware,
  InsertRequest,
  RiverErrorHandler,
  RiverHooks,
  RiverPlugin,
  WorkAttemptResult,
  WorkMiddleware,
} from "./extensions.js";
export { defineJob, isJobDefinition } from "./job-definition.js";
export type {
  DecodedJobInput,
  DecoderJobDefinitionConfig,
  DefineJobWithInput,
  JobDefinition,
  JobDefinitionArgs,
  JobDefinitionInput,
  JobDefinitionOptions,
  JobDefinitionTypeError,
  JsonCompatible,
  SchemaJobDefinitionConfig,
  StandardSchemaInput,
  StandardSchemaIssue,
  StandardSchemaOutput,
  StandardSchemaResult,
  StandardSchemaV1,
  UncheckedJobDefinitionConfig,
} from "./job-definition.js";
export type {
  InsertOptions,
  NormalizedInsertOptions,
  NormalizedUniqueOptions,
  UniqueOptions,
} from "./insert-options.js";
export type {
  LogAttributes,
  Logger,
  LogLevel,
  WorkLogFunction,
  WorkLogger,
} from "./logger.js";
export { consoleLogger } from "./logger.js";
export type { RiverMetric } from "./metrics.js";
export {
  JOB_STATE,
  jobFromJsonValue,
  jobToJsonValue,
  MAX_ATTEMPTS_DEFAULT,
  PRIORITY_DEFAULT,
  QUEUE_DEFAULT,
} from "./job.js";
export type {
  AttemptError,
  AttemptErrorJson,
  JobRow,
  JobRowJson,
  JobState,
} from "./job.js";
export {
  exactJsonNumber,
  isExactJsonNumber,
  isJsonNumber,
  jsonNumberToBigInt,
  JsonValueError,
  parseJson,
  parseJsonObject,
  stringifyJson,
  toJsonObject,
  toJsonValue,
} from "./json.js";
export type { ExactJsonNumber, JsonObject, JsonValue } from "./json.js";
export { periodicJob, PeriodicJobs } from "./periodic.js";
export type {
  DurablePeriodicJob,
  PeriodicJob,
  PeriodicJobArgs,
  PeriodicJobHandle,
  PeriodicJobInsert,
  PeriodicJobOptions,
  PeriodicJobsStartParams,
  PeriodicJobTiming,
  PeriodicSchedule,
} from "./periodic.js";
export { Resumable } from "./resumable.js";
export type { ResumableCheckpointOptions } from "./resumable.js";
export type {
  JobListOptions,
  JobListResult,
  JobDeleteManyOptions,
  JobUpdateOptions,
  QueueListOptions,
  QueueListResult,
  QueueRow,
  QueueUpdateOptions,
} from "./query.js";
export {
  currentWorkContext,
  recordOutput,
  setMetadata,
  RunHandle,
} from "./runtime.js";
export type {
  CurrentWorkContext,
  EventLoopDelayObservation,
  JobStuckHandler,
  JobStuckHandlerParams,
  JobStuckHandlerResult,
  QueueRuntimeDiagnostics,
  RetryPolicy,
  RunDiagnostics,
  RunState,
} from "./runtime.js";
export type {
  ClientOptions,
  DurationInput,
  EventLoopDelayOptions,
  MaintenanceOptions,
  QueueConfig,
  StopOptions,
} from "./options.js";
export type {
  MaintenanceDiagnostics,
  MaintenanceServiceName,
  ReindexerSchedule,
} from "./services.js";
export { REINDEXER_INDEX_NAMES_DEFAULT } from "./services.js";
export { assertRuntimeSupport } from "./runtime-support.js";
export { cancel, complete, discard, snooze, Workers } from "./worker.js";
export type {
  CancelOutcome,
  CompleteOutcome,
  DiscardOutcome,
  ExecutorWorkerRegistration,
  InProcessWorkerRegistration,
  Job,
  SnoozeOutcome,
  WorkAttemptContext,
  WorkContext,
  WorkHandler,
  WorkHandlerFactory,
  WorkExecution,
  WorkExecutor,
  WorkExecutorAbortOptions,
  WorkExecutorAbortResult,
  WorkExecutorHandle,
  WorkExecutorTarget,
  WorkerHooks,
  NormalizedWorkerOptions,
  WorkerOptions,
  WorkerPlugin,
  WorkerRegistration,
  WorkOutcome,
} from "./worker.js";

/// <reference lib="esnext.temporal" preserve="true" />
// The public declarations use the global `Temporal` types. The preserved
// reference loads them for TypeScript consumers without a `lib` setting.

/**
 * Explicitly unstable semantic SPI for first-party River backend packages.
 *
 * Application code must not implement or call these operations. This subpath
 * may change incompatibly before River's JavaScript implementation reaches
 * 1.0, independently of the ordinary producer and worker API.
 */
export { jobCompletionKey } from "./driver.js";
export {
  driverMigrationTarget,
  registerDriver,
} from "./internal/driver-registry.js";
export {
  POSTGRES_CAPABILITIES_SQL,
  postgresCapabilitiesFromRow,
  UNIQUE_INSERT_NONCE_KEY,
  uniqueInsertConflictSql,
} from "./internal/postgres-capabilities.js";
export type {
  PostgresCapabilities,
  UniqueInsertMode,
} from "./internal/postgres-capabilities.js";
export { recordQueueMetadataText } from "./internal/queue-metadata-text.js";
export { queueMetadataUpdate } from "./internal/queue-metadata-update.js";
export {
  abortableDelay,
  interruptibleDelay,
  LinkedAbortSignal,
} from "./internal/abort.js";
export { quoteIdentifier } from "./internal/sql.js";
export type { OperationTimeout, RuntimeTimer } from "./internal/backoff.js";
export { ManualTimer } from "./internal/manual-timer.js";
export type {
  ManualTimeout,
  ManualTimerEntry,
} from "./internal/manual-timer.js";
export { overrideRuntimeTiming } from "./runtime.js";
export type { RuntimeTiming } from "./runtime.js";
export { PilotClient } from "./pilot-client.js";
export { workerRegistration } from "./worker.js";
export type {
  PilotClientConstructor,
  PilotClientOptions,
} from "./pilot-client.js";
export type {
  DriverMigrationTarget,
  DriverRecord,
  FinalizedJobDeleteParams,
  PeerClaimContext,
  PeerOutcome,
  Pilot,
  PilotAttempts,
  PilotCompleteContext,
  PilotDatabase,
  PilotFactory,
  PilotHost,
  PilotInsertContext,
  PilotInsertReplacement,
  PilotInterceptors,
  PilotJobContext,
  PilotQueueOptions,
  PilotService,
  PilotRescueContext,
  PilotStuckContext,
  PilotTransactionContext,
  PreparedInsertParams,
  ProducerClaimContext,
  ProducerClaimNext,
  ProducerConfiguration,
  ProducerKeepAliveContext,
  ProducerShutdownContext,
  ProducerStartContext,
  PilotProducer,
} from "./pilot.js";
export {
  decodeAttemptError,
  decodeAttemptErrors,
  decodeJobState,
} from "./driver-codecs.js";
export { createJobArgsTransformPlugin } from "./job-args-transform.js";
export { createJobInsertMetadataTransformPlugin } from "./job-insert-metadata-transform.js";
export type {
  JobInsertMetadataTransformInput,
  JobInsertMetadataTransformer,
  JobInsertMetadataTransformPlugin,
  JobInsertMetadataTransformResult,
} from "./job-insert-metadata-transform.js";
export { decodeJobArgs } from "./job-definition.js";
export {
  toMilliseconds as durationToMilliseconds,
  type DurationInput,
  type DurationRules,
} from "./internal/duration.js";
export { postgresTimestamp } from "./internal/timestamp.js";
export type {
  DurablePeriodicJobUpsert,
  PeriodicJobStore,
} from "./periodic-job-store.js";
export {
  canonicalDecimal,
  canonicalEqualityDecimal,
  compareUtf8,
  jsonValuesEqual,
  numberRoundTrips,
  sjsonKey,
} from "./json.js";
export {
  decodeJobListCursor,
  encodeJobListCursor,
  jobListCursorValue,
  jobListKeyset,
  jobListKeysetSql,
} from "./query.js";
export { createResumable, finishResumable } from "./resumable.js";
export { buildUniqueKey, encodeUniqueArgs } from "./client.js";
export {
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
} from "./unique-bitmask.js";
export type {
  BackendResult,
  DriverAttemptError,
  DriverInsertResult,
  InsertDriver,
  InsertDriverOptions,
  JobCancellationNotice,
  JobClaimOptions,
  JobClaimParams,
  JobClaimQueue,
  JobClaimResult,
  JobCompletionCommand,
  JobCompletionResult,
  JobDeleteManyParams,
  JobDeleteResult,
  JobInsertParams,
  JobListAfter,
  JobListCursorValue,
  JobListKeyset,
  JobListOrderBy,
  JobListParams,
  JobListTimeField,
  JobUpdateParams,
  LeaderTerm,
  RuntimeJobCleanupParams,
  RuntimeJobRescue,
  RuntimeLeader,
  RuntimeMaintenanceBatch,
  RuntimeNotification,
  RuntimeScheduleParams,
  RuntimeWaitOptions,
  QueueListParams,
  QueueRow,
  QueueUpdateParams,
  RuntimeDriver,
  SortDirection,
} from "./driver.js";
export type {
  JobArgsInsertTransformInput,
  JobArgsInsertTransformOutput,
  JobArgsReadTransformInput,
  JobArgsTransformer,
  JobArgsTransformPlugin,
  ReadonlyJsonObject,
  ReadonlyJsonValue,
} from "./job-args-transform.js";

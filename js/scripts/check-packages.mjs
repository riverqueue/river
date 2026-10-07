import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  cp,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, posix, relative, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  assertNoDeniedContent,
  assertPortableArchivePath,
  deniedSubstrings,
} from "./package-guard.mjs";

const execFileAsync = promisify(execFile);
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const examplesOnly = process.argv.includes("--examples-only");
// The only JSON a package may ship: its manifest and runtime migration data.
// Anything else, like Go-generated test goldens, is test data.
const packagedJsonFiles = new Set(["migrations/manifest.json", "package.json"]);
const rootPackageJson = JSON.parse(
  await readFile(join(repositoryRoot, "package.json"), "utf8")
);
const packageSpecs = [
  { directory: ".", name: "riverqueue" },
  { directory: "migrate", name: "@riverqueue/migrate" },
  { directory: "driver/pg", name: "@riverqueue/driver-pg" },
  { directory: "driver/prisma", name: "@riverqueue/driver-prisma" },
  { directory: "driver/sqlite", name: "@riverqueue/driver-sqlite" },
  { directory: "worker-threads", name: "@riverqueue/worker-threads" },
  { directory: "test", name: "@riverqueue/test" },
  { directory: "cli", name: "@riverqueue/cli" },
];
const exampleSpecs = [
  {
    directory: "graceful-shutdown",
    name: "riverqueue-example-graceful-shutdown",
    requiresDatabase: false,
  },
  {
    directory: "hooks-metrics",
    name: "riverqueue-example-hooks-metrics",
    requiresDatabase: false,
  },
  {
    directory: "mixed-language",
    name: "riverqueue-example-mixed-language",
    requiresDatabase: true,
  },
  {
    directory: "node-postgres",
    name: "riverqueue-example-node-postgres",
    requiresDatabase: true,
  },
  {
    directory: "pg-worker",
    name: "riverqueue-example-pg-worker",
    requiresDatabase: true,
  },
  {
    directory: "prisma",
    name: "riverqueue-example-prisma",
    requiresDatabase: true,
  },
  {
    directory: "sqlite-worker",
    name: "riverqueue-example-sqlite-worker",
    requiresDatabase: false,
  },
  {
    directory: "worker-thread-cpu",
    name: "riverqueue-example-worker-thread-cpu",
    requiresDatabase: false,
  },
];

const temporaryDirectory = await mkdtemp(
  join(tmpdir(), "riverqueue-packages-")
);

try {
  const archivesDirectory = join(temporaryDirectory, "archives");
  const archiveByPackageName = new Map();
  await mkdir(archivesDirectory);

  for (const packageSpec of packageSpecs) {
    const packageDirectory = resolve(repositoryRoot, packageSpec.directory);
    const { stdout } = await run(
      "pnpm",
      ["pack", "--json", "--pack-destination", archivesDirectory],
      { cwd: packageDirectory }
    );
    const packResult = parseTrailingJson(stdout);

    assert.equal(packResult.name, packageSpec.name);
    const archivePath = packResult.filename;
    archiveByPackageName.set(packageSpec.name, archivePath);

    if (!examplesOnly) {
      await run(resolveBin("publint"), ["run", archivePath, "--strict"]);
      // `cjs-resolves-to-esm` is expected for an ESM-only package; the
      // CommonJS consumer check below proves `require(esm)` instead.
      await run(resolveBin("attw"), [
        archivePath,
        "--profile",
        "node16",
        "--ignore-rules",
        "cjs-resolves-to-esm",
      ]);
      await inspectArchive(packageSpec, archivePath);
    }
  }

  if (!examplesOnly) {
    await verifyConsumers(archiveByPackageName);
    await verifyVersionSkewRejected(archiveByPackageName);
  }
  await verifyExamples(archiveByPackageName);
  process.stdout.write(
    `validated ${packageSpecs.length} packed packages and ${exampleSpecs.length} packed examples\n`
  );
} finally {
  await rm(temporaryDirectory, { force: true, recursive: true });
}

async function inspectArchive(packageSpec, archivePath) {
  const extractionDirectory = join(
    temporaryDirectory,
    "extracted",
    packageSpec.name.replaceAll("/", "-")
  );
  await mkdir(extractionDirectory, { recursive: true });
  await run("tar", ["-xzf", archivePath, "-C", extractionDirectory]);

  const packageRoot = join(extractionDirectory, "package");
  const packageJson = JSON.parse(
    await readFile(join(packageRoot, "package.json"), "utf8")
  );
  const files = await listFiles(packageRoot);

  assert.equal(packageJson.name, packageSpec.name);
  assert.ok(
    files.includes("README.md"),
    `${packageSpec.name} packages README.md`
  );
  assert.deepEqual(
    files.filter((file) => /(?:^|\/)LICENSE$/u.test(file)),
    ["LICENSE"],
    `${packageSpec.name} packages exactly one license, at the package root`
  );
  assert.equal(
    await readFile(join(packageRoot, "LICENSE"), "utf8"),
    await readFile(join(repositoryRoot, "LICENSE"), "utf8"),
    `${packageSpec.name} packages the canonical MPL-2.0 text`
  );
  assert.equal(packageJson.license, "MPL-2.0");
  assert.equal(packageJson.engines?.node, ">=26");
  assert.equal(packageJson.publishConfig?.access, "public");
  assert.equal(packageJson.publishConfig?.provenance, true);
  assert.equal(packageJson.authors, undefined, "`authors` is not an npm field");
  assert.ok(
    Array.isArray(packageJson.contributors) &&
      packageJson.contributors.length > 0,
    `${packageSpec.name} lists contributors`
  );
  assert.ok(
    packageJson.sideEffects === false ||
      (Array.isArray(packageJson.sideEffects) &&
        packageJson.sideEffects.every((entry) =>
          files.includes(posix.normalize(entry))
        )),
    `${packageSpec.name} declares sideEffects as false or packed entry files`
  );
  inspectDependencies(packageSpec, packageJson);
  assert.deepEqual(
    files.filter((file) => /(?:^|[./])(?:integration\.)?test\./.test(file)),
    [],
    `${packageSpec.name} does not package test builds`
  );
  assert.deepEqual(
    files.filter(
      (file) =>
        /(?:^|\/)(?:testdata|fixtures?|goldens?)\/|\.tsbuildinfo$/u.test(
          file
        ) ||
        (file.endsWith(".json") && !packagedJsonFiles.has(file))
    ),
    [],
    `${packageSpec.name} does not package test data`
  );
  const denied = deniedSubstrings(repositoryRoot);
  for (const file of files) {
    assertPortableArchivePath(`${packageSpec.name}:${file}`, file);
    assertNoDeniedContent(`${packageSpec.name}:${file} (path)`, file, denied);
    if (/\.(?:[cm]?js|json|map|md|sql|ts)$/u.test(file)) {
      assertNoDeniedContent(
        `${packageSpec.name}:${file}`,
        await readFile(join(packageRoot, file), "utf8"),
        denied
      );
    }
  }
  await inspectSourceMaps(packageSpec, packageRoot, files);

  if (packageSpec.name === "@riverqueue/cli") {
    const binPath = join(packageRoot, packageJson.bin.riverqueue);
    const binStats = await stat(binPath);
    assert.notEqual(
      binStats.mode & 0o111,
      0,
      "the packaged CLI entry point is executable"
    );
  }
}

// Libraries share one `riverqueue` instance with the application: job
// definitions, drivers, and errors are recognized by identity, so a second
// copy silently breaks them. Every library therefore takes `riverqueue` as an
// exact peer, which makes version skew fail at install time. The CLI is a
// self-contained executable that never exchanges River objects with an
// application, so it keeps ordinary exact dependencies and works when
// installed on its own. Type packages are optional peers so JavaScript
// consumers do not install typings and TypeScript consumers choose versions.
function inspectDependencies(packageSpec, packageJson) {
  const label = packageSpec.name;
  assert.ok(
    !JSON.stringify(packageJson).includes("workspace:"),
    `${label} has no workspace protocol in published metadata`
  );
  const dependencies = {
    ...packageJson.dependencies,
    ...packageJson.optionalDependencies,
  };
  const peers = packageJson.peerDependencies ?? {};
  for (const [dependency, version] of Object.entries({
    ...dependencies,
    ...peers,
  })) {
    if (dependency === "riverqueue" || dependency.startsWith("@riverqueue/")) {
      assert.equal(
        version,
        packageJson.version,
        `${label} pins ${dependency} to its exact release line`
      );
    }
  }
  assert.deepEqual(
    Object.keys(dependencies).filter((name) => name.startsWith("@types/")),
    [],
    `${label} does not install type packages for JavaScript consumers`
  );
  for (const name of Object.keys(peers).filter((peer) =>
    peer.startsWith("@types/")
  )) {
    assert.equal(
      packageJson.peerDependenciesMeta?.[name]?.optional,
      true,
      `${label} makes ${name} an optional peer`
    );
  }
  if (label === "riverqueue") return;
  if (label === "@riverqueue/cli") {
    assert.equal(
      packageJson.dependencies?.riverqueue,
      packageJson.version,
      `${label} bundles its own exact riverqueue dependency`
    );
    return;
  }
  assert.equal(
    peers.riverqueue,
    packageJson.version,
    `${label} takes riverqueue as an exact peer`
  );
  assert.equal(
    dependencies.riverqueue,
    undefined,
    `${label} does not install a private riverqueue copy`
  );
}

// Every emitted module and declaration file must carry a map whose sources
// resolve to TypeScript shipped in the same archive, so stack traces,
// debuggers, and editor go-to-definition land on real source. Conversely,
// every packed source file must be referenced by a map, which keeps test
// helpers and uncompiled files out of the tarball.
async function inspectSourceMaps(packageSpec, packageRoot, files) {
  const packed = new Set(files);
  const referencedSources = new Set();
  const generatedFiles = files.filter(
    (file) => file.startsWith("dist/") && /\.(?:d\.ts|js)$/u.test(file)
  );
  assert.ok(generatedFiles.length > 0, `${packageSpec.name} packages dist`);
  for (const generated of generatedFiles) {
    const label = `${packageSpec.name}:${generated}`;
    const mapFile = `${generated}.map`;
    assert.ok(packed.has(mapFile), `${label} packages ${mapFile}`);
    const generatedText = await readFile(join(packageRoot, generated), "utf8");
    assert.ok(
      generatedText
        .trimEnd()
        .endsWith(`//# sourceMappingURL=${posix.basename(mapFile)}`),
      `${label} links its map`
    );
    const sourceMap = JSON.parse(
      await readFile(join(packageRoot, mapFile), "utf8")
    );
    assert.equal(sourceMap.version, 3, `${label} map uses version 3`);
    assert.equal(
      sourceMap.file,
      posix.basename(generated),
      `${label} map names its generated file`
    );
    assert.ok(
      Array.isArray(sourceMap.sources) && sourceMap.sources.length > 0,
      `${label} map lists its sources`
    );
    for (const source of sourceMap.sources) {
      const resolved = posix.join(
        posix.dirname(mapFile),
        sourceMap.sourceRoot ?? "",
        source
      );
      assertPortableArchivePath(`${label} map source`, resolved);
      assert.ok(
        resolved.startsWith("src/") && packed.has(resolved),
        `${label} map source ${source} resolves to packed source`
      );
      referencedSources.add(resolved);
    }
  }
  assert.deepEqual(
    files.filter(
      (file) =>
        file.endsWith(".ts") &&
        !file.endsWith(".d.ts") &&
        !referencedSources.has(file)
    ),
    [],
    `${packageSpec.name} packages only TypeScript source referenced by maps`
  );
}

async function verifyConsumers(archiveByPackageName) {
  const consumerDirectory = join(temporaryDirectory, "consumer");
  await mkdir(consumerDirectory);
  const dependencies = Object.fromEntries(
    [...archiveByPackageName].map(([name, archivePath]) => [
      name,
      `file:${archivePath}`,
    ])
  );
  await writeFile(
    join(consumerDirectory, "package.json"),
    `${JSON.stringify(
      {
        dependencies,
        name: "riverqueue-packed-consumer",
        private: true,
        type: "module",
        version: "0.0.0",
      },
      null,
      2
    )}\n`
  );
  // Install without type packages first: JavaScript consumers must not need
  // them, and the published metadata must not pull them in.
  await npmInstall(consumerDirectory);
  for (const typesPackage of ["@types/node", "@types/pg"]) {
    await assert.rejects(
      stat(join(consumerDirectory, "node_modules", typesPackage)),
      { code: "ENOENT" },
      `packed packages do not install ${typesPackage}`
    );
  }

  await writeFile(
    join(consumerDirectory, "import.mjs"),
    `
import * as river from "riverqueue";
import * as cli from "@riverqueue/cli";
import * as pg from "@riverqueue/driver-pg";
import * as prisma from "@riverqueue/driver-prisma";
import * as sqlite from "@riverqueue/driver-sqlite";
import * as migrate from "@riverqueue/migrate";
import * as testing from "@riverqueue/test";
import * as workerThreads from "@riverqueue/worker-threads";

for (const packageNamespace of [river, cli, pg, prisma, sqlite, migrate, testing, workerThreads]) {
  if (Object.keys(packageNamespace).length === 0) throw new Error("empty package namespace");
}

const exact = river.exactJsonNumber("9223372036854775807");
if (JSON.stringify({ exact }) !== '{"exact":9223372036854775807}') {
  throw new Error("packed exact JSON number lost precision");
}
const parsed = river.parseJson('{"exact":9223372036854775807}');
if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed) ||
    !river.isExactJsonNumber(parsed.exact)) {
  throw new Error("packed exact JSON parser lost precision");
}
`
  );
  await run(process.execPath, [join(consumerDirectory, "import.mjs")]);

  await writeFile(
    join(consumerDirectory, "require.cjs"),
    `
const packageNames = [
  "riverqueue",
  "@riverqueue/cli",
  "@riverqueue/driver-pg",
  "@riverqueue/driver-prisma",
  "@riverqueue/driver-sqlite",
  "@riverqueue/migrate",
  "@riverqueue/test",
  "@riverqueue/worker-threads",
];
const requiredPackages = new Map(
  packageNames.map((packageName) => [packageName, require(packageName)]),
);
for (const [packageName, packageNamespace] of requiredPackages) {
  if (Object.keys(packageNamespace).length === 0) {
    throw new Error(\`empty required namespace for \${packageName}\`);
  }
}

import("riverqueue").then((imported) => {
  if (requiredPackages.get("riverqueue").Client !== imported.Client) {
    throw new Error("require(esm) and import resolved different River implementations");
  }
});
`
  );
  await run(process.execPath, [join(consumerDirectory, "require.cjs")]);

  const { stdout: cliVersion } = await run(
    join(consumerDirectory, "node_modules", ".bin", "riverqueue"),
    ["--version"]
  );
  assert.match(
    cliVersion,
    /^riverqueue version \d+\.\d+\.\d+/,
    "the packed riverqueue executable reports its version"
  );

  await writeFile(
    join(consumerDirectory, "sqlite-worker.mjs"),
    `
import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { Client, Workers, defineJob } from "riverqueue";

const job = defineJob({
  kind: "package.consumer",
  decode(value) { return value; },
});
const workers = new Workers();
workers.add(job, () => undefined);
const driver = SqliteDriver.memory();
await createMigrator(driver).migrateUp();
const client = new Client(driver, {
  queues: { default: { maxWorkers: 1 } },
  workers,
});
await using events = client.subscribe({
  kinds: ["job_completed"],
  signal: AbortSignal.timeout(10_000),
});
const inserted = await client.insert(job, { source: "packed-archive" });
await using run = await client.start();
let completed = false;
for await (const event of events) {
  if (event.kind === "job_completed" && event.job.id === inserted.job.id) {
    completed = true;
    break;
  }
}
await run.stop({ mode: "graceful", timeout: { seconds: 5 } });
driver.close();
if (!completed) throw new Error("packed SQLite worker did not complete its job");
`
  );
  await run(process.execPath, [join(consumerDirectory, "sqlite-worker.mjs")]);

  // Exercise real behavior through the installed tarballs with Node's own
  // test runner, so no transpiler or workspace alias can hide a packaging
  // failure. The PostgreSQL tests run only when DATABASE_URL is set.
  await cp(
    resolve(repositoryRoot, "scripts", "packed-tests"),
    join(consumerDirectory, "packed-tests"),
    { recursive: true }
  );
  await run(
    process.execPath,
    ["--test", "--test-reporter=spec", "packed-tests/*.test.mjs"],
    { cwd: consumerDirectory }
  );

  await writeFile(
    join(consumerDirectory, "consumer.ts"),
    `
import {
  Client,
  Workers,
  defineJob,
  exactJsonNumber,
  type ExactJsonNumber,
  type InsertResult,
  type RiverEvent,
} from "riverqueue";
import * as stableRiver from "riverqueue";
import {
  PilotClient,
  uniqueBitmaskFromStates,
  uniqueBitmaskToStates,
  type Pilot,
  type PilotClientOptions,
  type PilotDatabase,
  type RuntimeDriver,
} from "riverqueue/unstable-driver";
import type {
  ClientDriver,
  QueueConfig,
  RunHandle,
} from "riverqueue";
import { run as runCli } from "@riverqueue/cli";
import { PgDriver } from "@riverqueue/driver-pg";
import { PrismaDriver, type PrismaClientLike } from "@riverqueue/driver-prisma";
import {
  SqliteDriver,
  transaction,
} from "@riverqueue/driver-sqlite";
import { createMigrator, type Migrator } from "@riverqueue/migrate";
import * as riverTest from "@riverqueue/test";
import { WorkerThreads } from "@riverqueue/worker-threads";
import { DatabaseSync } from "node:sqlite";
import type { Pool, PoolClient } from "pg";

const emailJob = defineJob({
  kind: "email.send",
  decode(value) {
    if (typeof value.address !== "string") throw new TypeError("address");
    return { address: value.address };
  },
});
declare const driver: RuntimeDriver;
declare const prisma: PrismaClientLike;
declare const pgPool: Pool;
declare const pgTx: PoolClient;
declare const sqliteDatabase: DatabaseSync;
const client = new Client(driver);
const pgDriver = new PgDriver(pgPool);
const pgClient = new Client(pgDriver);
const sqliteDriver = new SqliteDriver(sqliteDatabase);
const sqliteClient = new Client(sqliteDriver);
const migrators: Migrator[] = [
  createMigrator(pgDriver),
  createMigrator(sqliteDriver),
  createMigrator({ pool: pgPool, schema: "river" }),
  createMigrator({ database: sqliteDatabase }),
];
const workers = new Workers();
const period: Temporal.Duration = Temporal.Duration.from({ minutes: 5 });
const exactNumber: ExactJsonNumber = exactJsonNumber("9223372036854775807");
const uniqueMask = uniqueBitmaskFromStates(["available", "scheduled"]);
const uniqueStates = uniqueBitmaskToStates(Number.parseInt(uniqueMask, 2));
const insertion: Promise<InsertResult> = client.insert(emailJob, { address: "a@example.com" }, {
  unique: { byPeriod: period },
});
const pgInsertion: Promise<InsertResult> = pgClient.insert(
  emailJob,
  { address: "pg@example.com" },
  { tx: pgTx },
);
const sqliteInsertion: Promise<InsertResult> = transaction(
  sqliteDatabase,
  async (tx) => {
    tx.prepare("SELECT 1");
    return sqliteClient.insert(
      emailJob,
      { address: "sqlite@example.com" },
      { tx },
    );
  },
);
// A companion client extends PilotClient privately and publishes only an
// interface that extends Client, with its own queue configuration.
interface CompanionQueueConfig extends QueueConfig {
  readonly limit?: number;
}
interface CompanionClient<Transaction> extends Client<Transaction> {
  readonly companion: string;
  start(): Promise<RunHandle<CompanionQueueConfig>>;
}
interface CompanionClientConstructor {
  new <Transaction>(
    driver: ClientDriver<Transaction, "runtime">,
    options?: PilotClientOptions<Transaction, CompanionQueueConfig>,
  ): CompanionClient<Transaction>;
  readonly prototype: CompanionClient<unknown>;
}
class CompanionImplementation<Transaction> extends PilotClient<
  Transaction,
  CompanionQueueConfig
> {
  readonly companion = "companion";
  constructor(
    driver: ClientDriver<Transaction, "runtime">,
    options: PilotClientOptions<Transaction, CompanionQueueConfig> = {},
  ) {
    super(
      driver,
      options,
      (
        database: PilotDatabase<Transaction>,
      ): Pilot<Transaction, number, CompanionClient<Transaction>> => ({
        // The host's client is the final companion client.
        init: (host) => void [database.backend, host.client.companion],
        queueOptions: { keys: ["limit"], parse: () => 1 },
      }),
    );
  }
}
const CompanionClient =
  CompanionImplementation as unknown as CompanionClientConstructor;
const companion = new CompanionClient(sqliteDriver, {
  queues: { limited: { limit: 1, maxWorkers: 1 } },
});
// @ts-expect-error Queue keys neither River nor the pilot owns are rejected.
void new CompanionClient(sqliteDriver, { queues: { q: { maxWorkers: 1, other: 1 } } });
const companionAsClient: Client<DatabaseSync> =
  companion;
const companionRun: Promise<void> = companion
  .start()
  .then((run) => run.addQueue("limited", { limit: 1, maxWorkers: 1 }));
const stableRun: Promise<void> = sqliteClient
  .start()
  // @ts-expect-error River's own queue configuration has no pilot keys.
  .then((run) => run.addQueue("limited", { limit: 1, maxWorkers: 1 }));
// @ts-expect-error PilotClient is abstract.
void new PilotClient(sqliteDriver, {}, () => ({}));
// @ts-expect-error The pilot client belongs to riverqueue/unstable-driver.
void stableRiver.PilotClient;
void [companionAsClient, companionRun, stableRun];
// @ts-expect-error Drivers are opaque: no runtime methods,
void pgDriver.jobClaim;
// @ts-expect-error nor insertion methods,
void pgDriver.jobInsert;
// @ts-expect-error nor their connection or schema,
void pgDriver.pool;
// @ts-expect-error nor the application's SQLite handle.
void sqliteDriver.database;
// @ts-expect-error Protocol bitmask helpers belong to riverqueue/unstable-driver.
void stableRiver.uniqueBitmaskFromStates;
// @ts-expect-error Resolved insertion state is not a stable root API.
type StableResolvedInsertOpts = stableRiver.ResolvedInsertOptions;
// @ts-expect-error SQLite codecs are not package-entry exports.
void import("@riverqueue/driver-sqlite").then((module) => module.JOB_COLUMNS);
const event: RiverEvent | undefined = undefined;
void [workers, exactNumber, uniqueStates, insertion, pgInsertion, sqliteInsertion, event, runCli,
  pgDriver, new PrismaDriver(prisma), sqliteDriver,
  migrators, riverTest, WorkerThreads];
`
  );
  await copyFile(
    resolve(repositoryRoot, "fixtures/migration-0.1/after.ts"),
    join(consumerDirectory, "migration-0.1.ts")
  );
  await writeFile(
    join(consumerDirectory, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          exactOptionalPropertyTypes: true,
          lib: ["ES2024"],
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          strict: true,
          target: "ES2024",
        },
        files: ["consumer.ts", "migration-0.1.ts"],
      },
      null,
      2
    )}\n`
  );

  // TypeScript consumers install the optional type peers themselves.
  await npmInstall(consumerDirectory, [
    "--save-dev",
    ...["@types/node", "@types/pg"].map(
      (name) => `${name}@${rootPackageJson.devDependencies[name]}`
    ),
  ]);
  for (const compilerPackage of ["typescript", "typescript-next"]) {
    await run(
      process.execPath,
      [
        resolve(repositoryRoot, "node_modules", compilerPackage, "bin", "tsc"),
        "--project",
        join(consumerDirectory, "tsconfig.json"),
      ],
      { cwd: consumerDirectory }
    );
  }

  await verifyCommonJsTypeScriptConsumer(consumerDirectory);
}

// The packages are ESM-only. Node 26 loads them from CommonJS through
// `require(esm)`, and TypeScript models that only for `module: node20` and
// `nodenext`; `node16` and `node18` report TS1479, which is what Are The Types
// Wrong's `cjs-resolves-to-esm` rule flags. Compile a CommonJS consumer in the
// supported modes, run the emitted `require` calls, and check that they share
// the module instance an ESM import sees.
async function verifyCommonJsTypeScriptConsumer(consumerDirectory) {
  await writeFile(
    join(consumerDirectory, "commonjs-consumer.cts"),
    `
import { Client, Workers, defineJob } from "riverqueue";
import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";

const job = defineJob({ kind: "commonjs.consumer", decode: (value) => value });
const workers = new Workers();
workers.add(job, () => undefined);
const period: Temporal.Duration = Temporal.Duration.from({ minutes: 1 });
const driver = SqliteDriver.memory();

async function main(): Promise<void> {
  await createMigrator(driver).migrateUp();
  const client = new Client(driver, { workers });
  const inserted = await client.insert(job, { period: period.toString() });
  driver.close();
  const imported = await import("riverqueue");
  if (imported.Client !== Client || typeof require !== "function") {
    throw new Error("require(esm) and import resolved different River modules");
  }
  if (inserted.status !== "inserted") throw new Error("CommonJS insert failed");
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
`
  );
  const lanes = [
    ["typescript", "node20"],
    ["typescript", "nodenext"],
    ["typescript-next", "nodenext"],
  ];
  for (const [compilerPackage, moduleMode] of lanes) {
    const outDir = join(
      consumerDirectory,
      `commonjs-${compilerPackage}-${moduleMode}`
    );
    const project = join(consumerDirectory, `tsconfig.${moduleMode}.json`);
    await writeFile(
      project,
      `${JSON.stringify(
        {
          compilerOptions: {
            exactOptionalPropertyTypes: true,
            lib: ["ES2024"],
            module: moduleMode,
            outDir,
            strict: true,
            target: "ES2024",
            types: ["node"],
          },
          files: ["commonjs-consumer.cts"],
        },
        null,
        2
      )}\n`
    );
    await run(
      process.execPath,
      [
        resolve(repositoryRoot, "node_modules", compilerPackage, "bin", "tsc"),
        "--project",
        project,
      ],
      { cwd: consumerDirectory }
    );
    const emitted = await readFile(
      join(outDir, "commonjs-consumer.cjs"),
      "utf8"
    );
    assert.match(
      emitted,
      /require\("riverqueue"\)/u,
      `${compilerPackage} ${moduleMode} emits require() for riverqueue`
    );
    await run(process.execPath, [join(outDir, "commonjs-consumer.cjs")]);
  }
}

// A library built for one riverqueue release must refuse to install next to a
// different one instead of silently loading a second copy.
async function verifyVersionSkewRejected(archiveByPackageName) {
  const skewDirectory = join(temporaryDirectory, "skew");
  const skewPackageRoot = join(skewDirectory, "riverqueue", "package");
  await mkdir(skewPackageRoot, { recursive: true });
  await run("tar", [
    "-xzf",
    archiveByPackageName.get("riverqueue"),
    "-C",
    dirname(skewPackageRoot),
  ]);
  const packageJsonPath = join(skewPackageRoot, "package.json");
  const packageJson = JSON.parse(await readFile(packageJsonPath, "utf8"));
  packageJson.version = `${packageJson.version}-skew`;
  await writeFile(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);
  const skewedArchive = join(skewDirectory, "riverqueue-skew.tgz");
  await run("tar", [
    "-czf",
    skewedArchive,
    "-C",
    dirname(skewPackageRoot),
    "package",
  ]);

  for (const packageName of archiveByPackageName.keys()) {
    if (packageName === "riverqueue" || packageName === "@riverqueue/cli") {
      continue;
    }
    const consumerDirectory = join(
      skewDirectory,
      packageName.replaceAll("/", "-")
    );
    await mkdir(consumerDirectory);
    await writeFile(
      join(consumerDirectory, "package.json"),
      `${JSON.stringify(
        {
          dependencies: {
            [packageName]: `file:${archiveByPackageName.get(packageName)}`,
            riverqueue: `file:${skewedArchive}`,
          },
          name: "riverqueue-skewed-consumer",
          private: true,
          version: "0.0.0",
        },
        null,
        2
      )}\n`
    );
    await assert.rejects(
      npmInstall(consumerDirectory, ["--dry-run"], { quiet: true }),
      (error) => /ERESOLVE/u.test(`${error.stdout}${error.stderr}`),
      `${packageName} refuses to install beside a different riverqueue`
    );
  }
}

async function verifyExamples(archiveByPackageName) {
  const examplesRoot = join(temporaryDirectory, "examples");
  await cp(resolve(repositoryRoot, "examples"), examplesRoot, {
    filter: (source) =>
      !source
        .split(/[\\/]/u)
        .some((part) => ["dist", "generated", "node_modules"].includes(part)),
    recursive: true,
  });

  for (const spec of exampleSpecs) {
    const packagePath = join(examplesRoot, spec.directory, "package.json");
    const packageJson = JSON.parse(await readFile(packagePath, "utf8"));
    assert.equal(packageJson.name, spec.name);
    for (const dependencies of [
      packageJson.dependencies,
      packageJson.devDependencies,
      packageJson.optionalDependencies,
    ]) {
      if (dependencies === undefined) continue;
      for (const packageName of Object.keys(dependencies)) {
        const archive = archiveByPackageName.get(packageName);
        if (archive !== undefined) {
          dependencies[packageName] = `file:${archive}`;
        }
      }
    }
    assert.ok(
      !JSON.stringify(packageJson).includes("workspace:"),
      `${spec.name} has only packed River dependencies`
    );
    await writeFile(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`);
  }

  await writeFile(
    join(examplesRoot, "package.json"),
    `${JSON.stringify(
      {
        name: "riverqueue-packed-examples",
        private: true,
        type: "module",
        version: "0.0.0",
        workspaces: exampleSpecs.map((spec) => spec.directory),
      },
      null,
      2
    )}\n`
  );
  await npmInstall(examplesRoot);

  const runDatabaseExamples =
    typeof process.env.DATABASE_URL === "string" &&
    process.env.DATABASE_URL.length > 0;
  for (const spec of exampleSpecs) {
    await run("npm", ["run", "build", "--workspace", spec.name], {
      cwd: examplesRoot,
    });
    if (!spec.requiresDatabase || runDatabaseExamples) {
      await run("npm", ["run", "start", "--workspace", spec.name], {
        cwd: examplesRoot,
      });
    }
  }
}

async function listFiles(root) {
  const result = [];

  async function walk(directory) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        await walk(path);
      } else {
        result.push(relative(root, path));
      }
    }
  }

  await walk(root);
  return result.sort();
}

function resolveBin(name) {
  return resolve(repositoryRoot, "node_modules", ".bin", name);
}

function parseTrailingJson(output) {
  const start = output.lastIndexOf("\n{");
  return JSON.parse(output.slice(start < 0 ? 0 : start + 1));
}

function npmInstall(cwd, args = [], options = {}) {
  return run(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      "--package-lock=false",
      ...args,
    ],
    { cwd, ...options }
  );
}

async function run(command, args, { quiet = false, ...options } = {}) {
  try {
    return await execFileAsync(command, args, {
      env: {
        ...process.env,
        NO_COLOR: "1",
        npm_config_cache: join(temporaryDirectory, "npm-cache"),
      },
      maxBuffer: 10 * 1024 * 1024,
      ...options,
    });
  } catch (error) {
    if (!quiet && error.stdout) process.stderr.write(error.stdout);
    if (!quiet && error.stderr) process.stderr.write(error.stderr);
    throw error;
  }
}

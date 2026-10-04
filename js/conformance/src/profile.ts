import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { ContractParams, type ContractParamsSource } from "./contract.js";

export interface PortableStorageProfile {
  readonly adapterVersion: number;
  readonly backend: "sqlite";
  readonly capabilities: readonly string[];
  readonly implementationVersion: string;
  readonly latestMigration: number;
  readonly methods: readonly string[];
  readonly migrationLine: string;
  readonly name: "portable-storage-v1";
  /** Rejects params the contract does not declare. */
  readonly params: ContractParams;
  readonly protocolRevision: number;
  readonly repository: string;
}

export interface InsertOnlyProfile {
  readonly adapterVersion: number;
  readonly backend: "postgres";
  readonly capabilities: readonly string[];
  readonly implementationVersion: string;
  readonly latestMigration: number;
  readonly methods: readonly string[];
  readonly migrationLine: string;
  readonly name: "insert-only-v1";
  /** Rejects params the contract does not declare. */
  readonly params: ContractParams;
  readonly protocolRevision: number;
  readonly repository: string;
}

export interface PostgresFullProfile {
  readonly adapterVersion: number;
  readonly backend: "postgres";
  readonly capabilities: readonly string[];
  readonly implementationVersion: string;
  readonly latestMigration: number;
  readonly methods: readonly string[];
  readonly migrationLine: string;
  readonly name: "postgres-full-v1";
  /** Rejects params the contract does not declare. */
  readonly params: ContractParams;
  readonly protocolRevision: number;
  readonly repository: string;
}

export interface SqliteRuntimeProfile {
  readonly adapterVersion: number;
  readonly backend: "sqlite";
  readonly capabilities: readonly string[];
  readonly implementationVersion: string;
  readonly latestMigration: number;
  readonly methods: readonly string[];
  readonly migrationLine: string;
  readonly name: "sqlite-runtime-v1";
  /** Rejects params the contract does not declare. */
  readonly params: ContractParams;
  readonly protocolRevision: number;
  readonly repository: string;
}

interface AdapterContractFile extends ContractParamsSource {
  readonly adapter_version: number;
  readonly protocol_revision: number;
}

interface AdapterProfileFile {
  readonly backend: string;
  readonly capabilities: readonly string[];
  readonly methods: readonly string[];
  readonly name: string;
  readonly protocol_revision: number;
}

interface ManifestFile {
  readonly implementations: Readonly<
    Record<
      string,
      {
        readonly package: string;
        readonly registry: string;
        readonly version: string;
      }
    >
  >;
  readonly migration: { readonly latest: number; readonly line: string };
  readonly protocol_revision: number;
}

interface PackageFile {
  readonly version: string;
}

interface FullManifestFile extends ManifestFile {
  readonly capabilities: Readonly<Record<string, string>>;
}

/** Load and cross-check River's canonical public SQLite adapter inventory. */
export async function loadPortableStorageProfile(
  repository = findRiverRepository()
): Promise<PortableStorageProfile> {
  const [contract, profile, manifest, packageInfo] = await Promise.all([
    readJson<AdapterContractFile>(
      resolve(repository, "conformance/adapter/contract.json")
    ),
    readJson<AdapterProfileFile>(
      resolve(repository, "conformance/adapter/profiles/sqlite.json")
    ),
    readJson<ManifestFile>(resolve(repository, "conformance/manifest.json")),
    readJson<PackageFile>(new URL("../package.json", import.meta.url)),
  ]);

  if (profile.backend !== "sqlite") {
    throw new Error(
      `portable profile backend is ${JSON.stringify(profile.backend)}`
    );
  }
  if (profile.name !== "portable-storage-v1") {
    throw new Error(`portable profile name is ${JSON.stringify(profile.name)}`);
  }
  if (
    profile.protocol_revision !== contract.protocol_revision ||
    profile.protocol_revision !== manifest.protocol_revision
  ) {
    throw new Error("canonical River protocol revisions disagree");
  }
  requireJavaScriptVersion(manifest, packageInfo);
  const contractMethods = new Set(contract.methods.map(({ name }) => name));
  for (const method of profile.methods) {
    if (!contractMethods.has(method)) {
      throw new Error(
        `portable profile method ${JSON.stringify(method)} is absent from the contract`
      );
    }
  }

  return Object.freeze({
    adapterVersion: contract.adapter_version,
    backend: "sqlite",
    capabilities: Object.freeze([...profile.capabilities]),
    implementationVersion: packageInfo.version,
    latestMigration: manifest.migration.latest,
    methods: Object.freeze([...profile.methods]),
    migrationLine: manifest.migration.line,
    name: "portable-storage-v1",
    params: new ContractParams(contract),
    protocolRevision: profile.protocol_revision,
    repository,
  });
}

/** Load and cross-check River's canonical full SQLite runtime inventory. */
export async function loadSqliteRuntimeProfile(
  repository = findRiverRepository()
): Promise<SqliteRuntimeProfile> {
  const [contract, profile, manifest, packageInfo] = await Promise.all([
    readJson<AdapterContractFile>(
      resolve(repository, "conformance/adapter/contract.json")
    ),
    readJson<AdapterProfileFile>(
      resolve(repository, "conformance/adapter/profiles/sqlite-runtime.json")
    ),
    readJson<ManifestFile>(resolve(repository, "conformance/manifest.json")),
    readJson<PackageFile>(new URL("../package.json", import.meta.url)),
  ]);

  if (profile.backend !== "sqlite") {
    throw new Error(
      `SQLite runtime profile backend is ${JSON.stringify(profile.backend)}`
    );
  }
  if (profile.name !== "sqlite-runtime-v1") {
    throw new Error(
      `SQLite runtime profile name is ${JSON.stringify(profile.name)}`
    );
  }
  if (
    profile.protocol_revision !== contract.protocol_revision ||
    profile.protocol_revision !== manifest.protocol_revision
  ) {
    throw new Error("canonical River protocol revisions disagree");
  }
  requireJavaScriptVersion(manifest, packageInfo);
  const contractMethods = new Set(contract.methods.map(({ name }) => name));
  for (const method of profile.methods) {
    if (!contractMethods.has(method)) {
      throw new Error(
        `SQLite runtime method ${JSON.stringify(method)} is absent from the contract`
      );
    }
  }

  return Object.freeze({
    adapterVersion: contract.adapter_version,
    backend: "sqlite",
    capabilities: Object.freeze([...profile.capabilities]),
    implementationVersion: packageInfo.version,
    latestMigration: manifest.migration.latest,
    methods: Object.freeze([...profile.methods]),
    migrationLine: manifest.migration.line,
    name: "sqlite-runtime-v1",
    params: new ContractParams(contract),
    protocolRevision: profile.protocol_revision,
    repository,
  });
}

/** Load and cross-check River's canonical insert-only PostgreSQL inventory. */
export async function loadInsertOnlyProfile(
  repository = findRiverRepository()
): Promise<InsertOnlyProfile> {
  const [contract, profile, manifest, packageInfo] = await Promise.all([
    readJson<AdapterContractFile>(
      resolve(repository, "conformance/adapter/contract.json")
    ),
    readJson<AdapterProfileFile>(
      resolve(repository, "conformance/adapter/profiles/insert-only.json")
    ),
    readJson<ManifestFile>(resolve(repository, "conformance/manifest.json")),
    readJson<PackageFile>(new URL("../package.json", import.meta.url)),
  ]);

  if (profile.backend !== "postgres") {
    throw new Error(
      `insert-only profile backend is ${JSON.stringify(profile.backend)}`
    );
  }
  if (profile.name !== "insert-only-v1") {
    throw new Error(
      `insert-only profile name is ${JSON.stringify(profile.name)}`
    );
  }
  if (
    profile.protocol_revision !== contract.protocol_revision ||
    profile.protocol_revision !== manifest.protocol_revision
  ) {
    throw new Error("canonical River protocol revisions disagree");
  }
  requireJavaScriptVersion(manifest, packageInfo);
  const contractMethods = new Set(contract.methods.map(({ name }) => name));
  for (const method of profile.methods) {
    if (!contractMethods.has(method)) {
      throw new Error(
        `insert-only method ${JSON.stringify(method)} is absent from the contract`
      );
    }
  }

  return Object.freeze({
    adapterVersion: contract.adapter_version,
    backend: "postgres",
    capabilities: Object.freeze([...profile.capabilities]),
    implementationVersion: packageInfo.version,
    latestMigration: manifest.migration.latest,
    methods: Object.freeze([...profile.methods]),
    migrationLine: manifest.migration.line,
    name: "insert-only-v1",
    params: new ContractParams(contract),
    protocolRevision: profile.protocol_revision,
    repository,
  });
}

/** Load River's exact versioned PostgreSQL full-engine contract. */
export async function loadPostgresFullProfile(
  repository = findRiverRepository()
): Promise<PostgresFullProfile> {
  const [contract, manifest, packageInfo] = await Promise.all([
    readJson<AdapterContractFile>(
      resolve(repository, "conformance/adapter/contract.json")
    ),
    readJson<FullManifestFile>(
      resolve(repository, "conformance/manifest.json")
    ),
    readJson<PackageFile>(new URL("../package.json", import.meta.url)),
  ]);
  if (contract.protocol_revision !== manifest.protocol_revision) {
    throw new Error("canonical River protocol revisions disagree");
  }
  requireJavaScriptVersion(manifest, packageInfo);
  // `postgres-full-v1` advertises exactly the complete capabilities. The
  // manifest records why any other capability is not yet claimable.
  const capabilities = Object.entries(manifest.capabilities)
    .filter(([, status]) => status === "complete")
    .map(([name]) => name)
    .sort();
  return Object.freeze({
    adapterVersion: contract.adapter_version,
    backend: "postgres",
    capabilities: Object.freeze(capabilities),
    implementationVersion: packageInfo.version,
    latestMigration: manifest.migration.latest,
    methods: Object.freeze(contract.methods.map(({ name }) => name)),
    migrationLine: manifest.migration.line,
    name: "postgres-full-v1",
    params: new ContractParams(contract),
    protocolRevision: contract.protocol_revision,
    repository,
  });
}

function requireJavaScriptVersion(
  manifest: ManifestFile,
  packageInfo: PackageFile
): void {
  const implementation = manifest.implementations.javascript;
  if (implementation === undefined) {
    throw new Error("canonical River manifest omits JavaScript");
  }
  if (
    implementation.registry !== "npm" ||
    implementation.package !== "riverqueue"
  ) {
    throw new Error(
      "canonical River manifest identifies an unexpected JavaScript package"
    );
  }
  if (implementation.version !== packageInfo.version) {
    throw new Error(
      `JavaScript package version ${JSON.stringify(packageInfo.version)} does not match canonical version ${JSON.stringify(implementation.version)}`
    );
  }
}

/**
 * The River repository this workspace lives in. The adapter runs from
 * `js/conformance/dist` and its tests from `js/conformance/src`, so River's
 * root is three directories up either way.
 */
function findRiverRepository(): string {
  return fileURLToPath(new URL("../../..", import.meta.url));
}

async function readJson<T>(path: string | URL): Promise<T> {
  return JSON.parse(await readFile(path, "utf8")) as T;
}

#!/usr/bin/env node

// Run River's shared conformance suite with this workspace as the candidate.
//
// This is a thin wrapper over the `make` targets of the River repository this
// workspace lives in, so local runs match CI: it points the harness at the
// JavaScript candidate descriptor (`conformance/candidate.json`), makes Rust
// the multi-engine peer, and puts the running Node.js first on `PATH` so the
// adapter runs on the same Node 26 build.

import { readFileSync } from "node:fs";
import { dirname, delimiter, resolve } from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";
import { fileURLToPath, URL } from "node:url";

const repository = resolve(fileURLToPath(new URL("..", import.meta.url)));
const descriptorPath = resolve(repository, "conformance/candidate.json");
const options = parseOptions(process.argv.slice(2));
const riverRepository = resolve(repository, "..");

requireDescriptorVersion();

const environment = {
  ...process.env,
  PATH: `${dirname(process.execPath)}${delimiter}${process.env.PATH ?? ""}`,
  RIVER_CONFORMANCE_CANDIDATE_FILE: descriptorPath,
  RIVER_CONFORMANCE_PEER_FILE: resolve(
    riverRepository,
    "conformance/adapter/candidates/rust.json"
  ),
};
// The harness rejects an inline descriptor alongside a descriptor file.
delete environment.RIVER_CONFORMANCE_CANDIDATE;
delete environment.RIVER_CONFORMANCE_PEER;
if (options.databaseURL !== null) {
  environment.RIVER_CONFORMANCE_DATABASE_URL = options.databaseURL;
}
if (options.performanceJobs !== null) {
  environment.RIVER_CONFORMANCE_PERFORMANCE_JOBS = options.performanceJobs;
}

runMake("test/conformance/sqlite");
if (options.databaseURL !== null) {
  // Mixed, maintenance, and resilience (fault proxy) scenarios, then the
  // insert-only profile served through the Prisma driver.
  runMake("test/conformance");
  runMake("test/conformance/insert-only");
}
if (options.multiEngine) runMake("test/conformance/multi-engine");
if (options.performance) {
  runMake("test/conformance/performance", {
    RIVER_CONFORMANCE_PERFORMANCE: "1",
  });
}
if (options.multiEnginePerformance) {
  runMake("test/conformance/multi-engine/performance", {
    RIVER_CONFORMANCE_MULTI_ENGINE_PERFORMANCE: "1",
  });
}
if (options.soakDuration !== null) {
  runMake("test/conformance/soak", {
    RIVER_CONFORMANCE_SOAK_DURATION: options.soakDuration,
  });
}
if (options.multiEngineSoakDuration !== null) {
  runMake("test/conformance/multi-engine/soak", {
    RIVER_CONFORMANCE_MULTI_ENGINE_SOAK_DURATION:
      options.multiEngineSoakDuration,
  });
}

function parseOptions(args) {
  let databaseURL = null;
  let multiEngine = false;
  let multiEnginePerformance = false;
  let multiEngineSoakDuration = null;
  let performance = false;
  let performanceJobs = null;
  let soakDuration = null;
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === "--") {
      continue;
    } else if (argument === "--database-url") {
      databaseURL = requiredValue(args, ++index, argument);
    } else if (argument === "--multi-engine") {
      multiEngine = true;
    } else if (argument === "--multi-engine-performance") {
      multiEnginePerformance = true;
    } else if (argument === "--multi-engine-soak-duration") {
      multiEngineSoakDuration = requiredValue(args, ++index, argument);
    } else if (argument === "--performance") {
      performance = true;
    } else if (argument === "--performance-jobs") {
      performanceJobs = requiredValue(args, ++index, argument);
      if (!/^[1-9][0-9]*$/u.test(performanceJobs)) {
        throw new Error("--performance-jobs must be a positive integer");
      }
    } else if (argument === "--soak-duration") {
      soakDuration = requiredValue(args, ++index, argument);
    } else {
      throw new Error(`unknown argument ${JSON.stringify(argument)}`);
    }
  }
  if (performanceJobs !== null && !performance && !multiEnginePerformance) {
    throw new Error(
      "--performance-jobs requires --performance or --multi-engine-performance"
    );
  }
  if (
    databaseURL === null &&
    (multiEngine ||
      multiEnginePerformance ||
      multiEngineSoakDuration !== null ||
      performance ||
      soakDuration !== null)
  ) {
    throw new Error(
      "multi-engine, performance, and soak tiers require --database-url"
    );
  }
  if (databaseURL !== null) requireTcpDatabaseURL(databaseURL);
  return {
    databaseURL,
    multiEngine,
    multiEnginePerformance,
    multiEngineSoakDuration,
    performance,
    performanceJobs,
    soakDuration,
  };
}

// The resilience scenarios reach PostgreSQL through a TCP fault proxy that
// rewrites the URL's host and port, so a Unix socket URL cannot work.
function requireTcpDatabaseURL(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error("--database-url must be a postgres:// URL");
  }
  if (
    (url.protocol !== "postgres:" && url.protocol !== "postgresql:") ||
    url.hostname === "" ||
    url.searchParams.has("host")
  ) {
    throw new Error(
      "--database-url must name a TCP host, such as postgres://localhost:5432/river_conformance; the resilience fault proxy cannot rewrite a Unix socket URL"
    );
  }
}

// The harness requires the descriptor's version to equal the one the adapter
// reports in its handshake, which is the conformance package's version.
function requireDescriptorVersion() {
  const adapterVersion = readJson(
    resolve(repository, "conformance/package.json")
  ).version;
  const descriptorVersion = readJson(descriptorPath).version;
  if (descriptorVersion !== adapterVersion) {
    throw new Error(
      `conformance/candidate.json version ${JSON.stringify(descriptorVersion)} does not match the adapter version ${JSON.stringify(adapterVersion)}`
    );
  }
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function requiredValue(args, index, option) {
  const value = args[index];
  if (value === undefined || value.length === 0) {
    throw new Error(`${option} requires a value`);
  }
  return value;
}

function runMake(target, extraEnvironment = {}) {
  const result = spawnSync("make", ["-C", riverRepository, target], {
    env: { ...environment, ...extraEnvironment },
    stdio: "inherit",
  });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

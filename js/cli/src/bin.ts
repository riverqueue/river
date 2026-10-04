#!/usr/bin/env node

import process from "node:process";

import { run } from "./run.js";

process.exitCode = await run(process.argv.slice(2));

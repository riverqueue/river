import { once } from "node:events";
import type { Readable, Writable } from "node:stream";

import { ADAPTER_ERROR_CODE, adapterErrorCode } from "./errors.js";

interface JsonRpcRequest {
  readonly id: number | string;
  readonly jsonrpc: "2.0";
  readonly method: string;
  readonly params: Record<string, unknown>;
}

export type JsonRpcDispatch = (
  method: string,
  params: Record<string, unknown>
) => Promise<unknown>;

interface JsonRpcErrorResponse {
  readonly error: { readonly code: number; readonly message: string };
  readonly id: number | string | null;
  readonly jsonrpc: "2.0";
}

interface JsonRpcSuccessResponse {
  readonly id: number | string;
  readonly jsonrpc: "2.0";
  readonly result: unknown;
}

type JsonParseWithSource = (
  text: string,
  reviver: (
    key: string,
    value: unknown,
    context: { readonly source: string }
  ) => unknown
) => unknown;

const parseWithSource = JSON.parse as JsonParseWithSource;
const rawJson = (JSON as unknown as { rawJSON: (value: string) => unknown })
  .rawJSON;

const MAX_ERROR_MESSAGE_LENGTH = 32 * 1024;

/** Match the canonical Go adapter's maximum newline-delimited request size. */
export const MAX_JSON_RPC_REQUEST_BYTES = 4 * 1024 * 1024;

/** Serve sequential newline-delimited JSON-RPC without writing diagnostics. */
export async function serveJsonRpc(
  input: Readable,
  output: Writable,
  dispatch: JsonRpcDispatch
): Promise<void> {
  for await (const line of readBoundedLines(input)) {
    if (line.trim().length === 0) continue;
    await writeWithBackpressure(
      output,
      `${JSON.stringify(
        await handleJsonRpcLine(line, dispatch),
        (_key, value: unknown) =>
          typeof value === "bigint" ? rawJson(value.toString(10)) : value
      )}\n`
    );
  }
}

export async function handleJsonRpcLine(
  line: string,
  dispatch: JsonRpcDispatch
): Promise<JsonRpcErrorResponse | JsonRpcSuccessResponse> {
  let parsed: unknown;
  try {
    parsed = parseJson(line);
  } catch (error: unknown) {
    return errorResponse(ADAPTER_ERROR_CODE.parseError, null, error);
  }

  let request: JsonRpcRequest;
  try {
    request = parseRequest(parsed);
  } catch (error: unknown) {
    return errorResponse(
      ADAPTER_ERROR_CODE.invalidRequest,
      extractResponseId(parsed),
      error
    );
  }

  try {
    const result = await dispatch(request.method, request.params);
    return { id: request.id, jsonrpc: "2.0", result: result ?? null };
  } catch (error: unknown) {
    return errorResponse(adapterErrorCode(error), request.id, error);
  }
}

function errorResponse(
  code: number,
  id: number | string | null,
  error: unknown
): JsonRpcErrorResponse {
  return {
    error: { code, message: safeErrorMessage(error) },
    id,
    jsonrpc: "2.0",
  };
}

function extractResponseId(value: unknown): number | string | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const id = (value as Record<string, unknown>).id;
  if (typeof id === "string") return id;
  if (typeof id === "number" && Number.isSafeInteger(id)) return id;
  return null;
}

function parseJson(line: string): unknown {
  return parseWithSource(
    line,
    (_key, parsed: unknown, context: { readonly source: string }) => {
      if (
        typeof parsed === "number" &&
        Number.isInteger(parsed) &&
        !Number.isSafeInteger(parsed) &&
        /^-?\d+$/.test(context.source)
      ) {
        return BigInt(context.source);
      }
      return parsed;
    }
  );
}

function parseRequest(value: unknown): JsonRpcRequest {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("JSON-RPC request must be an object");
  }
  const record = value as Record<string, unknown>;
  if (record.jsonrpc !== "2.0") {
    throw new TypeError('JSON-RPC request must use version "2.0"');
  }
  if (typeof record.id !== "number" && typeof record.id !== "string") {
    throw new TypeError("JSON-RPC request requires a string or integer id");
  }
  if (typeof record.id === "number" && !Number.isSafeInteger(record.id)) {
    throw new TypeError("JSON-RPC numeric id must be a safe integer");
  }
  if (typeof record.method !== "string" || record.method.length === 0) {
    throw new TypeError("JSON-RPC request requires a method");
  }
  const params = record.params ?? {};
  if (typeof params !== "object" || Array.isArray(params)) {
    throw new TypeError("JSON-RPC params must be an object");
  }
  return {
    id: record.id,
    jsonrpc: "2.0",
    method: record.method,
    params: params as Record<string, unknown>,
  };
}

function safeErrorMessage(error: unknown): string {
  const message =
    error instanceof Error
      ? error.message
      : `non-Error failure: ${String(error)}`;
  return message.slice(0, MAX_ERROR_MESSAGE_LENGTH);
}

async function* readBoundedLines(input: Readable): AsyncGenerator<string> {
  let lineChunks: Buffer[] = [];
  let lineLength = 0;

  for await (const rawChunk of input) {
    const chunk = Buffer.isBuffer(rawChunk)
      ? rawChunk
      : Buffer.from(String(rawChunk));
    let offset = 0;

    while (offset < chunk.length) {
      const newlineIndex = chunk.indexOf(0x0a, offset);
      const end = newlineIndex === -1 ? chunk.length : newlineIndex;
      const segment = chunk.subarray(offset, end);

      lineLength += segment.length;
      if (lineLength > MAX_JSON_RPC_REQUEST_BYTES) {
        throw new RangeError(
          `JSON-RPC request exceeds ${MAX_JSON_RPC_REQUEST_BYTES} bytes`
        );
      }
      if (segment.length > 0) lineChunks.push(segment);

      if (newlineIndex === -1) break;

      let line = Buffer.concat(lineChunks, lineLength);
      if (line.at(-1) === 0x0d) line = line.subarray(0, line.length - 1);
      yield line.toString("utf8");
      lineChunks = [];
      lineLength = 0;
      offset = newlineIndex + 1;
    }
  }

  if (lineLength > 0) {
    yield Buffer.concat(lineChunks, lineLength).toString("utf8");
  }
}

async function writeWithBackpressure(
  output: Writable,
  value: string
): Promise<void> {
  if (output.write(value)) return;
  await Promise.race([
    once(output, "drain"),
    once(output, "close").then(() => {
      throw new Error("JSON-RPC output closed before draining");
    }),
  ]);
}

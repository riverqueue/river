import { PassThrough, Readable, Writable } from "node:stream";

import { describe, expect, it } from "vitest";

import { rejected } from "./errors.js";
import {
  handleJsonRpcLine,
  MAX_JSON_RPC_REQUEST_BYTES,
  serveJsonRpc,
} from "./rpc.js";

describe("handleJsonRpcLine", () => {
  it("dispatches a valid request", async () => {
    await expect(
      handleJsonRpcLine(
        '{"jsonrpc":"2.0","id":7,"method":"echo","params":{"ok":true}}',
        async (method, params) => ({ method, params })
      )
    ).resolves.toEqual({
      id: 7,
      jsonrpc: "2.0",
      result: { method: "echo", params: { ok: true } },
    });
  });

  it("returns protocol errors without stack traces", async () => {
    const result = await handleJsonRpcLine(
      '{"jsonrpc":"2.0","id":"request","method":"fail"}',
      async () => {
        throw new Error("expected failure");
      }
    );
    expect(result).toEqual({
      error: { code: -32_002, message: "expected failure" },
      id: "request",
      jsonrpc: "2.0",
    });
    expect(JSON.stringify(result)).not.toContain("stack");
  });

  it("reports a dispatch error's own JSON-RPC code", async () => {
    await expect(
      handleJsonRpcLine(
        '{"jsonrpc":"2.0","id":3,"method":"cron_next"}',
        async () => {
          throw rejected("invalid expression");
        }
      )
    ).resolves.toEqual({
      error: { code: -32_002, message: "invalid expression" },
      id: 3,
      jsonrpc: "2.0",
    });
  });

  it("reports invalid requests separately from parse failures", async () => {
    const result = await handleJsonRpcLine("[]", async () => null);
    expect(result).toMatchObject({
      error: { code: -32_600 },
      id: null,
      jsonrpc: "2.0",
    });

    await expect(
      handleJsonRpcLine(
        '{"jsonrpc":"1.0","id":"request","method":"echo"}',
        async () => null
      )
    ).resolves.toMatchObject({
      error: { code: -32_600 },
      id: "request",
      jsonrpc: "2.0",
    });

    await expect(
      handleJsonRpcLine('{"jsonrpc":', async () => null)
    ).resolves.toMatchObject({
      error: { code: -32_700 },
      id: null,
      jsonrpc: "2.0",
    });
  });

  it("preserves integer tokens beyond JavaScript's safe range", async () => {
    const output = new PassThrough();
    let encoded = "";
    output.setEncoding("utf8");
    output.on("data", (chunk: string) => {
      encoded += chunk;
    });

    await serveJsonRpc(
      Readable.from([
        '{"jsonrpc":"2.0","id":1,"method":"exact","params":{"seed":18446744073709551615}}\n',
      ]),
      output,
      async (_method, params) => ({ seed: params.seed })
    );

    expect(encoded).toBe(
      '{"id":1,"jsonrpc":"2.0","result":{"seed":18446744073709551615}}\n'
    );
  });

  it("awaits output backpressure", async () => {
    let writes = 0;
    const output = new Writable({
      highWaterMark: 1,
      write(_chunk, _encoding, callback) {
        writes += 1;
        setImmediate(callback);
      },
    });

    await serveJsonRpc(
      Readable.from([
        '{"jsonrpc":"2.0","id":1,"method":"echo"}\n',
        '{"jsonrpc":"2.0","id":2,"method":"echo"}\n',
      ]),
      output,
      async () => null
    );

    expect(writes).toBe(2);
  });

  it("bounds request lines before dispatch", async () => {
    let dispatched = false;

    await expect(
      serveJsonRpc(
        Readable.from([
          Buffer.alloc(MAX_JSON_RPC_REQUEST_BYTES, 0x20),
          Buffer.from(" \n"),
        ]),
        new PassThrough(),
        async () => {
          dispatched = true;
          return null;
        }
      )
    ).rejects.toThrow(
      `JSON-RPC request exceeds ${MAX_JSON_RPC_REQUEST_BYTES} bytes`
    );
    expect(dispatched).toBe(false);
  });
});

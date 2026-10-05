import { Writable } from "node:stream";

import pino from "pino";
import { describe, expect, expectTypeOf, it, vi } from "vitest";

import {
  consoleLogger,
  createWorkLogger,
  internalLogger,
  isLoggerOption,
  resolveLogger,
  type Logger,
} from "./logger.js";

describe("Logger", () => {
  it("accepts pino directly and writes its structured records", () => {
    const lines: string[] = [];
    const destination = new Writable({
      write(chunk: Buffer, _encoding, callback) {
        lines.push(chunk.toString("utf8"));
        callback();
      },
    });
    const logger = pino(destination);
    expectTypeOf(logger).toExtend<Logger>();

    const work = createWorkLogger(logger, { jobId: "42", jobKind: "email" });
    work.info("plain message");
    work.warn({ attempt: 2 }, "with attributes");

    const records = lines.map(
      (line) => JSON.parse(line) as Record<string, unknown>
    );
    expect(records[0]).toMatchObject({
      jobId: "42",
      jobKind: "email",
      msg: "plain message",
    });
    expect(records[1]).toMatchObject({ attempt: 2, msg: "with attributes" });
  });

  it("defaults to console warnings and errors, and false silences", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const error = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    try {
      const logger = internalLogger(resolveLogger(undefined));
      logger.info("quiet");
      logger.warn("careful", { queue: "default" });
      logger.error("broken");
      expect(resolveLogger(undefined)).toBe(consoleLogger);
      expect(info).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith("[riverqueue] careful", {
        queue: "default",
      });
      expect(error).toHaveBeenCalledWith("[riverqueue] broken", {});

      internalLogger(resolveLogger(false)).error("silenced");
      expect(error).toHaveBeenCalledTimes(1);
    } finally {
      warn.mockRestore();
      error.mockRestore();
      info.mockRestore();
    }
  });

  it("validates logger options", () => {
    expect(isLoggerOption(false)).toBe(true);
    expect(isLoggerOption(consoleLogger)).toBe(true);
    expect(isLoggerOption({ info: () => undefined })).toBe(false);
    expect(isLoggerOption(null)).toBe(false);
  });
});

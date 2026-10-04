import { DatabaseOperationError } from "riverqueue";
import { describe, expect, it } from "vitest";

import {
  ADAPTER_ERROR_CODE,
  adapterErrorCode,
  notFound,
  unsupported,
} from "./errors.js";

describe("adapterErrorCode", () => {
  it("keeps codes the adapter classified itself", () => {
    expect(adapterErrorCode(notFound("job 1 not found"))).toBe(
      ADAPTER_ERROR_CODE.notFound
    );
    expect(adapterErrorCode(unsupported("no custom schemas"))).toBe(
      ADAPTER_ERROR_CODE.unsupported
    );
  });

  it("reports database failures, including wrapped driver errors", () => {
    const postgres = Object.assign(new Error("division by zero"), {
      code: "22012",
    });
    const sqlite = Object.assign(new Error("no such table"), {
      code: "ERR_SQLITE_ERROR",
    });

    expect(adapterErrorCode(postgres)).toBe(ADAPTER_ERROR_CODE.databaseError);
    expect(adapterErrorCode(new Error("wrapped", { cause: sqlite }))).toBe(
      ADAPTER_ERROR_CODE.databaseError
    );
    expect(
      adapterErrorCode(
        new DatabaseOperationError("insert failed", {
          backend: "postgres",
          operation: "jobInsert",
        })
      )
    ).toBe(ADAPTER_ERROR_CODE.databaseError);
  });

  it("treats every other failure as a rejection", () => {
    expect(adapterErrorCode(new Error("client already running"))).toBe(
      ADAPTER_ERROR_CODE.rejected
    );
    expect(adapterErrorCode("not an error")).toBe(ADAPTER_ERROR_CODE.rejected);
  });
});

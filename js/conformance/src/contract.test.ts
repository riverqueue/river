import { describe, expect, it } from "vitest";

import { ContractParams } from "./contract.js";
import { ADAPTER_ERROR_CODE } from "./errors.js";

const contract = new ContractParams({
  $defs: {
    job: {
      additionalProperties: false,
      properties: { message: { type: "string" } },
      type: "object",
    },
    result: { $ref: "../schema/normalized-job.schema.json" },
  },
  methods: [
    {
      name: "insert_many",
      params: {
        additionalProperties: false,
        properties: {
          jobs: { items: { $ref: "#/$defs/job" }, type: "array" },
          metadata: { type: "object" },
        },
        type: "object",
      },
    },
    { name: "open", params: { properties: {}, type: "object" } },
    { name: "external", params: { $ref: "#/$defs/result" } },
  ],
});

const invalidParams = expect.objectContaining({
  code: ADAPTER_ERROR_CODE.invalidParams,
});

describe("ContractParams", () => {
  it("accepts declared params and open objects", () => {
    expect(() => {
      contract.check("insert_many", {
        jobs: [{ message: "one" }],
        metadata: { anything: { goes: true } },
      });
    }).not.toThrow();
    expect(() => {
      contract.check("open", { anything: true });
    }).not.toThrow();
  });

  it("rejects unknown top-level and nested params", () => {
    expect(() => {
      contract.check("insert_many", { unexpected: true });
    }).toThrow(invalidParams);
    expect(() => {
      contract.check("insert_many", { jobs: [{ message: "a", extra: 1 }] });
    }).toThrow(invalidParams);
  });

  it("leaves methods and references outside the contract unchecked", () => {
    expect(() => {
      contract.check("not_in_contract", { anything: true });
    }).not.toThrow();
    expect(() => {
      contract.check("external", { anything: true });
    }).not.toThrow();
  });
});

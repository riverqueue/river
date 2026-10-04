import { describe, expect, expectTypeOf, it } from "vitest";

import {
  RiverError,
  ValidationError,
  type RiverErrorCode,
  type RiverErrorOptions,
} from "./errors.js";

describe("RiverError", () => {
  it("roots a companion package's errors, with codes of its own", () => {
    const EXAMPLE_ERROR_CODE = { cycle: "example.cycle" } as const;
    type ExampleErrorCode =
      (typeof EXAMPLE_ERROR_CODE)[keyof typeof EXAMPLE_ERROR_CODE];
    class ExampleError<
      Code extends ExampleErrorCode = ExampleErrorCode,
    > extends RiverError<Code> {
      constructor(message: string, options: RiverErrorOptions<Code>) {
        super(message, options);
        this.name = "ExampleError";
      }
    }
    class ExampleInputError extends ValidationError {
      constructor(message: string) {
        super(message, { details: { field: "input" } });
        this.name = "ExampleInputError";
      }
    }

    const cycle = new ExampleError("cycle", {
      code: EXAMPLE_ERROR_CODE.cycle,
      retryable: true,
    });
    const input = new ExampleInputError("bad input");

    expect(cycle).toBeInstanceOf(RiverError);
    expect(cycle).toMatchObject({
      code: "example.cycle",
      name: "ExampleError",
      retryable: true,
    });
    expectTypeOf(cycle.code).toEqualTypeOf<"example.cycle">();
    expect(input).toBeInstanceOf(ValidationError);
    expect(input).toMatchObject({
      code: "validation",
      name: "ExampleInputError",
    });
    expectTypeOf<RiverError["code"]>().toEqualTypeOf<RiverErrorCode>();
  });
});

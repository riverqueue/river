import { defineJob } from "riverqueue";
import { z } from "zod";

/**
 * Find the nth prime. The schema validates args wherever the job is worked,
 * including jobs inserted by producers in other languages.
 */
export const findPrime = defineJob({
  kind: "example.find_prime",
  schema: z.object({ ordinal: z.number().int().positive() }),
});

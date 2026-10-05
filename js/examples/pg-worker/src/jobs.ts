import { defineJob } from "riverqueue";
import { z } from "zod";

/**
 * Job definitions are shared by producers and workers. The schema validates
 * arguments when a job is inserted and again before it is worked.
 */
export const chargeInvoice = defineJob({
  defaults: { maxAttempts: 5, queue: "billing" },
  kind: "example.charge_invoice",
  schema: z.object({
    amountCents: z.number().int().positive(),
    invoiceId: z.string().min(1),
  }),
});

export const sendReceipt = defineJob({
  kind: "example.send_receipt",
  schema: z.object({ invoiceId: z.string().min(1) }),
});

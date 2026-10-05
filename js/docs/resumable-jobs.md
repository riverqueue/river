# Resumable jobs

A job that does several expensive steps can skip the steps an earlier attempt
already finished. Wrap each step in `ctx.resumable.step`; when an attempt
fails, River records the last completed step in the job's metadata, and the
next attempt skips every step up to and including it:

<!-- ts-setup
import { Workers, defineJob } from "riverqueue";
const onboardAccount = defineJob<{ accountId: string }>()({
  kind: "onboard_account",
});
declare const billing: { createCustomer(id: string): Promise<void> };
declare const storage: { provision(id: string): Promise<void> };
declare const mailer: { sendWelcome(id: string): Promise<void> };
-->

```ts
const workers = new Workers().add(
  onboardAccount,
  async ({ job, resumable }) => {
    const accountId = String(job.args.accountId);
    await resumable.step("billing", () => billing.createCustomer(accountId));
    await resumable.step("storage", () => storage.provision(accountId));
    await resumable.step("welcome", () => mailer.sendWelcome(accountId));
  }
);
```

If `storage` fails, the retry skips `billing` and runs `storage` again. Steps
must be awaited one at a time (nested steps are fine, concurrent ones are
not), and names must stay stable across deploys because they are persisted.
Catching a step's error doesn't make the attempt succeed: River still fails
the attempt and keeps the checkpoint.

## Cursors

A step that works through a list can save a cursor so a retry resumes
mid-step. `stepWithCursor` passes the cursor saved by the last failed attempt,
or `null` the first time:

<!-- ts-setup
import { Workers, defineJob } from "riverqueue";
const exportRows = defineJob({ kind: "export_rows" });
declare const rows: {
  page(after: number | null): Promise<{ ids: number[]; last: number | null }>;
};
declare function exportPage(ids: number[]): Promise<void>;
-->

```ts
const workers = new Workers().add(exportRows, async ({ resumable }) => {
  await resumable.stepWithCursor("export", async (cursor) => {
    let after = typeof cursor === "number" ? cursor : null;
    for (;;) {
      const page = await rows.page(after);
      if (page.last === null) return;
      await exportPage(page.ids);
      after = page.last;
      resumable.setCursor(after);
    }
  });
});
```

The cursor is any JSON value and is saved when the attempt ends. To save
progress immediately, and atomically with the step's own writes, call
`resumable.checkpoint({ cursor, tx })` inside the step with your database
transaction.

## Across languages

Progress lives in the job's metadata under `river:resumable_step` and
`river:resumable_cursor`, the same keys River for Go and Rust use, so a job
can resume in a different language as long as step names and cursor shapes
match. `@riverqueue/test`'s `workOnce` returns the updated metadata so a test
can run a second attempt from the first one's checkpoint; see
[testing](./testing.md).

import type { QueueUpdateParams } from "../driver.js";
import { stringifyJson } from "../json.js";

/**
 * What a queue update with new metadata writes: `text`, the metadata to
 * store, and `notification`, the `metadata_changed` notification's payload
 * for queue `queue`. Undefined when the update keeps the queue's metadata.
 */
export function queueMetadataUpdate(
  queue: string,
  params: QueueUpdateParams
): { readonly notification: string; readonly text: string } | undefined {
  if (params.metadata === undefined) return undefined;
  return {
    notification: stringifyJson({
      action: "metadata_changed",
      metadata: params.metadata,
      queue,
    }),
    text: stringifyJson(params.metadata),
  };
}

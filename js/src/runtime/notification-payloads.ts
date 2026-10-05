/**
 * Parsing of runtime notification payloads. Notifications are hints, so
 * malformed payloads are ignored rather than reported.
 */
export function notificationQueue(payload: string): string | null {
  try {
    const value: unknown = JSON.parse(payload);
    if (
      value !== null &&
      typeof value === "object" &&
      "queue" in value &&
      typeof value.queue === "string"
    ) {
      return value.queue;
    }
  } catch {
    // Notifications are hints; malformed payloads are ignored.
  }
  return null;
}

/** The job ID a control notification asks to cancel, if any. */
export function notificationCancellation(payload: string): bigint | null {
  if (!/"action"\s*:\s*"cancel"/.test(payload)) return null;
  const match = /"job_id"\s*:\s*(?:"([0-9]+)"|([0-9]+))/.exec(payload);
  const value = match?.[1] ?? match?.[2];
  return value === undefined ? null : BigInt(value);
}

/**
 * The leader ID of a leadership notification announcing that a leader
 * resigned, like River for Go's `{"action": "resigned", "leader_id": ...}`.
 */
export function notificationLeaderResigned(payload: string): string | null {
  try {
    const value: unknown = JSON.parse(payload);
    if (
      value !== null &&
      typeof value === "object" &&
      "action" in value &&
      value.action === "resigned" &&
      "leader_id" in value &&
      typeof value.leader_id === "string"
    ) {
      return value.leader_id;
    }
  } catch {
    // Notifications are hints; malformed payloads are ignored.
  }
  return null;
}

/** Whether a leadership notification asks the current leader to resign. */
export function notificationRequestsLeadershipResignation(
  payload: string
): boolean {
  try {
    const value: unknown = JSON.parse(payload);
    return (
      value !== null &&
      typeof value === "object" &&
      "action" in value &&
      value.action === "request_resign"
    );
  } catch {
    // Notifications are hints; malformed payloads are ignored.
    return false;
  }
}

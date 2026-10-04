/**
 * The database River hands an extension's pilot, through which
 * `delete_finalized` runs one cleaner batch as an extension's own cleaner
 * pass would.
 */
import type { ClientDriver } from "riverqueue";
import { PilotClient, type PilotDatabase } from "riverqueue/unstable-driver";

/** A client that is never started, whose pilot only keeps its database. */
export class PilotDatabaseClient<Transaction> extends PilotClient<Transaction> {
  /** The database River handed the client's pilot. */
  readonly database: PilotDatabase<Transaction>;

  constructor(driver: ClientDriver<Transaction, "runtime">) {
    let database: PilotDatabase<Transaction> | undefined;
    super(driver, {}, (pilotDatabase) => {
      database = pilotDatabase;
      return {};
    });
    if (database === undefined) {
      throw new Error("River created no pilot for the client");
    }
    this.database = database;
  }
}

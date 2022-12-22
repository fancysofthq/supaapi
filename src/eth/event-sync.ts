import { Database } from "better-sqlite3";
import { ethers } from "ethers";
import pRetry from "p-retry";
import { Hash } from "@fancysofthq/supabase";
import { sleep } from "@fancysofthq/supabase/utils/aux";

export interface Job {
  readonly eventTable: string;
  run(cancel: () => boolean): Promise<void>;
}

const BATCH_SIZE = 5760; // Approximately 24 hours

export async function sync<T extends ethers.Event>(
  db: Database,
  syncTable: string,
  historicalBlockColumn: string,
  eventTableColumn: string,
  eventTable: string,
  contract: ethers.Contract,
  contractDeployTx: Hash,
  eventFilter: ethers.EventFilter,
  insert: (db: Database, events: T[]) => void,
  cancel: () => boolean
) {
  const deployBlock = (
    await contract.provider.getTransaction(contractDeployTx.toString())
  ).blockNumber;
  if (!deployBlock) throw new Error("Contract deploy block not found");

  const currentBlock = await contract.provider.getBlockNumber();

  console.info("Syncing", contract.address, "for", eventTable);

  await Promise.all([
    syncHistorical(
      db,
      syncTable,
      historicalBlockColumn,
      eventTableColumn,
      eventTable,
      contract,
      eventFilter,
      deployBlock,
      currentBlock,
      insert
    ),
    syncRealtime(db, contract, eventFilter, insert, cancel),
  ]);
}

async function syncHistorical(
  db: Database,
  syncTable: string,
  historicalBlockColumn: string,
  eventTableColumn: string,
  eventTable: string,
  contract: ethers.Contract,
  filter: ethers.EventFilter,
  deployBlock: number,
  currentBlock: number,
  insert: (db: Database, events: ethers.Event[]) => void
) {
  const getHistoricalBlockStmt = db
    .prepare(
      `SELECT ${historicalBlockColumn}
      FROM ${syncTable}
      WHERE ${eventTableColumn} = '${eventTable}'`
    )
    .pluck();

  const setHistoricalBlockStmt = db.prepare(
    `UPDATE ${syncTable}
    SET ${historicalBlockColumn} = ?
    WHERE ${eventTableColumn} = '${eventTable}'`
  );

  let historicalBlock = getHistoricalBlockStmt.get() as number | undefined;

  let from = historicalBlock || deployBlock;
  let to = Math.min(from + BATCH_SIZE, currentBlock);

  while (from < to) {
    const events = await pRetry(() => contract.queryFilter(filter, from, to));

    console.log(
      "Queried",
      contract.address,
      "for",
      events.length,
      eventTable,
      "events from blocks",
      from,
      "to",
      to
    );

    db.transaction(() => {
      insert(db, events);
      setHistoricalBlockStmt.run(to);
    })();

    from = to;
    to = Math.min(from + BATCH_SIZE, currentBlock);
  }
}

async function syncRealtime(
  db: Database,
  contract: ethers.Contract,
  filter: ethers.EventFilter,
  insert: (db: Database, events: ethers.Event[]) => void,
  cancel: () => boolean
) {
  contract.on(filter, (...data) => {
    const e: ethers.Event = data[data.length - 1];
    console.log("Inserting realtime", e.event, "event for", contract.address);

    db.transaction(() => {
      insert(db, [e]);
    })();
  });

  while (!cancel()) {
    await sleep(1000);
  }

  contract.removeAllListeners(filter);
}

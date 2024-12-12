import { PGlite } from "@electric-sql/pglite";

type Operation = `insert` | `update` | `delete`;

type ChangeMessage<T> = {
  value: T;
  headers: { operation: Operation; txid?: number; relation: string[] };
} & { headers: { control: "up-to-date" } };

type InputMessage = {
  block_height: number;
  input: string;
  player_id: number;
  event_id: number;
};

type TxId = number;

const byTransaction: Map<
  TxId,
  { op: Operation; value: any; relation: string[] }[]
> = new Map();

type ElectricSchemaColumnType = "varchar" | "int4" | "text";
type ElectricSchemaColumn = {
  pk_index?: number;
  type: ElectricSchemaColumnType;
};

const schemas: Map<string, { [column: string]: ElectricSchemaColumn }> =
  new Map();

let url = `http://localhost:3000/v1/shape`;

type Updater = (mainTable: boolean) => Promise<void>;

type TableSpec = {
  table: string;
  filters?: {
    column: string;
    value: string | number;
  }[];
};

function updater(arg: TableSpec): Updater {
  const { table, filters } = arg;

  let live = false;
  let offset = "-1";
  let handle: string | null = null;

  return async (mainTable: boolean) => {
    let shapeUrl = `${url}?table=${table}&offset=${offset}&live=${live}`;

    if (filters) {
      const conditions = filters
        .map(({ column, value }) => `${column}=${JSON.stringify(value)}`)
        .join(" AND ");
      shapeUrl += `&where=${conditions}`;
    }

    if (handle) {
      shapeUrl += `&handle=${handle} `;
    }

    const data = await fetch(shapeUrl);

    if (data.status >= 400) {
      throw new Error(`Error: ${data.statusText}`);
    }

    if (!schemas.has(table)) {
      const schema = data.headers.get("electric-schema");
      if (schema) {
        schemas.set(table, JSON.parse(schema));
      }
    }

    offset = data.headers.get("electric-offset")!;

    if (handle === null) {
      handle = data.headers.get("electric-handle");
    }

    live = (live || data.headers.has("electric-up-to-date")) && mainTable;

    if (data.status == 204) {
      return;
    }

    try {
      const messages: ChangeMessage<InputMessage>[] = await data.json();

      for (const msg of messages) {
        const txId = msg.headers.txid || 0;
        if (msg.value) {
          if (!byTransaction.has(txId)) {
            byTransaction.set(
              txId,
              [] as {
                op: Operation;
                value: any;
                relation: string[];
              }[]
            );
          }

          byTransaction.get(txId)?.push({
            op: msg.headers.operation,
            value: msg,
            relation: msg.headers.relation,
          });
        }
      }
    } catch (error) {
      console.log("updater error", error);
    }
  };
}

async function materialize(
  txs: Array<number>,
  pg: PGlite,
  onError: () => Promise<void>
) {
  try {
    for (const key of txs) {
      const tx = byTransaction.get(key)!;

      await pg.transaction(async (pg) => {
        // this deactivates the triggers, which in particular deactivates the foreign
        // key relations, which helps because then the order doesn't matter.
        await pg.exec("SET session_replication_role TO replica;");

        for (const change of tx) {
          const schema = schemas.get(
            change.relation[change.relation.length - 1]
          );

          if (!schema) {
            throw new Error("Table schema not found");
          }

          const dollar = "$";
          switch (change.op) {
            case "insert":
              await (async () => {
                const table = change.relation.map((s) => `"${s}"`).join(".");

                const rows = Object.keys(change.value.value).join(",");
                const params = Object.keys(change.value.value)
                  .map((_, index) => `${dollar}${index + 1}`)
                  .join(",");

                const checkConstraints = Object.keys(change.value.value)
                  .map((key, index) => `${key} = ${dollar}${index + 1}`)
                  .join(" AND ");

                const checkQuery = `SELECT COUNT(*) FROM ${table} WHERE ${checkConstraints}; `;

                const existingEntry = await pg.query(
                  checkQuery,
                  Object.values(change.value.value)
                );

                if (existingEntry.rows[0].count === 0) {
                  const insertQuery = `INSERT INTO ${table} (${rows}) VALUES(${params}); `;
                  const insertValues = Object.values(change.value.value);

                  await pg.query(insertQuery, insertValues);
                }

                // TODO: this could be generalized an extracted from this
                // function, but this is the easiest way.
                const eventId = change.value.value["event_id"];

                if (eventId) {
                  const query = `DELETE FROM local_event WHERE event_id = ${eventId}`;
                  await pg.query(query);
                }
              })();
              break;
            case "update":
              await (async () => {
                const table = change.relation.map((s) => `"${s}"`).join(".");

                const pks = Object.keys(change.value.value).filter(
                  (column) => schema[column].pk_index !== undefined
                );
                const non_pks = Object.keys(change.value.value).filter(
                  (column) => schema[column].pk_index === undefined
                );

                const updated = non_pks
                  .map((column) => {
                    let val = (() => {
                      if (schema[column].type === "varchar") {
                        return `'${change.value.value[column]}'`;
                      } else {
                        return `'${change.value.value[column]}'`;
                      }
                    })();

                    return `${column}=${val} `;
                  })
                  .join(",");

                const where = pks
                  .map(
                    (column, index) => `${column}=${change.value.value[column]}`
                  )
                  .join(" AND ");

                const updateQuery = `UPDATE ${table} SET ${updated} WHERE ${where}; `;

                await pg.query(updateQuery);
              })();

              break;
            case "delete":
              const table = change.relation.map((s) => `"${s}"`).join(".");

              const constraints = Object.entries(change.value.value)
                .map(([key, value], index) => `${key} = ${dollar}${index + 1}`)
                .join(" AND ");

              const deleteQuery = `DELETE FROM ${table} WHERE ${constraints}; `;

              await pg.query(deleteQuery, Object.values(change.value.value));

              break;
          }
        }

        await pg.exec("SET session_replication_role TO origin;");
      });

      byTransaction.delete(key);
    }
  } catch (error) {
    console.log("materialization error", error);
    await onError();
  }
}

export async function syncTables(
  specs: TableSpec[],
  pg: PGlite,
  onChange: () => Promise<void>,
  onError: () => Promise<void>
) {
  const updaters = specs.map(updater);

  while (true) {
    await updaters[0](true);

    let transactionsToMaterialize = Array.from(byTransaction.keys());

    await Promise.all(updaters.slice(1).map((f) => f(false)));

    await materialize(transactionsToMaterialize, pg, onError);

    if (transactionsToMaterialize.length > 0) {
      await onChange();
    }
  }
}

import { processInput, setupSchema } from "./domain";
import { syncTables } from "./syncClient";
import "./style.css";
import { PGlite } from "@electric-sql/pglite";
import { live } from '@electric-sql/pglite/live'

const tables = [
  { table: "input", filters: [{ column: 'player_id', value: 0 }] },
  { table: "mutable", filters: [{ column: 'player_id', value: 0 }] }
];

const pg = new PGlite({ extensions: { live } });

const schemaQuery = setupSchema();

await pg.exec(schemaQuery);

tables.forEach(async (value, index) => {
  const query = `SELECT * FROM ${value.table} ORDER BY event_id;`;
  await generateTable(await pg.query(query), `table${index + 1}-container`);

  // @ts-ignore
  pg.live.query(query, [], async (res) => {
    // @ts-ignore
    await generateTable(res, `table${index + 1}-container`);
  });
});


async function rollbackHandler() {
  await pg.exec(`
CREATE TABLE IF NOT EXISTS local_event (
	event_id INTEGER PRIMARY KEY
);
  `);

  for (const table of tables) {
    await pg.exec(`
CREATE OR REPLACE FUNCTION add_uncommitted()
RETURNS TRIGGER AS $$
    BEGIN
    INSERT INTO local_event(event_id)
    VALUES(NEW.event_id:: INTEGER) ON CONFLICT DO NOTHING;
    RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_insert_trigger
AFTER INSERT ON ${table.table}
FOR EACH ROW
EXECUTE FUNCTION add_uncommitted();
`);
  }

  return async () => {
    await pg.transaction(async pg => {
      const toDelete = await pg.query('select event_id from local_event;');

      for (const row of toDelete.rows) {
        // @ts-ignore
        const eventId = row.event_id;
        for (const table of tables) {
          const query = `delete from ${table.table} where event_id = ${eventId}`;
          await pg.query(query);
        }

        const query = `delete from local_event where event_id = ${eventId}`;
        await pg.query(query);
      }
    });
  };
}

const doRollback = await rollbackHandler();

syncTables(tables, pg, async () => {
  tables.forEach(async (value, index) => {
    const query = `SELECT * FROM ${value.table} ORDER BY event_id;`;
    await generateTable(await pg.query(query), `table${index + 1}-container`);
  })
}, doRollback);

// @ts-ignore
window.pg = pg;

document.querySelector<HTMLDivElement>("#app")!.innerHTML = `
<div >
  <div>
    <label for="input">Input:</label>
    <input type="text" id="input" name="input" required minlength="1" size="20"/>
  </div>
  <div>
    <button id="batch"> Batch </button>
    <button id="submit"> Submit </button>
    <button id="rollback"> Rollback </button>
  </div>
  <div id="table1-container"></div>
  <div id="table2-container"></div>
</div>
`;

const batchButton = document.getElementById("batch") as HTMLButtonElement;
const submitButton = document.getElementById("submit") as HTMLButtonElement;
const rollback = document.getElementById("rollback") as HTMLButtonElement;
const textInput = document.getElementById("input") as HTMLInputElement;

let batch = [] as string[];

rollback?.addEventListener("click", doRollback);

batchButton?.addEventListener("click", async () => {
  const playerId = 0;
  const queries = processInput(textInput.value, playerId, false);

  if (queries) {
    batch.push(textInput.value);
    await pg.query("BEGIN;");
    for (const query of queries) {
      await pg.exec(query);
    }
    await pg.query("END;");
  }

  textInput.value = '';
});

submitButton?.addEventListener("click", async () => {
  await fetch("http://localhost:3112/submit", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(batch),
  })
    .then((response) => {
      if (!response.ok) {
        throw new Error("Network response was not ok");
      }

      batch = [];
      return response.json();
    })
    .then((data) => {
      console.log("Success:", data);
    })
    .catch((error) => {
      console.error("Error:", error);
    });
});

type Data = {
  fields: {
    name: string;
  }[];
  rows: {
    [column: string]: any;
  }[];
};

async function generateTable(data: Data, tableContainerName: string) {
  const tableContainer = document.getElementById(tableContainerName);
  const table = document.createElement('table') as HTMLTableElement;

  const headerRow = document.createElement('tr');
  data.fields.forEach(key => {
    const th = document.createElement('th');
    th.textContent = key.name;
    headerRow.appendChild(th);
  });
  table.appendChild(headerRow);

  data.rows.forEach(item => {
    const row = document.createElement('tr');
    Object.values(item).forEach(value => {
      const td = document.createElement('td');
      td.textContent = value;
      row.appendChild(td);
    });
    table.appendChild(row);
  });


  tableContainer?.replaceChildren();
  tableContainer?.appendChild(table);
}



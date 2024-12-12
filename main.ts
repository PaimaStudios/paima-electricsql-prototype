import pg from "pg";
import { Application, Router } from "https://deno.land/x/oak@v17.1.3/mod.ts";
import { oakCors } from "https://deno.land/x/cors@v1.2.2/mod.ts";
import {
  processInput,
  setupSchema,
  initialSetup,
} from "./browser-test/src/domain.ts";

const { Pool } = pg;

const pool = new Pool({
  user: "postgres",
  password: "password",
  host: "localhost",
  port: 54321,
  database: "electric",
});

try {
  await pool.query("select * from input");
  await pool.query("select * from mutable");
} catch (_err) {
  await pool.query(setupSchema());
  await pool.query(initialSetup());
}

const router = new Router();

router.get("/health", (ctx) => {
  ctx.response.body = "healthy";
});

router.post("/submit", async (context) => {
  const client = await pool.connect();

  try {
    await client.query("BEGIN;");
    const body = await context.request.body.json();

    for (const input of body) {
      const queries = processInput(input, 0, true);

      for (const query of queries) {
        await client.query(query);
      }
    }

    await client.query("COMMIT");
    context.response.status = 200;
    context.response.body = { message: "Data processed" };
    // for some reason the middleware doesn't set this
    context.response.headers.append(
      "Access-Control-Allow-Origin",
      "http://localhost:5173"
    );
  } catch (error) {
    context.response.status = 400;
    context.response.body = { message: `Bad request: ${error}` };
  }
});

const app = new Application();
app.use(router.routes());
app.use(router.allowedMethods());
app.use(
  oakCors({
    origin: /^.+localhost:5173$/,
  })
);

app.listen({ port: 3112 });

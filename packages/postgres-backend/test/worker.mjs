import { Pool } from "pg";
import { PostgresBackend } from "../dist/index.js";

const [queue] = process.argv.slice(2);
const pool = new Pool({ connectionString: process.env.JOB_SYSTEM_POSTGRES_URL });
const backend = new PostgresBackend({ pool, queue, leaseDurationMs: 600, pollIntervalMs: 20 });
await backend.work(async (message, signal, attempt) => {
  await pool.query("INSERT INTO job_system.test_effects (queue, id) VALUES ($1, $2) ON CONFLICT DO NOTHING", [queue, message.id]);
  process.send({ event: "committed", id: message.id, attempt });
  await new Promise(() => {});
});

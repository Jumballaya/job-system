import { Container, createJobSystem, defineJob } from "core";
import { RedisBackend } from "redis-backend";
import { Ledger } from "./ledger.mjs";

const [queue, filename, mode] = process.argv.slice(2);
const ledger = new Ledger(filename);
const credit = defineJob({
  deps: [Ledger],
  async handler(input, store) {
    const output = store.credit(input);
    process.send({ kind: "committed", output });
    if (mode === "crash") await new Promise(() => {});
    return output;
  },
  metadata: { idempotencyKey: (input) => JSON.stringify([input.tenant, input.operation]) },
});
const jobs = createJobSystem({
  container: new Container().register(Ledger, { useValue: ledger }),
  jobs: { credit },
  backend: new RedisBackend({ queue, connection: { host: "127.0.0.1", port: Number(process.env.JOB_SYSTEM_REDIS_PORT) } }),
});
process.send({ kind: "initialized" });
process.once("message", async () => {
  await jobs.close();
  ledger.close();
  process.disconnect();
});

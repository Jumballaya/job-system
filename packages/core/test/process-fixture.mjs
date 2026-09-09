import { Container, createJobSystem, defineJob, JsonCodec, RedisBackend } from "../dist/index.js";

const [role, queue] = process.argv.slice(2);
const backend = new RedisBackend({
  queue,
  connection: { host: "127.0.0.1", port: Number(process.env.JOB_SYSTEM_REDIS_PORT) },
});

if (role === "worker") {
  const localHandler = defineJob({
    deps: [], handler: (input) => ({ value: input * 2, workerPid: process.pid }),
  });
  const ready = defineJob({ deps: [], handler() {} });
  const jobs = createJobSystem({ container: new Container(), jobs: { ready, double: localHandler }, backend });
  await ready(null).result();
  process.send({ kind: "ready", workerPid: process.pid });
  process.once("message", async () => {
    await jobs.close();
    process.disconnect();
  });
} else {
  const message = { id: crypto.randomUUID(), name: "double", input: new JsonCodec().encode(21), policy: { attempts: 1, backoff: { type: "fixed", delay: 0 } } };
  try {
    await backend.submit(message);
    const outcome = await backend.result(message.id);
    await backend.close();
    const reader = new RedisBackend({
      queue,
      connection: { host: "127.0.0.1", port: Number(process.env.JOB_SYSTEM_REDIS_PORT) },
    });
    try {
      const late = await reader.result(message.id);
      process.send({ kind: "completed", producerPid: process.pid, outcome, late });
    } finally {
      await reader.close();
    }
  } finally {
    await backend.close();
    process.disconnect();
  }
}

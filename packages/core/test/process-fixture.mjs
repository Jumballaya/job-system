import { RedisBackend } from "../dist/index.js";

const [role, queue] = process.argv.slice(2);
const backend = new RedisBackend({
  queue,
  connection: { host: "127.0.0.1", port: Number(process.env.JOB_SYSTEM_REDIS_PORT) },
});

if (role === "worker") {
  const worker = await backend.work(async (message) => ({
    status: "succeeded",
    output: JSON.stringify({ value: JSON.parse(message.input) * 2, workerPid: process.pid }),
  }));
  process.send({ kind: "ready", workerPid: process.pid });
  process.once("message", async () => {
    await worker.close();
    await backend.close();
    process.disconnect();
  });
} else {
  const message = { id: crypto.randomUUID(), name: "double", input: "21", policy: { attempts: 1, backoff: { type: "fixed", delay: 0 } } };
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

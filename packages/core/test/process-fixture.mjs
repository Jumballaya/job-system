import { Container, createJobSystem, defineJob, RedisBackend } from "../dist/index.js";

const [role, queue] = process.argv.slice(2);
const backend = new RedisBackend({
  queue,
  connection: { host: "127.0.0.1", port: Number(process.env.JOB_SYSTEM_REDIS_PORT) },
});

class Multiplier {
  multiply(value) { return { value: value * 2, workerPid: process.pid }; }
}
const double = defineJob({ deps: [Multiplier], handler: (input, multiplier) => multiplier.multiply(input) });

if (role === "worker") {
  const jobs = createJobSystem({ container: new Container().register(Multiplier), jobs: { double }, backend });
  process.send({ kind: "initialized", workerPid: process.pid });
  process.once("message", async () => {
    await jobs.close();
    process.disconnect();
  });
} else {
  const jobs = createJobSystem({ jobs: { double }, backend, worker: false });
  try {
    const handle = await double(21);
    if (role === "submit") {
      process.send({ kind: "accepted", id: handle.id });
    } else {
      const output = await handle.result();
      const outcome = await backend.result(handle.id);
      await jobs.close();
      const reader = new RedisBackend({
        queue,
        connection: { host: "127.0.0.1", port: Number(process.env.JOB_SYSTEM_REDIS_PORT) },
      });
      try {
        const late = await reader.result(handle.id);
        process.send({ kind: "completed", producerPid: process.pid, output, outcome, late });
      } finally {
        await reader.close();
      }
    }
  } finally {
    await jobs.close();
    process.disconnect();
  }
}

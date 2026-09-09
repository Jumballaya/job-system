import { createJobSystem, defineJob, RedisBackend } from "../../dist/index.js";

const settings = JSON.parse(process.argv[2]);
const gates = new Map();
const handles = new Map();
const attempts = new Map();
const emit = (event, details) => process.send({ event, pid: process.pid, at: Date.now(), ...details });
function gate(label) {
  if (!gates.has(label)) gates.set(label, Promise.withResolvers());
  return gates.get(label);
}

function definition(name, concurrency = Infinity) {
  return defineJob({
    deps: [],
    metadata: { concurrency, key: (input) => input.key ?? input.label, retries: { attempts: 2, backoff: { type: "fixed", delay: 100 } } },
    beforeRun(input, context) {
      attempts.set(context.jobId, context.attempt);
      emit("started", { name, input, id: context.jobId, attempt: context.attempt });
    },
    async handler(input, signal) {
      if (input.gate && settings.hold !== false) {
        if (input.cooperative) {
          try {
            await new Promise((resolve, reject) => {
              const abort = () => { emit("aborted", { name, input }); reject(signal.reason); };
              signal.addEventListener("abort", abort, { once: true });
              gate(input.gate).promise.then(resolve).finally(() => signal.removeEventListener("abort", abort));
              if (signal.aborted) abort();
            });
          } finally {
            if (input.cleanup) await gate(input.cleanup).promise;
            emit("cleanup", { name, input });
          }
          signal.throwIfAborted();
        } else {
          await gate(input.gate).promise;
        }
      }
      if (input.fail) throw new Error("provider unavailable");
      return { label: input.label, value: input.value ?? 42 };
    },
    onSuccess(output, context) {
      emit("effect", { name, input: { label: output.label }, output, id: context.jobId, attempt: attempts.get(context.jobId) });
    },
    onError(error, context) { emit("attemptFailed", { name, id: context.jobId, attempt: context.attempt, error: error.message }); },
  });
}

const catalog = { record: definition("record"), slow: definition("slow", 1), fast: definition("fast") };
const jobs = createJobSystem({
  jobs: catalog,
  backend: new RedisBackend({ queue: settings.queue, connection: settings.connection, resultTTLSeconds: settings.resultTTLSeconds ?? 300,
    failureTTLSeconds: settings.failureTTLSeconds ?? 300 }),
  concurrency: settings.concurrency ?? 4,
  worker: settings.worker ?? true,
  shutdownTimeoutMs: settings.shutdownTimeoutMs,
  onError(error) { emit("systemError", { error: { name: error.name, message: error.message } }); },
});

async function command(message) {
  switch (message.command) {
    case "call": {
      const handle = await catalog[message.name](message.input, message.timing);
      handles.set(handle.id, handle);
      return { id: handle.id };
    }
    case "result": return handles.get(message.id).result();
    case "inspect": return jobs.get(message.id);
    case "update": return jobs.schedule(message.id).update(message.timing);
    case "remove": return jobs.schedule(message.id).remove();
    case "release": gate(message.label).resolve(); return;
    case "close": return jobs.close();
    case "exit": await jobs.close(); return;
    default: throw new Error(`Unknown fixture command: ${message.command}`);
  }
}

process.on("message", async (message) => {
  try {
    const value = await command(message);
    process.send({ request: message.request, value }, () => {
      if (message.command === "exit") process.disconnect();
    });
  } catch (error) {
    process.send({ request: message.request, error: { name: error.name, message: error.message } });
  }
});
emit("initialized", {});

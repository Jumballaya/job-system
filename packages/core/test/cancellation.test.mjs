import assert from "node:assert/strict";
import { createServer } from "node:http";
import { setImmediate as nextTurn, setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { Container, createJobSystem, defineJob, JobExecutionError, MemoryBackend } from "../dist/index.js";

const deadline = 250;
const testOptions = { timeout: 10_000 };
const once = { attempts: 1 };
const twice = { attempts: 2, backoff: { type: "fixed", delay: 0 } };
const timedOut = (error) => error instanceof JobExecutionError && error.failure.name === "TimeoutError";

class HttpClient {
  async read(url, signal) {
    const response = await fetch(url, { signal });
    return response.text();
  }
}

function fixture(t) {
  const gates = [];
  const systems = [];
  const servers = [];
  t.after(async () => {
    for (const gate of gates) gate.resolve();
    const closing = servers.map((server) => {
      server.closeAllConnections();
      return new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    });
    const finished = new AbortController();
    try {
      await Promise.race([
        Promise.all([...closing, ...systems.map((system) => system.close())]),
        delay(2_000, undefined, { signal: finished.signal }).then(() => { throw new Error("Cancellation test cleanup did not drain"); }),
      ]);
    } finally { finished.abort(); }
  });
  return {
    observe(pending) {
      // Intermediate assertions can fail before await; cleanup must not create unhandled rejections.
      void pending.catch(() => {});
      return pending;
    },
    gate() {
      const gate = Promise.withResolvers();
      gates.push(gate);
      return gate;
    },
    system(jobs, options = {}) {
      const system = createJobSystem({
        container: new Container().register(HttpClient), jobs, backend: new MemoryBackend(), ...options,
      });
      systems.push(system);
      return system;
    },
    async server(handle) {
      const server = createServer(handle);
      await new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
      });
      servers.push(server);
      return `http://127.0.0.1:${server.address().port}`;
    },
  };
}

test("timeout closes an in-flight response and retries only after asynchronous cleanup finishes", testOptions, async (t) => {
  const env = fixture(t);
  const remoteClosed = env.gate();
  const cleaning = env.gate();
  const finishCleanup = env.gate();
  const events = [];
  const signals = [];
  const committed = [];
  let requests = 0;
  const url = await env.server((_request, response) => {
    requests++;
    if (requests > 1) { response.end("complete"); return; }
    response.on("close", () => {
      if (!response.writableEnded) remoteClosed.resolve();
    });
    response.writeHead(200, { "Content-Type": "text/plain" });
    response.write("unfinished body");
  });
  const read = defineJob({
    deps: [HttpClient],
    async handler(input, client, signal) {
      const attempt = signals.push(signal);
      events.push(`start ${attempt}`);
      try {
        const result = await client.read(input, signal);
        committed.push(result);
        return result;
      } finally {
        if (attempt === 1) { cleaning.resolve(); await finishCleanup.promise; }
        events.push(`clean ${attempt}`);
      }
    },
    metadata: { timeout: deadline, retries: twice },
    onError: (_error, context) => events.push(`error ${context.attempt}`),
  });
  env.system({ read });
  const result = env.observe(read(url).result());
  await cleaning.promise;
  await remoteClosed.promise;
  await nextTurn();
  assert.equal(requests, 1);
  assert.deepEqual(events, ["start 1"]);
  assert.deepEqual(committed, []);
  finishCleanup.resolve();
  assert.equal(await result, "complete");
  assert.deepEqual(events, ["start 1", "clean 1", "error 1", "start 2", "clean 2"]);
  assert.deepEqual(committed, ["complete"]);
  assert.notEqual(signals[0], signals[1]);
  assert.equal(signals[1].aborted, false);
});

test("canceling one deduplicated caller leaves the actual request and other result waits intact", testOptions, async (t) => {
  const env = fixture(t);
  const received = env.gate();
  let response;
  let requests = 0;
  let committed = 0;
  let disconnected = false;
  const url = await env.server((_request, reply) => {
    requests++;
    response = reply;
    reply.on("close", () => { if (!reply.writableEnded) disconnected = true; });
    reply.writeHead(200);
    reply.write("saved ");
    received.resolve();
  });
  const read = defineJob({
    deps: [HttpClient],
    async handler(input, client, signal) { const output = await client.read(input, signal); committed++; return output; },
    metadata: { timeout: Infinity, key: (input) => input },
  });
  env.system({ read });
  const first = await read(url);
  const duplicate = await read(url);
  const controller = new AbortController();
  const reason = new Error("client disconnected");
  const canceled = env.observe(assert.rejects(first.result({ signal: controller.signal }), (error) => error === reason));
  const remaining = env.observe(duplicate.result());
  await received.promise;
  controller.abort(reason);
  await canceled;
  await nextTurn();
  assert.equal(first.id, duplicate.id);
  assert.equal(disconnected, false);
  assert.equal(committed, 0);
  response.end("once");
  assert.equal(await remaining, "saved once");
  assert.equal(await first.result(), "saved once");
  assert.equal(requests, 1);
  assert.equal(committed, 1);
});

test("a handler ignoring timeout retains its dedupe reservation until it settles and cannot overlap its retry", testOptions, async (t) => {
  const env = fixture(t);
  const aborted = env.gate();
  const finish = env.gate();
  const events = [];
  let active = 0;
  let peak = 0;
  let attempts = 0;
  let successes = 0;
  const stubborn = defineJob({
    deps: [],
    async handler(_input, signal) {
      const attempt = ++attempts;
      peak = Math.max(peak, ++active);
      try {
        if (attempt === 1) {
          signal.addEventListener("abort", () => aborted.resolve(), { once: true });
          await finish.promise;
          events.push("late side effect");
          return "late success";
        }
        events.push("retry");
        return "recovered";
      } finally { active--; }
    },
    metadata: { timeout: deadline, retries: twice, key: () => "operation" },
    onError(_error, context) { events.push(`failed attempt ${context.attempt}`); },
    onSuccess() { successes++; },
  });
  env.system({ stubborn }, { concurrency: 2 });
  const original = await stubborn(null);
  let settled = false;
  const result = env.observe(original.result().finally(() => { settled = true; }));
  await aborted.promise;
  await nextTurn();
  const duplicate = await stubborn(null);
  assert.equal(duplicate.id, original.id);
  assert.equal(settled, false);
  assert.equal(attempts, 1);
  assert.equal(successes, 0);
  assert.deepEqual(events, []);
  finish.resolve();
  assert.equal(await result, "recovered");
  assert.equal(await duplicate.result(), "recovered");
  assert.deepEqual(events, ["late side effect", "failed attempt 1", "retry"]);
  assert.equal(peak, 1);
  assert.equal(successes, 1);
});

test("an execution ignoring cancellation holds its own capacity while queued work keeps a fresh deadline and other jobs run", testOptions, async (t) => {
  const env = fixture(t);
  const entered = env.gate();
  const finish = env.gate();
  const seen = [];
  const limited = defineJob({
    deps: [],
    async handler(input, signal) { seen.push(input); if (input === "blocked") { entered.resolve(signal); await finish.promise; } return input; },
    metadata: { timeout: deadline, retries: once, concurrency: 1 },
  });
  const probe = defineJob({ deps: [], handler: () => "worker slot available" });
  env.system({ limited, probe }, { concurrency: 2 });
  const first = limited("blocked").result().then((value) => ({ value }), (error) => ({ error }));
  const signal = await entered.promise;
  const queued = await limited("queued");
  if (!signal.aborted) await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
  assert.equal(await probe(null).result({ signal: AbortSignal.timeout(2_000) }), "worker slot available");
  assert.deepEqual(seen, ["blocked"]);
  finish.resolve();
  assert.ok(timedOut((await first).error));
  assert.equal(await queued.result(), "queued");
  assert.equal(await limited("healthy").result(), "healthy");
  assert.deepEqual(seen, ["blocked", "queued", "healthy"]);
});

test("timeout during beforeRun cancels pending work and prevents handler effects", testOptions, async (t) => {
  const env = fixture(t);
  const events = [];
  const job = defineJob({
    deps: [], handler() { events.push("handler committed"); },
    metadata: { timeout: deadline, retries: once },
    async beforeRun(_input, context) {
      try {
        await delay(60_000, undefined, { signal: context.signal });
        events.push("hook committed");
      } finally { events.push("hook cleaned"); }
    },
    onError(error) { events.push(error.name); },
    onSuccess() { events.push("success"); },
  });
  env.system({ job });
  await assert.rejects(job(null).result(), timedOut);
  assert.deepEqual(events, ["hook cleaned", "TimeoutError"]);
});

test("a success hook finishing after timeout cannot report success or retry completed handler effects", testOptions, async (t) => {
  const env = fixture(t);
  const aborted = env.gate();
  const finish = env.gate();
  let committed = 0;
  let errors = 0;
  const job = defineJob({
    deps: [], handler() { committed++; return "committed"; },
    metadata: { timeout: deadline, retries: twice },
    async onSuccess(_output, context) {
      context.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      await finish.promise;
    },
    onError() { errors++; },
  });
  env.system({ job });
  const result = job(null).result();
  const failed = env.observe(assert.rejects(result, timedOut));
  await aborted.promise;
  finish.resolve();
  await failed;
  assert.equal(committed, 1);
  assert.equal(errors, 0);
});

test("graceful shutdown rejects local waits but drains the live request and its cleanup before closing", testOptions, async (t) => {
  const env = fixture(t);
  const received = env.gate();
  const cleaning = env.gate();
  const finishCleanup = env.gate();
  const events = [];
  let response;
  let disconnected = false;
  const url = await env.server((_request, reply) => {
    response = reply;
    reply.on("close", () => { if (!reply.writableEnded) disconnected = true; });
    reply.writeHead(200);
    reply.write("drained ");
    received.resolve();
  });
  const job = defineJob({
    deps: [HttpClient],
    async handler(input, client, signal) {
      try { const output = await client.read(input, signal); events.push(output); return output; }
      finally { cleaning.resolve(); await finishCleanup.promise; events.push("cleaned"); }
    },
    metadata: { timeout: Infinity, retries: once },
  });
  const jobs = env.system({ job });
  const waiting = env.observe(assert.rejects(job(url).result(), /closed/));
  await received.promise;
  let closed = false;
  const closing = env.observe(jobs.close().then(() => { closed = true; }));
  await waiting;
  await assert.rejects(job(url), /not attached/);
  assert.equal(disconnected, false);
  response.end("once");
  await cleaning.promise;
  await nextTurn();
  assert.equal(closed, false);
  assert.deepEqual(events, ["drained once"]);
  finishCleanup.resolve();
  await closing;
  assert.deepEqual(events, ["drained once", "cleaned"]);
});

test("backend execution cancellation aborts a live request even when the job has no timeout", testOptions, async (t) => {
  const env = fixture(t);
  const received = env.gate();
  const remoteClosed = env.gate();
  const execution = new AbortController();
  const reason = new Error("execution lease lost");
  let committed = 0;
  let cleaned = 0;
  let hookError;
  class Backend extends MemoryBackend {
    async work(execute, options) {
      return super.work((message, _signal, attempt) => execute(message, execution.signal, attempt), options);
    }
  }
  const url = await env.server((_request, response) => {
    response.on("close", () => { if (!response.writableEnded) remoteClosed.resolve(); });
    response.writeHead(200);
    response.write("unfinished");
    received.resolve();
  });
  const job = defineJob({
    deps: [HttpClient],
    async handler(input, client, signal) {
      try { const output = await client.read(input, signal); committed++; return output; }
      finally { cleaned++; }
    },
    metadata: { timeout: Infinity, retries: once },
    onError(error) { hookError = error; },
  });
  env.system({ job }, { backend: new Backend() });
  const failed = env.observe(assert.rejects(job(url).result(), (error) => error instanceof JobExecutionError && error.message === reason.message));
  await received.promise;
  execution.abort(reason);
  await failed;
  await remoteClosed.promise;
  assert.equal(hookError, reason);
  assert.equal(committed, 0);
  assert.equal(cleaned, 1);
});

test("execution revoked before dispatch cannot enter its handler or strand the next job", testOptions, async (t) => {
  const env = fixture(t);
  const release = env.gate();
  const controllers = new Map();
  const seen = [];
  const reason = new Error("execution revoked during handoff");
  class Backend extends MemoryBackend {
    async work(execute, options) {
      return super.work((message, _signal, attempt) => {
        const controller = controllers.get(message.id) ?? new AbortController();
        controllers.set(message.id, controller);
        return execute(message, controller.signal, attempt);
      }, options);
    }
  }
  let canceledId;
  const job = defineJob({
    deps: [],
    async handler(input) { seen.push(input); if (input === "first") await release.promise; return input; },
    metadata: { timeout: Infinity, retries: once, concurrency: 1 },
    onSuccess(output) {
      if (output === "first") controllers.get(canceledId).abort(reason);
    },
  });
  env.system({ job }, { backend: new Backend(), concurrency: 3 });
  const first = await job("first");
  const canceled = await job("canceled");
  canceledId = canceled.id;
  controllers.set(canceledId, new AbortController());
  const next = await job("next");
  const rejected = env.observe(assert.rejects(canceled.result(), (error) => error instanceof JobExecutionError && error.message === reason.message));
  release.resolve();
  assert.equal(await first.result(), "first");
  await rejected;
  assert.equal(await next.result({ signal: AbortSignal.timeout(2_000) }), "next");
  assert.equal(await job("later").result(), "later");
  assert.deepEqual(seen, ["first", "next", "later"]);
});

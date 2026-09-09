import assert from "node:assert/strict";
import { test } from "node:test";
import { Container, createJobSystem, defineJob, JsonCodec, MemoryBackend } from "../dist/index.js";

test("initialization consumes existing work through its DI scope and hooks without a local submission", { timeout: 5_000 }, async (t) => {
  const events = [];
  let submissions = 0;
  class Backend extends MemoryBackend {
    async submit(message) { submissions++; return super.submit(message); }
  }
  class Multiplier { multiply(value) { return value * 2; } }
  const double = defineJob({
    deps: [Multiplier],
    handler(input, multiplier, signal) {
      signal.throwIfAborted();
      events.push("handler");
      return multiplier.multiply(input);
    },
    beforeRun() { events.push("before"); },
    onSuccess() { events.push("success"); },
  });
  const backend = new Backend();
  const codec = new JsonCodec();
  await backend.submit({
    id: "existing", name: "double", input: codec.encode(21),
    policy: { attempts: 1, backoff: { type: "fixed", delay: 0 } },
  });
  const jobs = createJobSystem({ container: new Container().register(Multiplier), jobs: { double }, backend });
  t.after(() => jobs.close());
  assert.equal(codec.decode((await backend.result("existing")).output), 42);
  assert.deepEqual(events, ["before", "handler", "success"]);
  assert.equal(submissions, 1, "startup must not enqueue a bootstrap job");
  await jobs.close();
  await assert.rejects(double(1), /not attached/);
  await assert.rejects(backend.submit({}), /closed/);
});

test("a submit-only system accepts work without worker dependencies or a local consumer", async () => {
  let starts = 0;
  let accepted;
  class Backend extends MemoryBackend {
    async work(...args) { starts++; return super.work(...args); }
    async submit(message) { accepted = message; return super.submit(message); }
  }
  class WorkerOnlyService {}
  const job = defineJob({ deps: [WorkerOnlyService], handler() { assert.fail("producer executed a handler"); } });
  const jobs = createJobSystem({ jobs: { job }, backend: new Backend(), worker: false });
  try {
    const handle = await job({ value: 42 });
    assert.equal(handle.id, accepted.id);
    assert.deepEqual(new JsonCodec().decode(accepted.input), { value: 42 });
    assert.equal(starts, 0);
    const waiting = assert.rejects(handle.result(), /closed/);
    await jobs.close();
    await waiting;
    await assert.rejects(job({ value: 1 }), /not attached/);
  } finally {
    await jobs.close();
  }
});

test("initialization starts one worker and submissions wait for it to be ready", async (t) => {
  const ready = Promise.withResolvers();
  let starts = 0;
  let closes = 0;
  let submitted = 0;
  class Backend extends MemoryBackend {
    async work(...args) { starts++; await ready.promise; return super.work(...args); }
    async submit(message) { submitted++; return super.submit(message); }
    async close() { closes++; return super.close(); }
  }
  const echo = defineJob({ deps: [], handler: (input) => input });
  const jobs = createJobSystem({ jobs: { echo }, backend: new Backend() });
  t.after(async () => { ready.resolve(); await jobs.close(); });
  assert.equal(starts, 1, "initialization must start the worker before any call");
  const result = echo("value").result();
  const second = echo("another").result();
  await Promise.resolve();
  assert.equal(starts, 1);
  assert.equal(submitted, 0);
  ready.resolve();
  assert.deepEqual(await Promise.all([result, second]), ["value", "another"]);
  assert.equal(starts, 1);
  await Promise.all([jobs.close(), jobs.close()]);
  assert.equal(closes, 1);
});

test("closing immediately after initialization drains the late worker even without a submission", async () => {
  const ready = Promise.withResolvers();
  const closed = [];
  const backend = {
    async work() {
      await ready.promise;
      return { done: new Promise(() => {}), async close() { closed.push("worker"); } };
    },
    async close() { closed.push("backend"); },
    async submit() { assert.fail("closed system accepted work"); },
  };
  const echo = defineJob({ deps: [], handler: (input) => input });
  const jobs = createJobSystem({ jobs: { echo }, backend });
  const closing = jobs.close();
  await assert.rejects(echo(1), /not attached/);
  ready.resolve();
  await closing;
  assert.deepEqual(closed, ["worker", "backend"]);
});

test("background startup failure is retained for callers and shutdown without an unhandled rejection", async () => {
  const unavailable = new Error("worker connection unavailable");
  let starts = 0;
  let closed = false;
  const backend = {
    async work() { starts++; throw unavailable; },
    async close() { closed = true; },
    async submit() { assert.fail("unready worker accepted work"); },
  };
  const job = defineJob({ deps: [], handler() {} });
  const jobs = createJobSystem({ jobs: { job }, backend });
  await new Promise((resolve) => setImmediate(resolve));
  await assert.rejects(job(null), (error) => error === unavailable);
  await assert.rejects(jobs.close(), (error) => error === unavailable);
  assert.equal(starts, 1);
  assert.equal(closed, true);
});

test("a rejected catalog neither starts a consumer nor attaches its valid definitions", async () => {
  const backend = {
    async work() { assert.fail("invalid catalog started a consumer"); },
  };
  const echo = defineJob({ deps: [], handler: (input) => input });
  assert.throws(() => createJobSystem({ jobs: { echo, invalid: {} }, backend }), /defineJob/);
  await assert.rejects(echo(1), /not attached/);
});

import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { Queue } from "bullmq";
import { Container, createJobSystem, defineJob, JsonCodec, RedisBackend } from "../dist/index.js";

const port = Number(process.env.JOB_SYSTEM_REDIS_PORT);
const redis = { skip: !port, timeout: 20_000 };
const success = (output) => ({ status: "succeeded", output });
const single = { attempts: 1, backoff: { type: "fixed", delay: 0 } };
const triple = { attempts: 3, backoff: { type: "fixed", delay: 0 } };

function fixture(t, settings = {}) {
  const queue = `job-system-test-${crypto.randomUUID()}`;
  const connection = { host: "127.0.0.1", port };
  const backends = [];
  t.after(async () => {
    await Promise.all(backends.map((backend) => backend.close()));
    const cleanup = new Queue(queue, { connection });
    try { await cleanup.obliterate({ force: true }); }
    finally { await cleanup.close(); }
  });
  return {
    queue,
    backend() {
      const backend = new RedisBackend({ queue, connection, ...settings });
      backends.push(backend);
      return backend;
    },
  };
}

function launch(t, role, queue) {
  const child = fork(fileURLToPath(new URL("./process-fixture.mjs", import.meta.url)), [role, queue], {
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  let errors = "";
  child.stderr.on("data", (chunk) => { errors += chunk; });
  const messages = [];
  const waiters = new Map();
  child.on("message", (message) => {
    messages.push(message);
    waiters.get(message.kind)?.resolve(message);
  });
  child.on("exit", (code) => {
    for (const waiter of waiters.values()) waiter.reject(new Error(`Child exited ${code}: ${errors}`));
  });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  return {
    child,
    message(kind) {
      const received = messages.find((message) => message.kind === kind);
      if (received) return Promise.resolve(received);
      return new Promise((resolve, reject) => { waiters.set(kind, { resolve, reject }); });
    },
  };
}

test("separate producer and worker processes exchange retained results", redis, async (t) => {
  const env = fixture(t);
  const worker = launch(t, "worker", env.queue);
  const ready = await worker.message("ready");
  const producer = launch(t, "producer", env.queue);
  const completed = await producer.message("completed");
  assert.notEqual(completed.producerPid, ready.workerPid);
  assert.notEqual(ready.workerPid, process.pid);
  assert.deepEqual(new JsonCodec().decode(completed.outcome.output), { value: 42, workerPid: ready.workerPid });
  assert.deepEqual(completed.late, completed.outcome);
  const workerExit = once(worker.child, "exit");
  worker.child.send("close");
  assert.equal((await workerExit)[0], 0);
  if (producer.child.exitCode === null) assert.equal((await once(producer.child, "exit"))[0], 0);
});

test("identical submissions share one execution and conflicting IDs reject", redis, async (t) => {
  const backend = fixture(t).backend();
  let calls = 0;
  await backend.work(async () => { calls++; return success("answer"); });
  const message = { id: "id:with:separator", name: "job", input: "first", policy: single };
  await Promise.all([backend.submit(message), backend.submit(message)]);
  assert.deepEqual(await backend.result(message.id), success("answer"));
  await assert.rejects(backend.submit({ ...message, input: "different" }), /already belongs/);
  assert.equal(calls, 1);
});

test("application failures are terminal while infrastructure failures can recover", redis, async (t) => {
  const backend = fixture(t).backend();
  let businessCalls = 0;
  let infrastructureCalls = 0;
  const failure = { status: "failed", error: { name: "DomainError", message: "no" } };
  await backend.work(async (message) => {
    if (message.name === "business") { businessCalls++; return failure; }
    if (++infrastructureCalls < 3) throw new Error("temporary infrastructure failure");
    return success("recovered");
  });
  await backend.submit({ id: "business", name: "business", input: "null", policy: single });
  assert.deepEqual(await backend.result("business"), failure);
  await backend.submit({ id: "infra", name: "infra", input: "null", policy: triple });
  assert.deepEqual(await backend.result("infra"), success("recovered"));
  assert.equal(businessCalls, 1);
  assert.equal(infrastructureCalls, 3);
});

test("aborting one result wait leaves accepted work and other waiters intact", redis, async (t) => {
  const env = fixture(t);
  const producer = env.backend();
  await producer.submit({ id: "waiting", name: "job", input: "null", policy: single });
  const abort = new AbortController();
  const reason = new Error("caller stopped waiting");
  const canceled = producer.result("waiting", { signal: abort.signal });
  const rejected = assert.rejects(canceled, (error) => error === reason);
  const remaining = producer.result("waiting");
  abort.abort(reason);
  await rejected;
  await env.backend().work(async () => success("finished"));
  assert.deepEqual(await remaining, success("finished"));
});

test("close rejects waits and new operations while draining an active execution", redis, async (t) => {
  const env = fixture(t);
  const backend = env.backend();
  let enter;
  const entered = new Promise((resolve) => { enter = resolve; });
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const worker = await backend.work(async () => { enter(); await gate; return success("drained"); });
  await backend.submit({ id: "active", name: "job", input: "null", policy: single });
  await entered;
  const waiting = assert.rejects(backend.result("active"), /closed/);
  const closing = backend.close();
  release();
  await Promise.all([closing, waiting, worker.done, worker.close(), backend.close()]);
  await assert.rejects(backend.submit({ id: "late", name: "job", input: "null", policy: single }), /closed/);
  await assert.rejects(backend.work(async () => success("no")), /closed/);
  assert.deepEqual(await env.backend().result("active"), success("drained"));
});

test("expired and unknown outcomes reject without requiring a later completion", redis, async (t) => {
  const backend = fixture(t, { resultTTLSeconds: 1 }).backend();
  await backend.work(async () => success("retained briefly"));
  await backend.submit({ id: "expires", name: "job", input: "null", policy: single });
  assert.deepEqual(await backend.result("expires"), success("retained briefly"));
  await new Promise((resolve) => setTimeout(resolve, 1_100));
  await assert.rejects(backend.result("expires"), { name: "ResultUnavailableError" });
  await assert.rejects(backend.submit({ id: "expires", name: "job", input: "null", policy: single }),
    { name: "ResultUnavailableError" });
  await assert.rejects(backend.result("absent"), { name: "ResultUnavailableError" });
});

test("exhausted infrastructure retries reject the result without stopping the worker", redis, async (t) => {
  const backend = fixture(t).backend();
  let calls = 0;
  await backend.work(async (message) => {
    if (message.name === "broken") { calls++; throw new Error("cannot reach dependency"); }
    return success("worker remains available");
  });
  await backend.submit({ id: "broken", name: "broken", input: "null", policy: triple });
  await assert.rejects(backend.result("broken"), /infrastructure failed: cannot reach dependency/);
  assert.equal(calls, 3);
  await backend.submit({ id: "healthy", name: "healthy", input: "null", policy: single });
  assert.deepEqual(await backend.result("healthy"), success("worker remains available"));
});

test("closing during startup rejects readiness and releases owned connections", redis, async (t) => {
  const backend = fixture(t).backend();
  const starting = assert.rejects(backend.work(async () => success("unused")), /closed/);
  await Promise.all([starting, backend.close()]);
});


test("job systems serialize inputs, inject worker services, and await lifecycle hooks over Redis", redis, async (t) => {
  const env = fixture(t);
  const events = [];
  class Counter {
    total = 0;
    add(amount) { this.total += amount; return this.total; }
  }
  const add = defineJob({
    deps: [Counter],
    handler(input, counter, signal) {
      assert.equal(signal.aborted, false);
      assert.ok(counter instanceof Counter);
      events.push(["handler", input.amount]);
      return { total: counter.add(input.amount), label: input.label };
    },
    async beforeRun(input, context) {
      await Promise.resolve();
      events.push(["before", context.jobId, input.amount]);
    },
    async onSuccess(output, context) {
      await Promise.resolve();
      events.push(["success", context.jobId, output.total]);
    },
  });
  const jobs = createJobSystem({
    container: new Container().register(Counter), jobs: { add }, backend: env.backend(), concurrency: 2,
  });
  try {
    const input = { amount: 2, label: "submitted snapshot" };
    const first = add(input).result();
    input.amount = 100;
    input.label = "changed after submission";
    const firstOutput = await first;
    const firstId = events[0][1];
    assert.deepEqual(firstOutput, { total: 2, label: "submitted snapshot" });
    assert.deepEqual(events, [
      ["before", firstId, 2], ["handler", 2], ["success", firstId, 2],
    ]);
    assert.deepEqual(await add({ amount: 3, label: "second" }).result(),
      { total: 5, label: "second" });
    assert.equal(events.length, 6);
    await jobs.close();
    await assert.rejects(add({ amount: 1, label: "closed" }), /not attached/);
  } finally {
    await jobs.close();
  }
});


test("Redis deduplication shares work only within the same registered job", redis, async (t) => {
  const env = fixture(t);
  const release = Promise.withResolvers();
  let firstCalls = 0;
  let secondCalls = 0;
  const first = defineJob({
    deps: [], async handler() { firstCalls++; await release.promise; return 42; },
    metadata: { key: () => "same-key" },
  });
  const second = defineJob({
    deps: [], async handler() { secondCalls++; await release.promise; return "second"; },
    metadata: { key: () => "same-key" },
  });
  const jobs = createJobSystem({ container: new Container(), jobs: { first, second }, backend: env.backend(), concurrency: 2 });
  try {
    const original = await first(null);
    const duplicate = await first(null);
    const other = await second(null);
    assert.equal(original.id, duplicate.id);
    assert.notEqual(original.id, other.id);
    release.resolve();
    assert.deepEqual(await Promise.all([original.result(), duplicate.result(), other.result()]), [42, 42, "second"]);
    assert.equal(firstCalls, 1);
    assert.equal(secondCalls, 1);
  } finally {
    release.resolve();
    await jobs.close();
  }
});

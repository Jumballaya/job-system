import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { MemoryBackend, ResultUnavailableError } from "../dist/index.js";

const once = { attempts: 1, backoff: { type: "fixed", delay: 0 } };
const message = (id, policy = once) => ({ id, name: "example", input: "null", policy });
const success = (output = "null") => ({ status: "succeeded", output });

test("accepted work waits for a worker and its outcome can be read repeatedly", async (t) => {
  const backend = new MemoryBackend();
  t.after(() => backend.close());
  await backend.submit(message("first"));
  const pending = backend.result("first");
  let calls = 0;
  await backend.work(async (job) => {
    calls++;
    assert.equal(job.id, "first");
    return success("42");
  });
  assert.deepEqual(await pending, success("42"));
  assert.deepEqual(await backend.result("first"), success("42"));
  assert.equal(calls, 1);
  await assert.rejects(backend.result("unknown"), ResultUnavailableError);
});

test("concurrency bounds active executions while completed slots admit queued work", { timeout: 5_000 }, async (t) => {
  const backend = new MemoryBackend();
  const release = Array.from({ length: 3 }, () => Promise.withResolvers());
  const twoStarted = Promise.withResolvers();
  const threeStarted = Promise.withResolvers();
  let active = 0;
  let maximum = 0;
  let started = 0;
  t.after(async () => {
    for (const gate of release) gate.resolve();
    await backend.close();
  });
  await backend.work(async () => {
    const index = started++;
    maximum = Math.max(maximum, ++active);
    if (started === 2) twoStarted.resolve();
    if (started === 3) threeStarted.resolve();
    await release[index].promise;
    active--;
    return success();
  }, { concurrency: 2 });
  for (const id of ["one", "two", "three"]) await backend.submit(message(id));
  await twoStarted.promise;
  assert.equal(started, 2);
  release[0].resolve();
  await threeStarted.promise;
  assert.equal(maximum, 2);
  release[1].resolve();
  release[2].resolve();
  await Promise.all(["one", "two", "three"].map((id) => backend.result(id)));
});

test("aborting one result wait leaves accepted work and other waits intact", async (t) => {
  const backend = new MemoryBackend();
  t.after(() => backend.close());
  await backend.submit(message("pending"));
  const controller = new AbortController();
  const aborted = assert.rejects(backend.result("pending", { signal: controller.signal }), /caller stopped waiting/);
  const other = backend.result("pending");
  controller.abort(new Error("caller stopped waiting"));
  await aborted;
  await backend.work(async () => success("7"));
  assert.deepEqual(await other, success("7"));
});

test("worker closure drains active work and leaves queued work for another worker", { timeout: 5_000 }, async (t) => {
  const backend = new MemoryBackend();
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  t.after(async () => { release.resolve(); await backend.close(); });
  const first = await backend.work(async () => {
    entered.resolve();
    await release.promise;
    return success("1");
  });
  await backend.submit(message("active"));
  await entered.promise;
  await backend.submit(message("queued"));
  let closed = false;
  const closing = first.close().then(() => { closed = true; });
  await Promise.resolve();
  assert.equal(closed, false);
  release.resolve();
  await closing;
  await first.done;
  await first.close();
  assert.deepEqual(await backend.result("active"), success("1"));
  await backend.work(async () => success("2"));
  assert.deepEqual(await backend.result("queued"), success("2"));
});

test("backend closure rejects pending waits and remains safe to repeat", async () => {
  const backend = new MemoryBackend();
  await backend.submit(message("unhandled"));
  const rejected = assert.rejects(backend.result("unhandled"), /clos/i);
  await Promise.all([backend.close(), backend.close()]);
  await rejected;
  await backend.close();
  await assert.rejects(backend.submit(message("late")), /clos/i);
  await assert.rejects(backend.work(async () => success()), /clos/i);
});

test("executor rejection fails its worker without consuming unrelated queued work", async (t) => {
  const backend = new MemoryBackend();
  t.after(() => backend.close());
  await backend.submit(message("broken"));
  await backend.submit(message("later"));
  const failedResult = assert.rejects(backend.result("broken"), /transport failed/);
  const worker = await backend.work(async () => { throw new Error("transport failed"); });
  await assert.rejects(worker.done, /transport failed/);
  await failedResult;
  await backend.work(async () => success("9"));
  assert.deepEqual(await backend.result("later"), success("9"));
});

test("a failed worker waits for its other active execution before reporting shutdown", { timeout: 5_000 }, async (t) => {
  const backend = new MemoryBackend();
  const bothStarted = Promise.withResolvers();
  const fail = Promise.withResolvers();
  const finish = Promise.withResolvers();
  let started = 0;
  t.after(async () => { fail.resolve(); finish.resolve(); await backend.close(); });
  const worker = await backend.work(async (job) => {
    if (++started === 2) bothStarted.resolve();
    if (job.id === "broken") {
      await fail.promise;
      throw new Error("delivery failed");
    }
    await finish.promise;
    return success("completed");
  }, { concurrency: 2 });
  let shutdownReported = false;
  const shutdown = assert.rejects(worker.done, /delivery failed/).then(() => { shutdownReported = true; });
  await backend.submit(message("broken"));
  await backend.submit(message("active"));
  const failedResult = assert.rejects(backend.result("broken"), /delivery failed/);
  await bothStarted.promise;
  fail.resolve();
  await failedResult;
  assert.equal(shutdownReported, false);
  finish.resolve();
  await shutdown;
  assert.deepEqual(await backend.result("active"), success("completed"));
});

test("duplicate submission preserves the first payload and executes only once", async (t) => {
  const backend = new MemoryBackend();
  t.after(() => backend.close());
  await backend.submit(message("same"));
  await backend.submit(message("same"));
  await assert.rejects(backend.submit({ ...message("same"), input: "42" }));
  let calls = 0;
  await backend.work(async () => { calls++; return success(); });
  await backend.result("same");
  await backend.submit(message("same"));
  assert.equal(calls, 1);
});

test("terminal outcomes expire instead of remaining available indefinitely", async (t) => {
  const backend = new MemoryBackend({ resultTTLms: 10 });
  t.after(() => backend.close());
  await backend.submit(message("short-lived"));
  const firstRead = backend.result("short-lived");
  await backend.work(async () => success());
  assert.deepEqual(await firstRead, success());
  await delay(30);
  await assert.rejects(backend.result("short-lived"), ResultUnavailableError);
});

test("invalid concurrency and retention fail before a worker starts", async () => {
  for (const resultTTLms of [0, -1, NaN, Infinity]) {
    assert.throws(() => new MemoryBackend({ resultTTLms }));
  }
  const backend = new MemoryBackend();
  try {
    for (const concurrency of [0, -1, 1.5, NaN, Infinity]) {
      await assert.rejects(backend.work(async () => success(), { concurrency }));
    }
  } finally {
    await backend.close();
  }
});

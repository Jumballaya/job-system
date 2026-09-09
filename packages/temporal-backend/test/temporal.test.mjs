import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { setTimeout as pause } from "node:timers/promises";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, DefaultLogger, Runtime } from "@temporalio/worker";
import { Container, createJobSystem, defineJob, IdempotencyConflictError, JsonCodec, NonRetryableError, ShutdownTimeoutError } from "core";
import { TemporalBackend } from "../dist/index.js";

let env;
let workflowBundle;
before(async () => {
  Runtime.install({ logger: new DefaultLogger("ERROR") });
  env = await TestWorkflowEnvironment.createLocal();
  workflowBundle = await bundleWorkflowCode({ workflowsPath: fileURLToPath(new URL("../dist/workflows.js", import.meta.url)), logger: new DefaultLogger("ERROR") });
});
after(async () => { await env?.teardown(); });

function backend(taskQueue) {
  return new TemporalBackend({ client: env.client, taskQueue, worker: { connection: env.nativeConnection, workflowBundle } });
}
async function eventually(read, accepts) {
  const until = Date.now() + 10_000;
  do {
    const value = await read();
    if (accepts(value)) return value;
    await pause(50);
  } while (Date.now() < until);
  assert.fail("Temporal did not reach the expected state");
}

test("a new worker consumes delayed producer work with DI, retries, inspection, and independent result cancellation", { timeout: 30_000 }, async (t) => {
  const queue = crypto.randomUUID();
  const producerJob = defineJob({ deps: [], handler() { assert.fail("producer executed work"); } });
  const producer = createJobSystem({ jobs: { job: producerJob }, backend: backend(queue), worker: false });
  t.after(() => producer.close());
  const submittedAt = Date.now();
  const handle = await producerJob(21, { after: "1s" });
  assert.equal((await producer.get(handle.id)).status, "queued");
  const cancellation = new AbortController();
  const reason = new Error("caller left");
  const canceled = assert.rejects(handle.result({ signal: cancellation.signal }), (error) => error === reason);
  cancellation.abort(reason);
  await canceled;
  class Multiplier { double(input) { return input * 2; } }
  const attempts = [];
  const job = defineJob({ deps: [Multiplier], metadata: { retries: { attempts: 3, backoff: { type: "fixed", delay: 20 } } },
    beforeRun(_, context) { attempts.push(context.attempt); assert.equal(context.jobId, handle.id); },
    handler(input, multiplier) {
      assert.ok(Date.now() >= submittedAt + 1_000);
      if (attempts.length < 2) throw new Error("temporary");
      return multiplier.double(input);
    } });
  const worker = createJobSystem({ container: new Container().register(Multiplier), jobs: { job }, backend: backend(queue) });
  t.after(() => worker.close());
  assert.equal(await handle.result(), 42);
  assert.deepEqual(attempts, [1, 2]);
  const record = await producer.get(handle.id);
  assert.equal(record.status, "succeeded");
  assert.equal(record.attempts, 2);
  assert.equal(record.output, 42);
  assert.equal(await producer.get("unknown-id"), null);
  await worker.close();
  assert.equal(await handle.result(), 42, "results remain readable after workers close");
});

test("active coalescing pins old results and durable operation records survive backend recreation with input conflicts", { timeout: 30_000 }, async (t) => {
  const queue = crypto.randomUUID();
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  let effects = 0;
  const keyed = defineJob({ deps: [], metadata: { key: () => "same" }, async handler(input) { entered.resolve(); await release.promise; return input; } });
  const operation = defineJob({ deps: [], metadata: { idempotencyKey: (input) => input.id }, handler(input) { effects++; return input.amount; } });
  const jobs = createJobSystem({ jobs: { keyed, operation }, backend: backend(queue), concurrency: 2 });
  t.after(async () => { release.resolve(); await jobs.close(); });
  const first = await keyed(1);
  await entered.promise;
  const duplicate = await keyed(2);
  assert.equal(first.id, duplicate.id);
  release.resolve();
  assert.equal(await first.result(), 1);
  const later = await keyed(3);
  assert.notEqual(later.id, first.id);
  assert.equal(await later.result(), 3);
  assert.equal(await first.result(), 1);
  const permanent = await operation({ id: "credit", amount: 10 });
  assert.equal(await permanent.result(), 10);
  await jobs.close();
  const reopened = createJobSystem({ jobs: { keyed, operation }, backend: backend(queue) });
  t.after(() => reopened.close());
  const replay = await operation({ amount: 10, id: "credit" });
  assert.equal(replay.id, permanent.id);
  assert.equal(await replay.result(), 10);
  await assert.rejects(operation({ id: "credit", amount: 20 }), IdempotencyConflictError);
  assert.equal(effects, 1);
  assert.equal((await reopened.get(permanent.id)).status, "succeeded");
});

test("timed-out handlers finish cleanup before retry and saturated work leaves capacity for other jobs", { timeout: 30_000 }, async (t) => {
  const cleanup = Promise.withResolvers();
  const aborted = Promise.withResolvers();
  let attempts = 0;
  const work = defineJob({ deps: [], metadata: { concurrency: 1, timeout: 100, retries: { attempts: 2, backoff: "fixed" } },
    async handler(input, signal) {
      if (input === "first" && ++attempts === 1) {
        await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
        aborted.resolve();
        await cleanup.promise;
      }
      return input;
    } });
  const fast = defineJob({ deps: [], handler: (input) => input });
  const jobs = createJobSystem({ jobs: { work, fast }, backend: backend(crypto.randomUUID()), concurrency: 3 });
  t.after(async () => { cleanup.resolve(); await jobs.close(); });
  const first = work("first").result();
  await aborted.promise;
  const waiting = Array.from({ length: 8 }, (_, i) => work(`queued-${i}`).result());
  assert.equal(await fast("still available").result(), "still available");
  assert.equal(attempts, 1, "timeout must not overlap an unsettled handler with its retry");
  cleanup.resolve();
  assert.equal(await first, "first");
  assert.equal(attempts, 2);
  assert.equal((await Promise.all(waiting)).length, 8);
});

test("native schedules coalesce registrations, survive producer replacement, update and remove through their handle", { timeout: 30_000 }, async (t) => {
  const queue = crypto.randomUUID();
  const seen = [];
  const job = defineJob({ deps: [], metadata: { key: () => "same" }, beforeRun(_, context) { seen.push(context.jobId); }, handler: (input) => input });
  const jobs = createJobSystem({ jobs: { job }, backend: backend(queue) });
  t.after(() => jobs.close());
  const [schedule, duplicate] = await Promise.all([job({ value: 1 }, { every: "1s" }), job({ value: 1 }, { every: "1s" })]);
  t.after(() => env.client.schedule.getHandle(schedule.id).delete().catch(() => {}));
  assert.equal(schedule.id, duplicate.id);
  await eventually(() => seen.length, (count) => count >= 2);
  assert.equal(new Set(seen).size, seen.length);
  await jobs.close();
  const replacement = createJobSystem({ jobs: { job }, backend: backend(queue) });
  t.after(() => replacement.close());
  const existing = replacement.schedule(schedule.id);
  const count = seen.length;
  await eventually(() => seen.length, (length) => length > count);
  await existing.update({ daily: "09:00", timezone: "America/New_York" });
  assert.equal((await env.client.schedule.getHandle(schedule.id).describe()).spec.timezone, "America/New_York");
  await job({ value: 1 }, { every: "2s" });
  assert.equal((await env.client.schedule.getHandle(schedule.id).describe()).spec.intervals[0].every, 2_000);
  await existing.remove();
  await existing.remove();
  await assert.rejects(existing.update({ every: "1s" }));
});

test("shutdown aborts cooperative work and a replacement retries it without spending a business attempt", { timeout: 30_000 }, async (t) => {
  const queue = crypto.randomUUID();
  const entered = Promise.withResolvers();
  const attempts = [];
  let stopping = true;
  const job = defineJob({ deps: [], metadata: { timeout: Infinity, retries: { attempts: 1 } },
    beforeRun(_, context) { attempts.push(context.attempt); },
    async handler(_, signal) {
      if (!stopping) return "recovered";
      entered.resolve();
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
      signal.throwIfAborted();
    } });
  const oldBackend = backend(queue);
  const old = createJobSystem({ jobs: { job }, backend: oldBackend, shutdownTimeoutMs: 25 });
  const handle = await job(null);
  await entered.promise;
  await assert.rejects(old.close(), ShutdownTimeoutError);
  await oldBackend.close();
  stopping = false;
  const replacementBackend = backend(queue);
  const replacement = createJobSystem({ jobs: { job }, backend: replacementBackend });
  t.after(() => replacement.close());
  assert.equal(new JsonCodec().decode((await replacementBackend.result(handle.id)).output), "recovered");
  assert.deepEqual(attempts, [1, 1]);
  await replacement.close();
  assert.ok(await env.client.workflowService.getSystemInfo({}), "borrowed connections remain usable");
});

test("an existing workflow and activity share the worker with jobs, whose terminal errors remain inspectable", { timeout: 30_000 }, async (t) => {
  const taskQueue = crypto.randomUUID();
  const combined = await bundleWorkflowCode({ workflowsPath: fileURLToPath(new URL("./existing-workflows.mjs", import.meta.url)), logger: new DefaultLogger("ERROR") });
  const strategy = new TemporalBackend({ client: env.client, taskQueue,
    worker: { connection: env.nativeConnection, workflowBundle: combined, activities: { existingActivity: (input) => input * 2 } } });
  let calls = 0;
  const job = defineJob({ deps: [], handler() { calls++; throw new NonRetryableError("refused"); } });
  const jobs = createJobSystem({ jobs: { job }, backend: strategy });
  t.after(() => jobs.close());
  const handle = await job(null);
  await assert.rejects(handle.result(), /refused/);
  assert.equal((await jobs.get(handle.id)).status, "failed");
  assert.equal(calls, 1);
  assert.equal(await env.client.workflow.execute("existingWorkflow", { workflowId: crypto.randomUUID(), taskQueue, args: [21] }), 42);
});

test("idempotent outcomes remain readable after a month and history compaction without rerunning effects", { timeout: 60_000 }, async () => {
  const clock = await TestWorkflowEnvironment.createTimeSkipping();
  let jobs;
  try {
    const strategy = new TemporalBackend({ client: clock.client, taskQueue: crypto.randomUUID(),
      worker: { connection: clock.nativeConnection, workflowBundle } });
    let effects = 0;
    const job = defineJob({ deps: [], metadata: { idempotencyKey: () => "once" }, handler() { return ++effects; } });
    jobs = createJobSystem({ jobs: { job }, backend: strategy });
    const handle = await job(null);
    assert.equal(await handle.result(), 1);
    const before = await clock.client.workflow.getHandle(handle.id).describe();
    await clock.sleep("31 days");
    const after = await clock.client.workflow.getHandle(handle.id).describe();
    assert.notEqual(before.runId, after.runId);
    assert.equal(await job(null).result(), 1);
    assert.equal(effects, 1);
    assert.equal((await jobs.get(handle.id)).status, "succeeded");
  } finally {
    await jobs?.close();
    await clock.teardown();
  }
});

test("Temporal workflow cancellation reaches the live handler and waits for cleanup before closing the execution", { timeout: 30_000 }, async (t) => {
  const entered = Promise.withResolvers();
  const aborted = Promise.withResolvers();
  const cleanup = Promise.withResolvers();
  const job = defineJob({ deps: [], metadata: { timeout: Infinity }, async handler(_, signal) {
    entered.resolve();
    await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    aborted.resolve();
    await cleanup.promise;
    signal.throwIfAborted();
  } });
  const jobs = createJobSystem({ jobs: { job }, backend: backend(crypto.randomUUID()) });
  t.after(async () => { cleanup.resolve(); await jobs.close(); });
  const handle = await job(null);
  await entered.promise;
  const [workflowId, runId] = handle.id.split("/");
  const execution = env.client.workflow.getHandle(workflowId, runId);
  const result = assert.rejects(handle.result(), /cancel/i);
  await execution.cancel();
  await aborted.promise;
  assert.equal((await execution.describe()).status.name, "RUNNING");
  cleanup.resolve();
  await result;
  const record = await jobs.get(handle.id);
  assert.equal(record.status, "failed");
  assert.equal(record.attempts, 1);
});

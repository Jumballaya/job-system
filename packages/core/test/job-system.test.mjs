import assert from "node:assert/strict";
import { test } from "node:test";
import { Container, createJobSystem, defineJob, JobExecutionError, JsonCodec, MemoryBackend, NonRetryableError } from "../dist/index.js";

test("calling a definition starts processing and retains one job ID across hooks", async (t) => {
  const contexts = [];
  const add = defineJob({
    name: "add", deps: [],
    handler: (input) => input + 1,
    beforeRun: (_input, context) => contexts.push(["before", context.jobId]),
    onSuccess: (_output, context) => contexts.push(["success", context.jobId]),
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [add] });
  t.after(() => jobs.close());
  const handle = await add(4);
  assert.equal(await handle.result(), 5);
  assert.equal(contexts[0][1], handle.id);
  assert.deepEqual(contexts, [["before", handle.id], ["success", handle.id]]);
  assert.equal(add.name, "add");
});

test("a submission's result can be awaited directly, repeatedly, and after an aborted wait", async (t) => {
  const echo = defineJob({ name: "echo", deps: [], handler: (input) => input });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [echo] });
  t.after(() => jobs.close());
  assert.equal(await echo(1).result(), 1);
  const handle = await echo(2);
  await assert.rejects(handle.result({ signal: AbortSignal.abort(new Error("canceled")) }), /canceled/);
  assert.equal(await handle.result(), 2);
  assert.equal(await handle.result(), 2);
});

test("definitions are callable only while attached to one open system", async () => {
  const echo = defineJob({ name: "echo", deps: [], handler: (input) => input });
  await assert.rejects(echo(1), /not attached/);
  const first = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [echo] });
  assert.throws(
    () => createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [echo] }),
    /already attached/,
  );
  assert.equal(await echo(1).result(), 1);
  await first.close();
  await assert.rejects(echo(1), /not attached/);
  const second = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [echo] });
  try {
    assert.equal(await echo(2).result(), 2);
  } finally {
    await second.close();
  }
});

test("a rejected catalog attaches nothing", async () => {
  const good = defineJob({ name: "good", deps: [], handler: (input) => input });
  const bad = defineJob({ name: "good", deps: [], handler: (input) => input });
  assert.throws(
    () => createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [good, bad] }),
    /duplicate/i,
  );
  await assert.rejects(good(1), /not attached/);
  assert.throws(
    () => createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [() => {}] }),
    /defineJob/,
  );
});

test("overlapping handlers share singletons while keeping scoped dependencies isolated", { timeout: 5_000 }, async (t) => {
  class Shared {}
  class Scope {}
  const container = new Container().register(Shared).register(Scope, { lifetime: "scoped", useFactory: () => new Scope() });
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const seen = [];
  const inspect = defineJob({
    name: "inspect", deps: [Shared, Scope, Scope],
    async handler(input, shared, firstScope, secondScope) {
      seen.push({ shared, firstScope, secondScope });
      if (seen.length === 2) entered.resolve();
      await release.promise;
      return input;
    },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container, jobs: [inspect], concurrency: 2 });
  t.after(async () => { release.resolve(); await jobs.close(); });
  const first = inspect(1).result();
  const second = inspect(2).result();
  await entered.promise;
  assert.equal(seen[0].shared, seen[1].shared);
  assert.equal(seen[0].firstScope, seen[0].secondScope);
  assert.notEqual(seen[0].firstScope, seen[1].firstScope);
  release.resolve();
  assert.deepEqual(await Promise.all([first, second]), [1, 2]);
});

test("handler failures are retained outcomes and invoke the error hook once", async (t) => {
  let calls = 0;
  const hookErrors = [];
  const fail = defineJob({
    name: "fail", deps: [], metadata: { retries: { attempts: 1 } },
    handler() { calls++; throw new TypeError("bad input"); },
    onError(error, context) { hookErrors.push([error.message, context.jobId]); },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [fail] });
  t.after(() => jobs.close());
  const handle = await fail(null);
  await assert.rejects(handle.result(), (error) =>
    error instanceof JobExecutionError && error.jobId === handle.id
    && error.failure.name === "TypeError" && error.message === "bad input");
  assert.equal(calls, 1);
  assert.deepEqual(hookErrors, [["bad input", handle.id]]);
});

test("success hook failure does not rerun work or invoke the error hook", async (t) => {
  let executed = 0;
  let errors = 0;
  const notify = defineJob({
    name: "notify", deps: [],
    handler() { executed++; return "done"; },
    onSuccess() { throw new Error("notification failed"); },
    onError() { errors++; },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [notify] });
  t.after(() => jobs.close());
  await assert.rejects(notify(null).result(), /notification failed/);
  assert.equal(executed, 1);
  assert.equal(errors, 0);
});

test("before and error hook failures become terminal results without running the handler", async (t) => {
  let executed = false;
  const hooks = defineJob({
    name: "hooks", deps: [], metadata: { retries: { attempts: 1 } },
    handler() { executed = true; },
    beforeRun() { throw new Error("before failed"); },
    onError() { throw new Error("error hook failed"); },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [hooks] });
  t.after(() => jobs.close());
  await assert.rejects(hooks(null).result(), (error) => error instanceof JobExecutionError && error.failure.name === "AggregateError");
  assert.equal(executed, false);
});

test("cancelling a caller wait does not cancel its accepted handler", { timeout: 5_000 }, async (t) => {
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  let handlerSignal;
  let completed = false;
  const wait = defineJob({
    name: "wait", deps: [],
    async handler(_input, signal) {
      handlerSignal = signal;
      entered.resolve();
      await release.promise;
      completed = true;
      return 7;
    },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [wait] });
  t.after(async () => { release.resolve(); await jobs.close(); });
  const controller = new AbortController();
  const waiting = assert.rejects(wait(null).result({ signal: controller.signal }), /no longer waiting/);
  await entered.promise;
  controller.abort(new Error("no longer waiting"));
  await waiting;
  assert.equal(handlerSignal.aborted, false);
  release.resolve();
  await jobs.close();
  assert.equal(completed, true);
});

test("unknown jobs and malformed payloads settle as failures without stopping the worker", async (t) => {
  const backend = new MemoryBackend();
  const codec = new JsonCodec();
  const good = defineJob({ name: "good", deps: [], handler: (input) => input });
  const jobs = createJobSystem({ container: new Container(), jobs: [good], backend });
  t.after(() => jobs.close());
  const policy = { attempts: 1, backoff: { type: "fixed", delay: 0 } };
  await backend.submit({ id: "unknown", name: "missing", input: codec.encode(null), policy });
  await backend.submit({ id: "malformed", name: "good", input: "invalid payload", policy });
  assert.equal(await good(3).result(), 3);
  assert.equal((await backend.result("unknown")).status, "failed");
  assert.equal((await backend.result("malformed")).status, "failed");
  assert.equal(await good(3).result(), 3);
});

test("unencodable outputs fail terminally and do not poison later executions", async (t) => {
  const output = defineJob({ name: "output", deps: [], handler: (bad) => bad ? 1n : 5 });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [output] });
  t.after(() => jobs.close());
  await assert.rejects(output(true).result(), JobExecutionError);
  assert.equal(await output(false).result(), 5);
});

test("closing during worker startup drains the late worker and closes the backend once", { timeout: 5_000 }, async () => {
  const starting = Promise.withResolvers();
  const ready = Promise.withResolvers();
  const done = Promise.withResolvers();
  let workerClosed = 0;
  let backendClosed = 0;
  const backend = {
    async submit() {},
    async result() { throw new Error("unused"); },
    async work() {
      starting.resolve();
      await ready.promise;
      return { done: done.promise, async close() { workerClosed++; done.resolve(); } };
    },
    async close() { backendClosed++; },
  };
  const job = defineJob({ name: "test", deps: [], handler() {} });
  const jobs = createJobSystem({ container: new Container(), jobs: [job], backend });
  const startupOutcome = assert.rejects(job(null), /closed/);
  await starting.promise;
  const closing = jobs.close();
  ready.resolve();
  await closing;
  await startupOutcome;
  await jobs.close();
  assert.equal(workerClosed, 1);
  assert.equal(backendClosed, 1);
  await assert.rejects(job(null), /not attached/);
});

test("shutdown releases the backend even when the worker fails and preserves both errors", async () => {
  const workerError = new Error("worker shutdown failed");
  const backendError = new Error("backend shutdown failed");
  let backendClosed = 0;
  const backend = {
    async submit() {},
    async result() { throw new Error("unused"); },
    async work() {
      return { done: new Promise(() => {}), async close() { throw workerError; } };
    },
    async close() { backendClosed++; throw backendError; },
  };
  const job = defineJob({ name: "test", deps: [], handler() {} });
  const jobs = createJobSystem({ container: new Container(), jobs: [job], backend });
  await assert.rejects(job(null).result(), /unused/);
  const closing = jobs.close();
  assert.equal(jobs.close(), closing);
  await assert.rejects(closing, (error) => {
    assert.ok(error instanceof AggregateError);
    assert.deepEqual(error.errors, [workerError, backendError]);
    return true;
  });
  assert.equal(backendClosed, 1);
});

test("system closure rejects pending result waits before draining active handlers", { timeout: 5_000 }, async (t) => {
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const drain = defineJob({
    name: "drain", deps: [],
    async handler() { entered.resolve(); await release.promise; return "finished"; },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [drain] });
  t.after(async () => { release.resolve(); await jobs.close(); });
  const waiting = assert.rejects(drain(null).result(), /clos/i);
  await entered.promise;
  let drained = false;
  const closing = jobs.close().then(() => { drained = true; });
  await waiting;
  assert.equal(drained, false);
  release.resolve();
  await closing;
});

test("simultaneous first submissions share startup and snapshot inputs before waiting for readiness", async (t) => {
  const ready = Promise.withResolvers();
  let starts = 0;
  let submissions = 0;
  class Backend extends MemoryBackend {
    async work(execute, options) {
      starts++;
      await ready.promise;
      return super.work(execute, options);
    }
    async submit(message) { submissions++; return super.submit(message); }
  }
  const echo = defineJob({ name: "echo", deps: [], handler: (input) => input });
  const jobs = createJobSystem({ container: new Container(), jobs: [echo], backend: new Backend() });
  t.after(async () => { ready.resolve(); await jobs.close(); });
  const input = { value: 1 };
  const first = echo(input).result();
  input.value = 99;
  const second = echo({ value: 2 }).result();
  assert.equal(starts, 1);
  assert.equal(submissions, 0);
  ready.resolve();
  assert.deepEqual(await Promise.all([first, second]), [{ value: 1 }, { value: 2 }]);
  assert.equal(submissions, 2);
});

test("invalid inputs and unattached jobs do not start a worker or submit work", async (t) => {
  let starts = 0;
  class Backend extends MemoryBackend {
    async work(...args) { starts++; return super.work(...args); }
    async submit() { assert.fail("must not submit invalid work"); }
  }
  const echo = defineJob({ name: "echo", deps: [], handler: (input) => input });
  const stray = defineJob({ name: "stray", deps: [], handler: (input) => input });
  const jobs = createJobSystem({ container: new Container(), jobs: [echo], backend: new Backend() });
  t.after(() => jobs.close());
  await assert.rejects(echo(1n), TypeError);
  await assert.rejects(stray(1), /not attached/);
  await assert.rejects(stray(1).result(), /not attached/);
  assert.equal(starts, 0);
});

test("startup failures reject submissions without submitting and still release backend resources", async () => {
  const failure = new Error("cannot start worker");
  let starts = 0;
  let closed = 0;
  const backend = {
    async work() { starts++; throw failure; },
    async submit() { assert.fail("must not submit before readiness"); },
    async result() { assert.fail("must not wait for unsubmitted work"); },
    async close() { closed++; },
  };
  const job = defineJob({ name: "test", deps: [], handler() {} });
  const jobs = createJobSystem({ container: new Container(), jobs: [job], backend });
  await assert.rejects(job(null), (error) => error === failure);
  await assert.rejects(job(null).result(), (error) => error === failure);
  await assert.rejects(jobs.close(), (error) => error === failure);
  assert.equal(starts, 1);
  assert.equal(closed, 1);
});

test("worker failure rejects pending and future submissions instead of leaving waits stranded", async (t) => {
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const stopped = Promise.withResolvers();
  const failure = new Error("worker lost");
  class Backend extends MemoryBackend {
    async work(...args) {
      const worker = await super.work(...args);
      return { ...worker, done: stopped.promise };
    }
  }
  const wait = defineJob({ name: "wait", deps: [], async handler() { entered.resolve(); await release.promise; } });
  const jobs = createJobSystem({ container: new Container(), jobs: [wait], backend: new Backend() });
  t.after(async () => { release.resolve(); await jobs.close(); });
  const pending = assert.rejects(wait(null).result(), (error) => error === failure);
  await entered.promise;
  stopped.reject(failure);
  await pending;
  await assert.rejects(wait(null), (error) => error === failure);
});

test("retryable failures are redelivered with backoff until they succeed or exhaust attempts", async (t) => {
  const seen = [];
  let failures = 0;
  const flaky = defineJob({
    name: "flaky", deps: [], metadata: { retries: { attempts: 3, backoff: { type: "fixed", delay: 1 } } },
    handler(input) {
      if (failures < input.failures) { failures++; seen.push("fail"); throw new Error("transient"); }
      seen.push("ok");
      return failures;
    },
    beforeRun: (_input, context) => seen.push(`attempt ${context.attempt}/${context.maxAttempts}`),
    onError: (error, context) => seen.push(`error ${context.attempt}: ${error.message}`),
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [flaky] });
  t.after(() => jobs.close());
  assert.equal(await flaky({ failures: 2 }).result(), 2);
  assert.deepEqual(seen, [
    "attempt 1/3", "fail", "error 1: transient",
    "attempt 2/3", "fail", "error 2: transient",
    "attempt 3/3", "ok",
  ]);
  failures = 0;
  await assert.rejects(flaky({ failures: 5 }).result(), (error) => error instanceof JobExecutionError && error.message === "transient");
  assert.equal(failures, 3);
});

test("NonRetryableError, success-hook failures, and malformed input never retry", async (t) => {
  let calls = 0;
  const stop = defineJob({
    name: "stop", deps: [], metadata: { retries: { attempts: 5, backoff: { type: "fixed", delay: 0 } } },
    handler() { calls++; throw new NonRetryableError("give up"); },
  });
  const hook = defineJob({
    name: "hook", deps: [], metadata: { retries: { attempts: 5, backoff: { type: "fixed", delay: 0 } } },
    handler() { calls++; return 1; },
    onSuccess() { throw new Error("notify failed"); },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [stop, hook] });
  t.after(() => jobs.close());
  await assert.rejects(stop(null).result(), (error) => error.failure.name === "NonRetryableError");
  await assert.rejects(hook(null).result(), /notify failed/);
  assert.equal(calls, 2);
});

test("a timeout aborts the handler's signal, fails the attempt, and allows a retry", async (t) => {
  const outcomes = [];
  const slow = defineJob({
    name: "slow", deps: [], metadata: { timeout: 20, retries: { attempts: 2, backoff: { type: "fixed", delay: 0 } } },
    async handler(input, signal) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, input.ms);
        signal.addEventListener("abort", () => { clearTimeout(timer); reject(signal.reason); }, { once: true });
      });
      return "finished";
    },
    onError: (error, context) => outcomes.push([context.attempt, error.name, context.signal.aborted]),
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [slow] });
  t.after(() => jobs.close());
  await assert.rejects(slow({ ms: 500 }).result(), (error) => error.failure.name === "TimeoutError");
  assert.deepEqual(outcomes, [[1, "TimeoutError", true], [2, "TimeoutError", true]]);
  assert.equal(await slow({ ms: 1 }).result(), "finished");
});

test("per-job concurrency bounds simultaneous executions within the system's limit", { timeout: 5_000 }, async (t) => {
  let active = 0;
  let peak = 0;
  const limited = defineJob({
    name: "limited", deps: [], metadata: { concurrency: 2 },
    async handler(input) {
      active++;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 10));
      active--;
      return input;
    },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [limited], concurrency: 4 });
  t.after(() => jobs.close());
  assert.deepEqual(await Promise.all([1, 2, 3, 4, 5].map((n) => limited(n).result())), [1, 2, 3, 4, 5]);
  assert.equal(peak, 2);
});

test("a key derived from the input reuses the active job and frees the key on completion", async (t) => {
  const release = Promise.withResolvers();
  let runs = 0;
  const sync = defineJob({
    name: "sync", deps: [], metadata: { key: (input) => `user:${input.userId}` },
    async handler(input) { runs++; await release.promise; return input.userId; },
  });
  const jobs = createJobSystem({ backend: new MemoryBackend(), container: new Container(), jobs: [sync] });
  t.after(async () => { release.resolve(); await jobs.close(); });
  const first = await sync({ userId: 7 });
  const duplicate = await sync({ userId: 7 });
  const other = await sync({ userId: 8 });
  assert.equal(duplicate.id, first.id);
  assert.notEqual(other.id, first.id);
  release.resolve();
  assert.equal(await duplicate.result(), 7);
  const later = await sync({ userId: 7 });
  assert.notEqual(later.id, first.id);
  assert.equal(await later.result(), 7);
  assert.equal(runs, 3);
});

test("invalid metadata is rejected when the job is defined", () => {
  const bad = (metadata) => () => defineJob({ name: "bad", deps: [], metadata, handler() {} });
  assert.throws(bad({ retries: { attempts: 0 } }), /retries.attempts/);
  assert.throws(bad({ retries: { backoff: "sometimes" } }), /backoff.type/);
  assert.throws(bad({ timeout: -1 }), /timeout/);
  assert.throws(bad({ concurrency: 1.5 }), /concurrency/);
  assert.throws(bad({ key: "static" }), /key/);
});

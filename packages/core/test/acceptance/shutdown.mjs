import assert from "node:assert/strict";
import { createJobSystem, defineJob, JsonCodec, MemoryBackend, ShutdownTimeoutError } from "../../dist/index.js";

const policy = { timeout: Infinity, retries: { attempts: 1 } };

// Exercise deadline expiry during handler cleanup, a success hook, and delayed startup.
for (const phase of ["handler", "success", "startup"]) {
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const closed = Promise.withResolvers();
  const outcomes = [];
  const effects = [];
  let executions = 0;
  let signal;
  class Backend extends MemoryBackend {
    async work(execute, options) {
      if (phase === "startup") { entered.resolve(); await release.promise; }
      return super.work(async (...args) => {
        const outcome = await execute(...args);
        outcomes.push(outcome);
        return outcome;
      }, options);
    }
    async close() { await super.close(); closed.resolve(); }
  }
  const backend = new Backend();
  const job = defineJob({
    deps: [], metadata: policy,
    async handler(input, executionSignal) {
      executions++;
      signal = executionSignal;
      if (phase === "handler") {
        entered.resolve();
        try {
          await new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }));
        } finally { await release.promise; }
      }
      effects.push(input);
      return input;
    },
    async onSuccess() {
      if (phase === "success") { entered.resolve(); await release.promise; }
    },
    onError() { assert.fail("shutdown cancellation became a business failure"); },
  });
  await backend.submit({ id: phase, name: "job", input: new JsonCodec().encode("effect"), policy: { attempts: 1, backoff: { type: "fixed", delay: 0 } } });
  const jobs = createJobSystem({ jobs: { job }, backend, shutdownTimeoutMs: 25 });
  try {
    await entered.promise;
    const closing = jobs.close();
    assert.equal(jobs.close(), closing);
    await assert.rejects(closing, ShutdownTimeoutError);
    if (signal) assert.ok(signal.aborted);
    await assert.rejects(job("late"), /not attached/);
    let disposed = false;
    void closed.promise.then(() => { disposed = true; });
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(disposed, false, "live execution or delayed startup lost its owned backend");
    release.resolve();
    await closed.promise;
    assert.equal(executions, phase === "startup" ? 0 : 1);
    assert.deepEqual(effects, phase === "success" ? ["effect"] : []);
    if (phase === "success") {
      assert.equal(outcomes.length, 1);
      assert.equal(outcomes[0].status, "failed");
      assert.equal(outcomes[0].retryable, false, "shutdown during a success hook must not repeat committed effects");
    } else {
      assert.deepEqual(outcomes, [], "interrupted work acquired a terminal result");
    }
    await assert.rejects(jobs.close(), ShutdownTimeoutError);
  } finally {
    release.resolve();
    await jobs.close().catch(() => {});
    await backend.close();
  }
}

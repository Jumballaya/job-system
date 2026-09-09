import assert from "node:assert/strict";
import { createJobSystem, defineJob, JsonCodec } from "../../dist/index.js";
import { eventually, pause } from "./helpers.mjs";

// Provider-level failures bypass the handler; public inspection must still describe their retained records.
export async function inspectBackend(backend, infrastructureAttempts) {
  const codec = new JsonCodec();
  const entered = Promise.withResolvers();
  const release = Promise.withResolvers();
  const echo = defineJob({ deps: [], metadata: { retries: { attempts: 2, backoff: { type: "fixed", delay: 20 } } }, handler: (input) => input });
  const jobs = createJobSystem({ jobs: { echo }, backend, worker: false });
  try {
    const held = await echo({ kind: "held", value: 42 });
    const queued = await jobs.get(held.id);
    assert.equal(queued.status, "queued");
    assert.equal(queued.attempts, 0);
    queued.input.value = 99;
    assert.equal((await jobs.get(held.id)).input.value, 42, "inspection mutated stored input");
    await backend.work(async (message) => {
      const input = codec.decode(message.input);
      if (input.kind === "held") { entered.resolve(); await release.promise; }
      if (input.kind === "infrastructure") throw new Error("transport failed");
      if (input.kind === "application") return { status: "failed", retryable: false, error: { name: "DomainError", message: "rejected" } };
      return { status: "succeeded", output: codec.encode({ value: input.value }) };
    });
    await entered.promise;
    assert.equal((await jobs.get(held.id)).status, "running");
    release.resolve();
    const finished = await eventually(() => jobs.get(held.id), (record) => {
      if (record.status === "running") assert.equal("output" in record, false);
      return record.status === "succeeded";
    }, "execution never produced a complete snapshot");
    assert.ok(finished.finishedAt >= finished.startedAt && finished.startedAt >= finished.createdAt);
    assert.deepEqual(finished.output, { value: 42 });
    finished.output.value = 99;
    assert.equal((await jobs.get(held.id)).output.value, 42, "inspection mutated stored output");

    const application = await echo({ kind: "application" });
    await assert.rejects(application.result(), /rejected/);
    const infrastructure = await echo({ kind: "infrastructure" });
    await assert.rejects(infrastructure.result({ signal: AbortSignal.timeout(5_000) }), /transport failed/);
    const failed = await jobs.get(infrastructure.id);
    assert.equal(failed.status, "failed");
    assert.equal(failed.attempts, infrastructureAttempts);
    assert.equal(failed.error.message, "transport failed");
    assert.ok(failed.error.stack.includes("transport failed"));
    await pause(1_200);
    assert.equal(await jobs.get(held.id), null);
    assert.equal((await jobs.get(application.id)).error.name, "DomainError");
    assert.equal((await jobs.get(infrastructure.id)).error.message, "transport failed");
    await pause(1_000);
    assert.equal(await jobs.get(application.id), null, "application failures never expired");
    assert.equal(await jobs.get(infrastructure.id), null, "infrastructure failures never expired");
  } finally {
    release.resolve();
    await jobs.close();
  }
}

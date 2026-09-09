import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { Queue } from "bullmq";
import { Container, createJobSystem, defineJob, IdempotencyConflictError, MemoryBackend, RedisBackend } from "../dist/index.js";

const port = Number(process.env.JOB_SYSTEM_REDIS_PORT);
const once = { attempts: 1, backoff: { type: "fixed", delay: 0 } };
const success = (output) => ({ status: "succeeded", output });
const message = (input = "original") => ({
  id: crypto.randomUUID(), name: "charge", input, policy: { ...once, idempotencyKey: "tenant/payment-1" },
});

function fixture(t, provider) {
  const backends = [];
  const queue = `idempotency-test-${crypto.randomUUID()}`;
  const connection = { host: "127.0.0.1", port };
  const inspector = provider === "redis" ? new Queue(queue, { connection }) : undefined;
  const memory = provider === "memory" ? new MemoryBackend({ resultTTLms: 1_000 }) : undefined;
  if (memory) backends.push(memory);
  t.after(async () => {
    await Promise.all(backends.map((backend) => backend.close()));
    if (inspector) {
      try { await inspector.obliterate({ force: true }); }
      finally { await inspector.close(); }
    }
  });
  return {
    inspector,
    backend() {
      if (memory) return memory;
      const backend = new RedisBackend({ queue, connection, resultTTLSeconds: 1 });
      backends.push(backend);
      return backend;
    },
  };
}

for (const provider of ["memory", "redis"]) {
  const options = { skip: provider === "redis" && !port, timeout: 20_000 };

  test(`${provider}: competing submissions reserve one immutable operation before any worker starts`, options, async (t) => {
    const env = fixture(t, provider);
    const first = env.backend();
    const second = env.backend();
    const proposals = Array.from({ length: 12 }, (_, i) => message(i % 2 ? "one" : "two"));
    const accepted = await Promise.allSettled(proposals.map((proposal, i) => (i % 2 ? first : second).submit(proposal)));
    const ids = accepted.filter((result) => result.status === "fulfilled").map((result) => result.value);
    const conflicts = accepted.filter((result) => result.status === "rejected");
    assert.equal(ids.length, 6);
    assert.equal(new Set(ids).size, 1);
    assert.equal(conflicts.length, 6);
    for (const conflict of conflicts) assert.ok(conflict.reason instanceof IdempotencyConflictError);
    const winningInput = proposals[accepted.findIndex((result) => result.status === "fulfilled")].input;
    let calls = 0;
    await second.work(async (job) => { calls++; assert.equal(job.id, ids[0]); return success(job.input); });
    assert.deepEqual(await first.result(ids[0]), success(winningInput));
    assert.equal(await first.submit(message(winningInput)), ids[0]);
    await assert.rejects(second.submit(message("changed after completion")), IdempotencyConflictError);
    assert.equal(calls, 1);
  });

  test(`${provider}: success, terminal application failure, and infrastructure failure survive ordinary result cleanup`, options, async (t) => {
    const env = fixture(t, provider);
    let backend = env.backend();
    let calls = 0;
    const worker = await backend.work(async (job) => {
      calls++;
      if (job.input === "infrastructure") throw new Error("connection lost");
      if (job.input === "failure") return { status: "failed", error: { name: "Declined", message: "declined" }, retryable: false };
      return success(job.input);
    });
    const submissions = ["success", "failure", "infrastructure"].map((input) => ({
      ...message(input), policy: { ...once, idempotencyKey: input },
    }));
    const ids = [];
    for (const proposal of submissions) {
      const id = await backend.submit(proposal);
      ids.push(id);
      if (proposal.input === "infrastructure") await assert.rejects(backend.result(id), /connection lost/);
      else await backend.result(id);
    }
    await worker.close().catch(() => {});
    if (provider === "redis") {
      await backend.close();
      backend = env.backend();
    }
    await backend.work(async () => { calls++; return success("ordinary"); });
    const ordinary = { id: "ordinary", name: "charge", input: "null", policy: once };
    await backend.submit(ordinary);
    await backend.result(ordinary.id);
    await delay(1_100);
    await backend.submit({ ...ordinary, id: "cleanup-trigger" });
    await backend.result("cleanup-trigger");
    await assert.rejects(backend.result(ordinary.id), { name: "ResultUnavailableError" });
    if (env.inspector) assert.equal(await env.inspector.getJob("job-ordinary"), undefined, "ordinary records physically removed");
    const beforeReplay = calls;
    for (let i = 0; i < submissions.length; i++) {
      assert.equal(await backend.submit({ ...submissions[i], id: crypto.randomUUID() }), ids[i]);
      if (i === 2) await assert.rejects(backend.result(ids[i]), /connection lost/);
      else assert.equal((await backend.result(ids[i])).status, i === 0 ? "succeeded" : "failed");
    }
    assert.equal(calls, beforeReplay, "neither success nor failure restarts an exhausted operation");
  });

  test(`${provider}: callable jobs replay canonical payloads and scope operations by job and tenant`, options, async (t) => {
    const env = fixture(t, provider);
    const deliveries = [];
    const definition = () => defineJob({
      deps: [],
      handler(input) { return input.details.amount; },
      metadata: { idempotencyKey: (input) => JSON.stringify([input.tenant, input.operation]) },
      beforeRun(_input, context) { deliveries.push(context.jobId); },
    });
    const charge = definition();
    const refund = definition();
    const jobs = createJobSystem({ container: new Container(), jobs: { charge, refund }, backend: env.backend() });
    t.after(() => jobs.close());
    const original = { tenant: "a", operation: "one", details: { amount: 10, currency: "USD" } };
    const handle = await charge(original);
    assert.equal(await handle.result(), 10);
    const duplicate = await charge({ details: { currency: "USD", amount: 10 }, operation: "one", tenant: "a" });
    assert.equal(duplicate.id, handle.id);
    assert.equal(await duplicate.result(), 10);
    await assert.rejects(charge({ ...original, details: { ...original.details, amount: 99 } }), IdempotencyConflictError);
    const otherTenant = await charge({ ...original, tenant: "b" });
    const otherJob = await refund(original);
    await Promise.all([otherTenant.result(), otherJob.result()]);
    assert.equal(new Set([handle.id, otherTenant.id, otherJob.id]).size, 3);
    assert.equal(deliveries.length, 3);
  });
}

test("memory idempotency records survive the ordinary result-count limit", async (t) => {
  const backend = new MemoryBackend();
  t.after(() => backend.close());
  let calls = 0;
  await backend.work(async () => success(String(++calls)));
  const original = message();
  const id = await backend.submit(original);
  assert.deepEqual(await backend.result(id), success("1"));
  for (let i = 0; i < 1_005; i++) {
    const ordinary = { ...message(), id: String(i), policy: once };
    await backend.submit(ordinary);
    await backend.result(ordinary.id);
  }
  assert.equal(await backend.submit(message()), id);
  assert.deepEqual(await backend.result(id), success("1"));
  assert.equal(calls, 1_006);
});

test("invalid operation keys cannot silently submit unprotected work", async (t) => {
  for (const idempotencyKey of [null, "fixed"]) {
    assert.throws(() => defineJob({ deps: [], handler() {}, metadata: { idempotencyKey } }), /idempotencyKey/);
  }
  assert.throws(() => defineJob({ deps: [], handler() {}, metadata: { key: () => "a", idempotencyKey: () => "b" } }), /not both/);
  for (const invalid of [undefined, null, "", 1]) {
    let calls = 0;
    const job = defineJob({ deps: [], handler() { calls++; }, metadata: { idempotencyKey: () => invalid } });
    const jobs = createJobSystem({ container: new Container(), jobs: { job }, backend: new MemoryBackend() });
    t.after(() => jobs.close());
    await assert.rejects(job(null), /idempotencyKey must be a nonempty string/);
    assert.equal(calls, 0);
  }
});

test("Redis cleanup advances past permanent records and removes ordinary results across batches", { skip: !port, timeout: 20_000 }, async (t) => {
  const env = fixture(t, "redis");
  const backend = env.backend();
  await backend.work(async () => success("done"));
  const retained = [];
  for (let i = 0; i < 105; i++) {
    const id = await backend.submit({ ...message(), policy: { ...once, idempotencyKey: `keep-${i}` } });
    retained.push(id);
    await backend.result(id);
  }
  for (let i = 0; i < 105; i++) {
    const id = await backend.submit({ ...message(), id: `expire-${i}`, policy: once });
    await backend.result(id);
  }
  await delay(1_100);
  for (let i = 0; i < 6; i++) {
    const id = await backend.submit({ ...message(), id: `cleanup-${i}`, policy: once });
    await backend.result(id);
  }
  const remaining = await env.inspector.getCompleted(0, -1);
  assert.equal(remaining.some((job) => job.id.startsWith("job-expire-")), false);
  const ids = new Set(remaining.map((job) => job.id));
  for (const id of retained) assert.ok(ids.has(`job-${id}`), "permanent operation must survive every cleanup batch");
});

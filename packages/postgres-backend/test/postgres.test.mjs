import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import EmbeddedPostgres from "embedded-postgres";
import { Pool } from "pg";
import { Container, createJobSystem, defineJob, IdempotencyConflictError, NonRetryableError, ShutdownTimeoutError } from "core";
import { PostgresBackend } from "../dist/index.js";

let pool, postgres, directory, connectionString;
before(async () => {
  connectionString = process.env.JOB_SYSTEM_POSTGRES_URL;
  if (!connectionString) {
    directory = await mkdtemp(join(tmpdir(), "job-system-postgres-"));
    const socket = createServer().listen(0, "127.0.0.1");
    await once(socket, "listening");
    const port = socket.address().port;
    await new Promise((resolve) => socket.close(resolve));
    postgres = new EmbeddedPostgres({ databaseDir: directory, port, user: "postgres", password: "postgres", persistent: false });
    await postgres.initialise();
    await postgres.start();
    connectionString = `postgres://postgres:postgres@127.0.0.1:${port}/postgres`;
  }
  pool = new Pool({ connectionString, max: 15, connectionTimeoutMillis: 2_000 });
  const setup = new PostgresBackend({ pool, queue: "migrations" });
  await Promise.all([setup.migrate(), setup.migrate(), setup.migrate()]);
  await setup.close();
  await pool.query("CREATE TABLE IF NOT EXISTS job_system.test_effects (queue text, id text, PRIMARY KEY(queue, id))");
}, { timeout: 30_000 });
after(async () => {
  await pool?.end();
  await postgres?.stop();
  if (directory) await rm(directory, { recursive: true, force: true });
});

const single = { attempts: 1, backoff: { type: "fixed", delay: 0 } };
const success = (output) => ({ status: "succeeded", output });
const proposal = (id, policy = single, name = "job") => ({ id, name, input: id, policy });
async function eventually(read, accepts, description = "expected database state", timeout = 5_000) {
  const end = Date.now() + timeout;
  let value;
  do {
    value = await read();
    if (accepts(value)) return value;
    await delay(15);
  } while (Date.now() < end);
  assert.fail(`${description}: ${JSON.stringify(value)}`);
}
function fixture(t, settings = {}) {
  const queue = `test-${crypto.randomUUID()}`;
  const backends = [];
  t.after(async () => {
    await Promise.all(backends.map((backend) => backend.close()));
    await pool.query("DELETE FROM job_system.schedules WHERE queue = $1", [queue]);
    await pool.query("DELETE FROM job_system.jobs WHERE queue = $1", [queue]);
    await pool.query("DELETE FROM job_system.test_effects WHERE queue = $1", [queue]);
  });
  return {
    queue,
    backend(overrides = {}) {
      const backend = new PostgresBackend({ pool, queue, pollIntervalMs: 20, leaseDurationMs: 600, ...settings, ...overrides });
      backends.push(backend);
      return backend;
    },
  };
}

test("migrations serialize and backend closure leaves the application's pool usable", async (t) => {
  const backend = fixture(t).backend();
  await backend.migrate();
  assert.equal((await pool.query("SELECT count(*)::int AS count FROM job_system.migrations")).rows[0].count, 1);
  await backend.close();
  assert.equal((await pool.query("SELECT 42 AS value")).rows[0].value, 42);
  await assert.rejects(backend.submit(proposal("late")), /closed/);
});

test("an exited producer's delayed work reaches a fresh worker with DI and cancelable result waits", { timeout: 10_000 }, async (t) => {
  const env = fixture(t);
  const definition = defineJob({ deps: [], handler() { assert.fail("producer must not execute"); } });
  const producer = createJobSystem({ jobs: { double: definition }, backend: env.backend(), worker: false });
  const due = Date.now() + 300;
  const handle = await definition(21, { at: due });
  assert.equal((await producer.get(handle.id)).status, "queued");
  const controller = new AbortController();
  const canceled = assert.rejects(handle.result({ signal: controller.signal }), /caller left/);
  controller.abort(new Error("caller left"));
  await canceled;
  await producer.close();
  class Multiplier { double(input) { return input * 2; } }
  const double = defineJob({ deps: [Multiplier], handler(input, multiplier) {
    assert.ok(Date.now() >= due); return multiplier.double(input);
  } });
  const jobs = createJobSystem({ container: new Container().register(Multiplier), jobs: { double }, backend: env.backend() });
  t.after(() => jobs.close());
  const completed = await eventually(() => jobs.get(handle.id), (row) => row?.status === "succeeded");
  assert.equal(completed.output, 42);
  assert.equal(completed.attempts, 1);
});

test("business retries respect backoff and permanent errors terminate immediately", { timeout: 10_000 }, async (t) => {
  const attempts = [], started = [];
  const flaky = defineJob({ deps: [], handler() {
    started.push(Date.now());
    if (started.length < 3) throw new Error("temporary");
    return "ok";
  }, beforeRun(_, context) { attempts.push(context.attempt); },
  metadata: { retries: { attempts: 3, backoff: { type: "fixed", delay: 70 } } } });
  let rejected = 0;
  const permanent = defineJob({ deps: [], handler() { rejected++; throw new NonRetryableError("invalid"); } });
  const jobs = createJobSystem({ jobs: { flaky, permanent }, backend: fixture(t).backend() });
  t.after(() => jobs.close());
  assert.equal(await flaky(null).result(), "ok");
  assert.deepEqual(attempts, [1, 2, 3]);
  assert.ok(started[1] - started[0] >= 70 && started[2] - started[1] >= 70);
  await assert.rejects(permanent(null).result(), /invalid/);
  assert.equal(rejected, 1);
});

test("competing producers deduplicate active work and preserve immutable operations after completion", async (t) => {
  const env = fixture(t), first = env.backend(), second = env.backend();
  const proposals = Array.from({ length: 12 }, (_, i) => ({ ...proposal(`id-${i}`, { ...single, idempotencyKey: "operation" }), input: i % 2 ? "a" : "b" }));
  const settled = await Promise.allSettled(proposals.map((message, i) => (i % 2 ? first : second).submit(message)));
  const accepted = settled.filter((result) => result.status === "fulfilled");
  assert.equal(accepted.length, 6);
  assert.equal(new Set(accepted.map((result) => result.value)).size, 1);
  for (const result of settled.filter((result) => result.status === "rejected")) assert.ok(result.reason instanceof IdempotencyConflictError);
  const active = await Promise.all(Array.from({ length: 12 }, (_, i) => first.submit(proposal(`active-${i}`, { ...single, key: "active" }))));
  assert.equal(new Set(active).size, 1);
  const otherName = await first.submit(proposal("different-name", { ...single, key: "active" }, "other"));
  assert.notEqual(otherName, active[0]);
  let calls = 0;
  await second.work(async (message) => { calls++; return success(message.input); });
  const id = accepted[0].value;
  const outcome = await first.result(id);
  assert.equal(await first.submit({ ...proposals[0], input: outcome.output }), id);
  await assert.rejects(first.submit({ ...proposals[0], input: "changed" }), IdempotencyConflictError);
  await first.result(active[0]);
  const another = await first.submit(proposal("new-active", { ...single, key: "active" }));
  assert.notEqual(another, active[0]);
  assert.ok(calls >= 1);
});

test("workers claim disjoint jobs and saturated job types leave capacity for other work", { timeout: 10_000 }, async (t) => {
  const env = fixture(t), producer = env.backend();
  const release = Promise.withResolvers();
  const counts = [0, 0], totals = [0, 0], seen = new Set();
  for (let index = 0; index < 10; index++) await producer.submit(proposal(`slow-${index}`, single, "slow"));
  for (let index = 0; index < 2; index++) {
    await env.backend().work(async (message) => {
      assert.ok(!seen.has(message.id)); seen.add(message.id);
      totals[index]++; assert.ok(totals[index] <= 2);
      if (message.name === "slow") { counts[index]++; assert.equal(counts[index], 1); await release.promise; counts[index]--; }
      totals[index]--;
      return success(message.id);
    }, { concurrency: 2, concurrencyByJob: { slow: 1 } });
  }
  t.after(() => release.resolve());
  await producer.submit(proposal("fast", single, "fast"));
  assert.deepEqual(await producer.result("fast"), success("fast"));
  release.resolve();
  await Promise.all(Array.from({ length: 10 }, (_, i) => producer.result(`slow-${i}`)));
  assert.equal(seen.size, 11);
});

test("stored schedules survive producer exit, overlap independently, and can be edited and removed by ID", { timeout: 10_000 }, async (t) => {
  const env = fixture(t), producer = env.backend();
  const message = proposal("seed", { ...single, key: "recurring" });
  const ids = await Promise.all(Array.from({ length: 8 }, () => producer.schedules.upsert(message, { every: 90 })));
  assert.equal(new Set(ids).size, 1);
  await producer.close();
  const worker = env.backend(), gate = Promise.withResolvers(), occurrences = [];
  t.after(() => gate.resolve());
  const session = await worker.work(async (message) => { occurrences.push(message.id); await gate.promise; return success(message.id); }, { concurrency: 3 });
  await eventually(() => occurrences.length, (count) => count >= 2);
  assert.equal(new Set(occurrences).size, occurrences.length);
  const manager = env.backend();
  await manager.schedules.update(ids[0], { cron: "0 9 * * 1-5", timezone: "America/New_York" });
  const schedule = (await pool.query("SELECT rule, next_at FROM job_system.schedules WHERE queue = $1", [env.queue])).rows[0];
  assert.equal(schedule.rule.timezone, "America/New_York");
  assert.ok(schedule.next_at.getTime() > Date.now());
  await manager.schedules.remove(ids[0]);
  await manager.schedules.remove(ids[0]);
  await assert.rejects(manager.schedules.update(ids[0], { every: 50 }), /not found/);
  gate.resolve();
  await Promise.all(occurrences.map((id) => manager.result(id)));
  await session.close();
  assert.equal((await pool.query("SELECT count(*)::int AS count FROM job_system.schedules WHERE queue = $1", [env.queue])).rows[0].count, 0);
});

test("a shutdown deadline keeps renewing ownership until handler cleanup settles", { timeout: 10_000 }, async (t) => {
  const env = fixture(t, { leaseDurationMs: 300 });
  const entered = Promise.withResolvers(), cleanup = Promise.withResolvers();
  const job = defineJob({ deps: [], async handler(_, signal) {
    entered.resolve();
    await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    await cleanup.promise;
    signal.throwIfAborted();
  } });
  const old = createJobSystem({ jobs: { job }, backend: env.backend(), shutdownTimeoutMs: 50 });
  const handle = await job(null);
  await entered.promise;
  await assert.rejects(old.close(), ShutdownTimeoutError);
  let calls = 0, attempt;
  const replacement = env.backend();
  await replacement.work(async (_, __, count) => { calls++; attempt = count; return success("recovered"); });
  await delay(800);
  assert.equal(calls, 0, "cleanup still owns the renewable lease");
  cleanup.resolve();
  assert.deepEqual(await replacement.result(handle.id), success("recovered"));
  assert.equal(attempt, 1, "shutdown must not spend an attempt");
});

test("a killed worker's committed effect survives and its unfinished job recovers without spending an attempt", { timeout: 10_000 }, async (t) => {
  const env = fixture(t), producer = env.backend();
  const child = fork(new URL("./worker.mjs", import.meta.url), [env.queue], {
    env: { ...process.env, JOB_SYSTEM_POSTGRES_URL: connectionString }, stdio: ["ignore", "ignore", "inherit", "ipc"],
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) { const exit = once(child, "exit"); child.kill("SIGKILL"); await exit; }
  });
  const committed = once(child, "message");
  await producer.submit(proposal("effect", { ...single, idempotencyKey: "effect" }));
  const [observed] = await committed;
  assert.equal(observed.attempt, 1);
  const exited = once(child, "exit"); child.kill("SIGKILL"); await exited;
  const replacement = env.backend();
  let recoveredAttempt;
  await replacement.work(async (message, _, attempt) => {
    recoveredAttempt = attempt;
    await pool.query("INSERT INTO job_system.test_effects (queue, id) VALUES ($1, $2) ON CONFLICT DO NOTHING", [env.queue, message.id]);
    return success("recovered");
  });
  assert.deepEqual(await producer.result(observed.id), success("recovered"));
  assert.equal(recoveredAttempt, 1);
  assert.equal((await pool.query("SELECT count(*)::int AS count FROM job_system.test_effects WHERE queue = $1", [env.queue])).rows[0].count, 1);
});

test("an expired owner cannot overwrite a replacement's terminal result", { timeout: 10_000 }, async (t) => {
  const env = fixture(t), old = env.backend(), producer = env.backend();
  const started = Promise.withResolvers(), release = Promise.withResolvers();
  let staleSignal;
  await old.work(async (_, signal) => { staleSignal = signal; started.resolve(); await release.promise; return success("stale"); });
  await producer.submit(proposal("fenced")); await started.promise;
  await pool.query("UPDATE job_system.jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE queue = $1 AND id = 'fenced'", [env.queue]);
  await env.backend().work(async () => success("replacement"));
  assert.deepEqual(await producer.result("fenced"), success("replacement"));
  await eventually(() => staleSignal.aborted, Boolean);
  release.resolve(); await old.close();
  assert.deepEqual(await producer.result("fenced"), success("replacement"));
});

test("retention is stored per execution, permanent operations survive cleanup, and queues are isolated", { timeout: 10_000 }, async (t) => {
  const env = fixture(t, { resultTTLSeconds: 0.1, failureTTLSeconds: 1 });
  const producer = env.backend(), worker = env.backend();
  await worker.work(async (message) => message.name === "fail"
    ? { status: "failed", retryable: false, error: { name: "Failure", message: "failed" } } : success(message.input));
  await producer.submit(proposal("short")); await producer.result("short");
  await producer.submit(proposal("failure", single, "fail")); await producer.result("failure");
  const permanent = await producer.submit(proposal("permanent", { ...single, idempotencyKey: "permanent" }));
  await producer.result(permanent);
  const indefinite = env.backend({ resultTTLSeconds: null });
  await indefinite.submit(proposal("indefinite")); await indefinite.result("indefinite");
  await delay(180);
  const reader = env.backend({ resultTTLSeconds: null });
  assert.equal(await reader.get("short"), null);
  await assert.rejects(reader.result("short"), /unknown or expired/);
  assert.equal((await reader.get("failure")).status, "failed");
  assert.deepEqual(await reader.result(permanent), success("permanent"));
  assert.deepEqual(await reader.result("indefinite"), success("indefinite"));
  assert.ok(await reader.prune() >= 1);
  assert.deepEqual(await reader.result(permanent), success("permanent"));
  const other = fixture(t).backend();
  assert.equal(await other.get(permanent), null);
  await delay(1_000);
  assert.equal(await reader.get("failure"), null);
});

test("executor failures use the retry budget and remain inspectable as infrastructure failures", async (t) => {
  const backend = fixture(t).backend();
  let calls = 0;
  await backend.work(async () => { calls++; throw new Error("executor broke"); });
  await backend.submit(proposal("broken", { attempts: 2, backoff: { type: "fixed", delay: 20 } }));
  await assert.rejects(backend.result("broken"), /executor broke/);
  const record = await backend.get("broken");
  assert.equal(record.status, "failed"); assert.equal(record.attempts, 2); assert.equal(calls, 2);
});

test("startup failures reach the host without a submission", async () => {
  const disconnected = new Pool({ connectionString });
  await disconnected.end();
  const reported = Promise.withResolvers();
  const jobs = createJobSystem({ jobs: {}, backend: new PostgresBackend({ pool: disconnected, queue: "startup-failure" }), onError: reported.resolve });
  assert.match((await reported.promise).message, /pool after calling end/);
  await assert.rejects(jobs.close(), /pool after calling end/);
});

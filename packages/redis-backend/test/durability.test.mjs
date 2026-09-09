import assert from "node:assert/strict";
import { fork, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { createServer as createSocketServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Queue } from "bullmq";
import { Container, createJobSystem, defineJob, IdempotencyConflictError, JsonCodec, MemoryBackend } from "core";
import { RedisBackend } from "redis-backend";
import { Ledger } from "./fixtures/ledger.mjs";

const port = Number(process.env.JOB_SYSTEM_REDIS_PORT);
const redisServer = process.env.JOB_SYSTEM_REDIS_SERVER;
const input = { tenant: "tenant-a", operation: "credit-1", amount: 25 };
const policy = { attempts: 3, backoff: { type: "fixed", delay: 0 }, idempotencyKey: JSON.stringify([input.tenant, input.operation]) };
const message = () => ({ id: crypto.randomUUID(), name: "credit", input: new JsonCodec().encode(input), policy });

function cleanupFor(t) {
  const callbacks = [];
  t.after(async () => {
    const errors = [];
    for (const callback of callbacks.reverse()) {
      try { await callback(); }
      catch (error) { errors.push(error); }
    }
    if (errors.length) throw new AggregateError(errors, "Fixture cleanup failed");
  });
  return (callback) => callbacks.push(callback);
}

async function storage(cleanup) {
  const directory = await mkdtemp(join(tmpdir(), "job-idempotency-"));
  cleanup(() => rm(directory, { recursive: true, force: true }));
  return directory;
}

async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
}

function worker(cleanup, queue, filename, mode) {
  const child = fork(fileURLToPath(new URL("./fixtures/idempotency-worker.mjs", import.meta.url)), [queue, filename, mode], {
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let errors = "";
  child.stderr.on("data", (chunk) => { errors += chunk; });
  const messages = [];
  const pending = new Map();
  child.on("message", (message) => { messages.push(message); pending.get(message.kind)?.resolve(message); });
  child.on("exit", (code) => {
    for (const waiter of pending.values()) waiter.reject(new Error(`Worker exited ${code}: ${errors}`));
  });
  cleanup(() => stop(child));
  return {
    child,
    message(kind) {
      const received = messages.find((message) => message.kind === kind);
      if (received) return Promise.resolve(received);
      if (child.exitCode !== null || child.signalCode !== null) return Promise.reject(new Error(errors));
      const waiter = Promise.withResolvers();
      pending.set(kind, waiter);
      return waiter.promise;
    },
  };
}

test("a lost HTTP response retries with the same downstream operation key without repeating the committed effect", { timeout: 10_000 }, async (t) => {
  const cleanup = cleanupFor(t);
  const directory = await storage(cleanup);
  const ledger = new Ledger(join(directory, "effects.sqlite"));
  cleanup(() => ledger.close());
  const receivedKeys = [];
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    receivedKeys.push(request.headers["idempotency-key"]);
    const body = JSON.parse(Buffer.concat(chunks).toString());
    assert.equal(request.headers["idempotency-key"], JSON.stringify([body.tenant, body.operation]));
    const output = ledger.credit(body);
    if (receivedKeys.length === 1) response.destroy();
    else response.end(JSON.stringify(output));
  });
  const listening = once(server, "listening");
  cleanup(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  server.listen(0, "127.0.0.1");
  await listening;
  const url = `http://127.0.0.1:${server.address().port}`;
  const credit = defineJob({
    deps: [],
    async handler(value, signal) {
      const response = await fetch(url, {
        method: "POST", signal, body: JSON.stringify(value),
        headers: { "Idempotency-Key": JSON.stringify([value.tenant, value.operation]) },
      });
      return response.json();
    },
    metadata: {
      idempotencyKey: (value) => JSON.stringify([value.tenant, value.operation]),
      retries: { attempts: 2, backoff: { type: "fixed", delay: 0 } },
    },
  });
  const jobs = createJobSystem({ container: new Container(), jobs: { credit }, backend: new MemoryBackend() });
  cleanup(() => jobs.close());
  assert.deepEqual(await credit(input).result(), { balance: 25 });
  assert.deepEqual(await credit(input).result(), { balance: 25 });
  assert.deepEqual(receivedKeys, [policy.idempotencyKey, policy.idempotencyKey]);
  assert.deepEqual(ledger.inspect(), { deliveries: 2, receipts: 1, balance: 25 });
});

test("a worker killed after database commit is recovered by another process without a second credit", { skip: !port, timeout: 100_000 }, async (t) => {
  const cleanup = cleanupFor(t);
  const directory = await storage(cleanup);
  const filename = join(directory, "effects.sqlite");
  const ledger = new Ledger(filename);
  const queue = `crash-idempotency-${crypto.randomUUID()}`;
  const connection = { host: "127.0.0.1", port };
  const backend = new RedisBackend({ queue, connection });
  const inspector = new Queue(queue, { connection });
  cleanup(async () => {
    await backend.close();
    try { await inspector.obliterate({ force: true }); }
    finally { await inspector.close(); ledger.close(); }
  });
  const doomed = worker(cleanup, queue, filename, "crash");
  await doomed.message("initialized");
  const id = await backend.submit(message());
  assert.deepEqual((await doomed.message("committed")).output, { balance: 25 });
  await stop(doomed.child);
  assert.equal(await (await inspector.getJob(`job-${id}`)).getState(), "active", "the dead worker never acknowledged completion");
  assert.deepEqual(ledger.inspect(), { deliveries: 1, receipts: 1, balance: 25 });
  const replacement = worker(cleanup, queue, filename, "recover");
  await replacement.message("initialized");
  assert.equal(await backend.submit(message()), id, "uncertain acceptance is safe to resubmit");
  const outcome = await backend.result(id, { signal: AbortSignal.timeout(90_000) });
  assert.deepEqual(new JsonCodec().decode(outcome.output), { balance: 25 });
  assert.deepEqual(ledger.inspect(), { deliveries: 2, receipts: 1, balance: 25 });
  assert.equal(await backend.submit(message()), id);
  assert.deepEqual(await backend.result(id), outcome);
  assert.deepEqual(ledger.inspect(), { deliveries: 2, receipts: 1, balance: 25 });
  const exited = once(replacement.child, "exit");
  replacement.child.send("close");
  assert.equal((await exited)[0], 0);
});

test("a Redis process crash preserves operation outcomes on disk for a fresh backend", { skip: !redisServer, timeout: 20_000 }, async (t) => {
  const cleanup = cleanupFor(t);
  const directory = await storage(cleanup);
  const reservation = createSocketServer();
  const listening = once(reservation, "listening");
  reservation.listen(0, "127.0.0.1");
  await listening;
  const redisPort = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  async function start() {
    const child = spawn(redisServer, ["--bind", "127.0.0.1", "--port", String(redisPort), "--dir", directory,
      "--save", "", "--appendonly", "yes", "--appendfsync", "always"], { stdio: ["ignore", "pipe", "pipe"] });
    cleanup(() => stop(child));
    await new Promise((resolve, reject) => {
      let output = "";
      child.on("error", reject);
      child.once("exit", () => reject(new Error(output)));
      child.stderr.on("data", (chunk) => { output += chunk; });
      child.stdout.on("data", (chunk) => {
        output += chunk;
        if (output.includes("Ready to accept connections")) resolve();
      });
    });
    return child;
  }
  const queue = `restart-${crypto.randomUUID()}`;
  const settings = { queue, connection: { host: "127.0.0.1", port: redisPort }, resultTTLSeconds: 1 };
  const redis = await start();
  const original = new RedisBackend(settings);
  cleanup(() => original.close());
  await original.work(async () => ({ status: "succeeded", output: "persisted" }));
  const id = await original.submit(message());
  assert.deepEqual(await original.result(id), { status: "succeeded", output: "persisted" });
  await original.close();
  await stop(redis);
  await start();
  const recovered = new RedisBackend(settings);
  cleanup(() => recovered.close());
  let reexecutions = 0;
  await recovered.work(async () => { reexecutions++; return { status: "succeeded", output: "duplicate effect" }; });
  assert.equal(await recovered.submit(message()), id);
  assert.deepEqual(await recovered.result(id), { status: "succeeded", output: "persisted" });
  await assert.rejects(recovered.submit({ ...message(), input: "changed" }), IdempotencyConflictError);
  assert.equal(reexecutions, 0);
  await recovered.close();
});

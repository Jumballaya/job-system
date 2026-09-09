import assert from "node:assert/strict";
import { fork, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function deadline(operation, ms, description) {
  let timer;
  try {
    return await Promise.race([
      operation,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(description)), ms); }),
    ]);
  } finally { clearTimeout(timer); }
}

export async function eventually(read, predicate, description, ms = 5_000) {
  const until = Date.now() + ms;
  let value;
  do {
    value = await read();
    if (predicate(value)) return value;
    await pause(10);
  } while (Date.now() < until);
  assert.fail(`${description}; last observation: ${JSON.stringify(value)}`);
}

export function scope(t) {
  const callbacks = [];
  t.after(async () => {
    const errors = [];
    for (const callback of callbacks.reverse()) {
      try { await callback(); }
      catch (error) { errors.push(error); }
    }
    if (errors.length) throw new AggregateError(errors, "Acceptance fixture cleanup failed");
  });
  return (callback) => callbacks.push(callback);
}

export async function kill(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await deadline(exited, 5_000, "Test child did not exit after SIGKILL");
}

// Own the server and its AOF; tests never inspect or change application Redis keys.
export async function redis(cleanup) {
  const executable = process.env.JOB_SYSTEM_REDIS_SERVER;
  assert.ok(executable, "Set JOB_SYSTEM_REDIS_SERVER to a redis-server binary; acceptance tests never skip Redis");
  const directory = await mkdtemp(join(tmpdir(), "job-system-acceptance-"));
  cleanup(() => rm(directory, { recursive: true, force: true }));
  const reservation = createServer();
  reservation.listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  let current;
  async function start() {
    const child = spawn(executable, ["--bind", "127.0.0.1", "--port", String(port), "--dir", directory,
      "--save", "", "--appendonly", "yes", "--appendfsync", "always"], { stdio: ["ignore", "pipe", "pipe"] });
    cleanup(() => kill(child));
    await deadline(new Promise((resolve, reject) => {
      let output = "";
      child.once("error", reject);
      child.once("exit", () => reject(new Error(`Redis exited: ${output}`)));
      child.stderr.on("data", (chunk) => { output += chunk; });
      child.stdout.on("data", (chunk) => {
        output += chunk;
        if (output.includes("Ready to accept connections")) resolve();
      });
    }), 5_000, "Isolated Redis did not become ready");
    current = child;
  }
  await start();
  return {
    connection: { host: "127.0.0.1", port },
    queue: `acceptance-${crypto.randomUUID()}`,
    async restart() { await kill(current); await start(); },
  };
}

// IPC carries test inputs and observed handler effects, never backend internals.
export class Peer {
  events = [];
  pending = new Map();
  sequence = 0;
  errors = "";

  constructor(cleanup, settings) {
    this.child = fork(fileURLToPath(new URL("./worker.mjs", import.meta.url)), [JSON.stringify(settings)], {
      stdio: ["ignore", "ignore", "pipe", "ipc"],
    });
    cleanup(() => kill(this.child));
    this.child.stderr.on("data", (chunk) => { this.errors += chunk; });
    this.child.on("message", (message) => {
      if (message.event) this.events.push(message);
      const pending = this.pending.get(message.request);
      if (!pending) return;
      if (message.error) pending.reject(Object.assign(new Error(message.error.message), { name: message.error.name }));
      else pending.resolve(message.value);
    });
    const fail = (error) => { for (const pending of this.pending.values()) pending.reject(error); };
    this.child.on("error", fail);
    this.child.on("exit", (code, signal) => fail(new Error(`Peer exited (${code ?? signal}): ${this.errors}`)));
  }

  async request(command, args = {}, ms = 5_000) {
    const request = ++this.sequence;
    const pending = Promise.withResolvers();
    this.pending.set(request, pending);
    this.child.send({ request, command, ...args }, (error) => { if (error) pending.reject(error); });
    try { return await deadline(pending.promise, ms, `${command} did not settle within ${ms} ms`); }
    finally { this.pending.delete(request); }
  }

  call(name, input, timing) { return this.request("call", { name, input, timing }); }
  result(id, ms = 5_000) { return this.request("result", { id }, ms); }
  effects(label) { return this.events.filter((event) => event.event === "effect" && event.input.label === label); }
  wait(predicate, description, ms = 5_000) {
    return eventually(() => this.events, (events) => events.some(predicate), description, ms)
      .then((events) => events.find(predicate));
  }
  async exit() {
    const exited = once(this.child, "exit");
    await this.request("exit");
    const [code] = await deadline(exited, 5_000, "Closed peer retained live resources");
    assert.equal(code, 0, this.errors);
  }
}

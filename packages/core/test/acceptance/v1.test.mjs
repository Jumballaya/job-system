import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { createJobSystem, defineJob, MemoryBackend } from "../../dist/index.js";
import { deadline, eventually, kill, pause, Peer, redis, scope } from "./helpers.mjs";

test("1 — delayed delivery and editable schedules survive producer, worker, and Redis restarts", { timeout: 30_000 }, async (t) => {
  const cleanup = scope(t);
  const server = await redis(cleanup);
  const producer = new Peer(cleanup, { ...server, worker: false });
  const first = new Peer(cleanup, server);

  const submittedAt = Date.now();
  const delayed = await producer.call("record", { label: "later", value: 17 }, { after: "500ms" });
  const immediate = await producer.call("record", { label: "now" });
  assert.deepEqual(await producer.result(immediate.id), { label: "now", value: 42 });
  const delayedOutput = await producer.result(delayed.id);
  const delivered = await first.wait((event) => event.event === "effect" && event.id === delayed.id, "Delayed job never executed");
  assert.ok(delivered.at >= submittedAt + 500, "after was ignored: delayed work executed early");
  assert.deepEqual(delayedOutput, { label: "later", value: 17 });
  assert.equal(first.effects("later").length, 1);
  const due = Date.now() + 400;
  const at = await producer.call("record", { label: "at" }, { at: new Date(due).toISOString() });
  await producer.result(at.id);
  assert.ok((await first.wait((event) => event.event === "effect" && event.id === at.id, "Absolute delivery missing")).at >= due);

  const otherProducer = new Peer(cleanup, { ...server, worker: false });
  const schedules = await Promise.all([
    producer.call("record", { label: "hourly", value: 8 }, { every: "400ms" }),
    otherProducer.call("record", { value: 8, label: "hourly" }, { every: "400ms" }),
  ]);
  assert.equal(schedules[0].id, schedules[1].id, "concurrent registrations created different schedules");
  await producer.exit();
  await otherProducer.exit();
  const firstRuns = await eventually(() => first.effects("hourly"), (events) => events.length >= 2, "Recurring work stopped when producers exited");
  assert.notEqual(firstRuns[0].id, firstRuns[1].id);
  assert.ok(firstRuns[1].at - firstRuns[0].at >= 300, "duplicate registration produced duplicate occurrences in one interval");
  await first.exit();
  await server.restart();

  const replacement = new Peer(cleanup, server);
  const manager = new Peer(cleanup, { ...server, worker: false });
  await replacement.wait((event) => event.event === "effect" && event.input.label === "hourly", "Restarted worker did not resume persisted schedule");
  assert.notEqual(replacement.child.pid, first.child.pid);
  await manager.request("update", { id: schedules[0].id, timing: { every: "100ms" } });
  const before = replacement.effects("hourly").length;
  await eventually(() => replacement.effects("hourly"), (events) => events.length >= before + 4, "Schedule update did not change delivery cadence", 1_000);
  const laterToday = new Date(Date.now() + 6 * 60 * 60 * 1_000).toISOString().slice(11, 16);
  await manager.request("update", { id: schedules[0].id, timing: { daily: laterToday, timezone: "UTC" } });
  await pause(150); // Let an occurrence already claimed during the update finish.
  const afterCalendarChange = replacement.effects("hourly").length;
  await pause(250);
  assert.equal(replacement.effects("hourly").length, afterCalendarChange, "switching to a calendar left the old interval running");
  await manager.request("update", { id: schedules[0].id, timing: { cron: "* * * * * *", timezone: "UTC" } });
  const calendarRuns = await eventually(() => replacement.effects("hourly").slice(afterCalendarChange),
    (events) => events.length >= 2, "Changing from daily to cron did not resume calendar delivery");
  assert.ok(calendarRuns[1].at - calendarRuns[0].at >= 800, "the old interval survived a calendar update");
  await manager.request("remove", { id: schedules[0].id });
  await manager.request("remove", { id: schedules[0].id });
  await assert.rejects(manager.request("update", { id: schedules[0].id, timing: { every: "100ms" } }), /Schedule not found/);
  await replacement.exit();
  await manager.exit();
  await server.restart();
  const observer = new Peer(cleanup, server);
  const observerProducer = new Peer(cleanup, { ...server, worker: false });
  const barrier = await observerProducer.call("record", { label: "still-alive" });
  await observerProducer.result(barrier.id);
  for (const timing of [{ every: "0s" }, { daily: "09:00" }, { after: "1s", every: "1h" }]) {
    await assert.rejects(observerProducer.call("record", { label: "invalid" }, timing));
  }
  await pause(600);
  assert.equal(observer.effects("hourly").length, 0, "removed schedule was resurrected after restart");
  assert.equal(observer.effects("invalid").length, 0, "invalid timing silently submitted immediate work");

  const calendar = fork(fileURLToPath(new URL("./calendar.mjs", import.meta.url)), { stdio: ["ignore", "ignore", "pipe", "ipc"] });
  cleanup(() => kill(calendar));
  let errors = "";
  calendar.stderr.on("data", (chunk) => { errors += chunk; });
  assert.equal((await deadline(once(calendar, "exit"), 5_000, "Calendar checks hung"))[0], 0, errors);
});

test("2 — recurring successors execute independently while ordinary deduplication protects active work and retries", { timeout: 15_000 }, async (t) => {
  const cleanup = scope(t);
  const server = await redis(cleanup);
  const worker = new Peer(cleanup, { ...server, concurrency: 4 });
  const producer = new Peer(cleanup, { ...server, worker: false });
  const active = await producer.call("record", { label: "ordinary", key: "shared", gate: "ordinary" });
  await worker.wait((event) => event.event === "started" && event.id === active.id, "Original job never started");
  const duplicate = await producer.call("record", { label: "duplicate", key: "shared" });
  const unrelated = await producer.call("fast", { label: "other-job", key: "shared" });
  assert.equal(active.id, duplicate.id);
  assert.notEqual(active.id, unrelated.id);
  assert.deepEqual(await producer.result(unrelated.id), { label: "other-job", value: 42 });

  const schedule = await producer.call("record", { label: "recurring", gate: "occurrences" }, { every: "150ms" });
  const occurrences = await eventually(() => worker.events.filter((event) => event.event === "started" && event.input.label === "recurring"),
    (events) => events.length >= 2, "Running occurrence swallowed its successor");
  assert.notEqual(occurrences[0].id, occurrences[1].id);
  assert.ok(occurrences[1].at >= occurrences[0].at + 100, "Successor executed before its next interval");
  await producer.request("remove", { id: schedule.id });
  await worker.request("release", { label: "occurrences" });
  await worker.request("release", { label: "ordinary" });
  assert.deepEqual(await producer.result(active.id), { label: "ordinary", value: 42 });
  assert.deepEqual(await producer.result(duplicate.id), { label: "ordinary", value: 42 });
  await eventually(() => worker.effects("recurring"), (effects) => effects.length >= 2, "Accepted occurrences failed to finish");
  const effects = worker.effects("recurring");
  assert.equal(new Set(effects.map((event) => event.id)).size, effects.length, "one occurrence repeated its effect");
  assert.equal(worker.effects("ordinary").length, 1);
  assert.equal(worker.effects("duplicate").length, 0);

  const failed = await producer.call("record", { label: "retry", key: "retry", fail: true });
  await worker.wait((event) => event.event === "attemptFailed" && event.id === failed.id && event.attempt === 1, "Retry setup did not fail first attempt");
  assert.equal((await producer.call("record", { label: "retry-duplicate", key: "retry" })).id, failed.id);
  await assert.rejects(producer.result(failed.id), /provider unavailable/);
  assert.deepEqual(worker.events.filter((event) => event.event === "started" && event.id === failed.id).map((event) => event.attempt), [1, 2]);
});

test("3 — saturated work cannot starve another job type or evade its own concurrency limit", { timeout: 15_000 }, async (t) => {
  const cleanup = scope(t);
  const server = await redis(cleanup);
  const worker = new Peer(cleanup, { ...server, concurrency: 2 });
  const producer = new Peer(cleanup, { ...server, worker: false });
  const slow = [];
  for (let index = 0; index < 6; index++) {
    slow.push(await producer.call("slow", { label: `slow-${index}`, gate: `slow-${index}` }));
  }
  await worker.wait((event) => event.event === "started" && event.name === "slow", "Slow work never started");
  for (let index = 0; index < 3; index++) {
    const fast = await producer.call("fast", { label: `fast-${index}` });
    assert.deepEqual(await producer.result(fast.id, 1_000), { label: `fast-${index}`, value: 42 });
  }
  await pause(350); // Force several capacity deferrals before allowing the limited backlog to drain.
  assert.equal((await producer.call("slow", { label: "slow-1", gate: "slow-1" })).id, slow[1].id,
    "waiting for capacity released the active deduplication reservation");
  assert.equal(worker.events.filter((event) => event.event === "started" && event.name === "slow").length, 1,
    "isolation must not bypass the configured slow-job concurrency");
  for (let index = 0; index < slow.length; index++) {
    const started = await worker.wait((event) => event.event === "started" && event.name === "slow" &&
      !worker.events.some((done) => done.event === "effect" && done.id === event.id), "Slow backlog stopped making progress");
    await worker.request("release", { label: started.input.gate });
    await producer.result(started.id);
  }
  assert.equal(new Set(worker.events.filter((event) => event.event === "effect" && event.name === "slow").map((event) => event.id)).size, slow.length);
  const started = worker.events.filter((event) => event.event === "started");
  assert.equal(started.length, slow.length + 3, "capacity waiting duplicated work or spent retries");
  assert.ok(started.every((event) => event.attempt === 1), "capacity waiting spent an execution attempt");
  assert.equal(worker.events.filter((event) => event.event === "attemptFailed").length, 0);
  let active = 0;
  for (const event of worker.events) {
    if (event.event === "started") active++;
    if (event.event === "effect") active--;
    assert.ok(active >= 0 && active <= 2, "dispatch exceeded the physical worker's total capacity");
  }
});

test("4 — shutdown is bounded, signals cleanup, and recovers unfinished work without premature overlap or spent attempts", { timeout: 100_000 }, async (t) => {
  const cleanup = scope(t);
  const server = await redis(cleanup);
  const old = new Peer(cleanup, { ...server, concurrency: 2, shutdownTimeoutMs: 100 });
  const producer = new Peer(cleanup, { ...server, worker: false });
  const cooperative = await producer.call("record", { label: "cooperative", gate: "io", cleanup: "cleanup", cooperative: true });
  const stubborn = await producer.call("record", { label: "stubborn", gate: "ignores-signal" });
  await old.wait((event) => event.event === "started" && event.id === cooperative.id, "Cooperative handler never started");
  await old.wait((event) => event.event === "started" && event.id === stubborn.id, "Stubborn handler never started");
  const queued = await producer.call("record", { label: "queued" });
  await assert.rejects(old.request("close", {}, 1_000), { name: "ShutdownTimeoutError" });
  await old.wait((event) => event.event === "aborted" && event.input.label === "cooperative", "Shutdown never aborted the cooperative request");
  await assert.rejects(old.call("record", { label: "late" }), /closed|not attached/);

  const replacement = new Peer(cleanup, { ...server, hold: false });
  await producer.result(queued.id);
  await pause(200);
  assert.equal(replacement.events.filter((event) => event.event === "started" && event.id === stubborn.id).length, 0,
    "deadline expiry allowed overlapping execution of a live handler");
  assert.equal(replacement.events.filter((event) => event.event === "started" && event.id === cooperative.id).length, 0,
    "retry began before cooperative cleanup settled");
  await old.request("release", { label: "cleanup" });
  await old.wait((event) => event.event === "cleanup", "Cooperative cleanup could not finish after shutdown deadline");
  await kill(old.child);

  assert.deepEqual(await producer.result(stubborn.id, 90_000), { label: "stubborn", value: 42 });
  assert.deepEqual(await producer.result(cooperative.id, 90_000), { label: "cooperative", value: 42 });
  for (const label of ["cooperative", "stubborn"]) {
    assert.equal(old.effects(label).length, 0);
    assert.equal(replacement.effects(label).length, 1);
    assert.equal(replacement.effects(label)[0].attempt, 1, "deployment shutdown spent a business retry attempt");
  }
  await replacement.exit();
});

test("5 — a fresh process can inspect queued, running, retried, and retained failed executions by ID", { timeout: 20_000 }, async (t) => {
  const cleanup = scope(t);
  const server = await redis(cleanup);
  const producer = new Peer(cleanup, { ...server, worker: false, resultTTLSeconds: 1, failureTTLSeconds: 60 });
  const submittedAt = Date.now();
  const held = await producer.call("record", { label: "inspected", gate: "inspect", value: 27 });
  const failed = await producer.call("record", { label: "failure", fail: true });
  await producer.exit();
  const reader = new Peer(cleanup, { ...server, worker: false });
  const queued = await reader.request("inspect", { id: held.id });
  assert.equal(queued.id, held.id);
  assert.equal(queued.name, "record");
  assert.equal(queued.status, "queued");
  assert.equal(queued.attempts, 0);
  assert.deepEqual(queued.input, { label: "inspected", gate: "inspect", value: 27 });
  assert.ok(queued.createdAt >= submittedAt && queued.createdAt <= Date.now());

  const worker = new Peer(cleanup, { ...server, resultTTLSeconds: 1, failureTTLSeconds: 60 });
  await worker.wait((event) => event.event === "started" && event.id === held.id, "Inspectable run never started");
  const running = await reader.request("inspect", { id: held.id });
  assert.equal(running.status, "running");
  assert.equal(running.attempts, 1);
  assert.ok(running.startedAt >= queued.createdAt);
  await worker.request("release", { label: "inspect" });
  const succeeded = await eventually(() => reader.request("inspect", { id: held.id }), (record) => record.status === "succeeded", "Success was never inspectable");
  assert.deepEqual(succeeded.output, { label: "inspected", value: 27 });
  assert.ok(succeeded.finishedAt >= succeeded.startedAt);
  const failure = await eventually(() => reader.request("inspect", { id: failed.id }), (record) => record.status === "failed", "Terminal failure was never inspectable");
  assert.equal(failure.attempts, 2);
  assert.equal(failure.error.message, "provider unavailable");
  assert.equal(failure.error.name, "Error");
  assert.ok(failure.finishedAt >= failure.startedAt);
  await worker.exit();
  await reader.exit();
  await pause(1_200);
  await server.restart();
  const fresh = new Peer(cleanup, { ...server, worker: false });
  assert.deepEqual(await fresh.request("inspect", { id: failed.id }), failure, "failure history vanished with ordinary results or its owning process");
  assert.equal(await fresh.request("inspect", { id: held.id }), null, "ordinary result retention was not enforced");
  assert.equal(await fresh.request("inspect", { id: crypto.randomUUID() }), null);
});

test("6 — startup and idle-worker failures reach one error hook without callers, duplicates, or leaked resources", { timeout: 10_000 }, async () => {
  for (const phase of ["startup", "idle"]) {
    const fatal = new Error(`${phase} processing failure`);
    const stopped = Promise.withResolvers();
    const notifications = [];
    let closes = 0;
    class Backend extends MemoryBackend {
      async work(...args) {
        if (phase === "startup") throw fatal;
        const worker = await super.work(...args);
        return { ...worker, done: stopped.promise };
      }
      async close() { closes++; await super.close(); }
    }
    const job = defineJob({ deps: [], handler: (input) => input });
    const jobs = createJobSystem({ jobs: { job }, backend: new Backend(), onError: (error) => notifications.push(error) });
    try {
      if (phase === "idle") {
        assert.equal(await job("healthy").result(), "healthy");
        stopped.reject(fatal);
      }
      await eventually(() => notifications, (errors) => errors.length > 0, `${phase} worker failure was never reported to its host`, 500);
      assert.deepEqual(notifications, [fatal]);
      await assert.rejects(job("late"), (error) => error === fatal);
      await assert.rejects(job("again"), (error) => error === fatal);
      assert.deepEqual(notifications, [fatal], "each rejected caller repeated the worker-failure notification");
    } finally {
      try { await jobs.close(); }
      catch (error) { assert.equal(error, fatal); }
    }
    assert.equal(closes, 1);
    assert.deepEqual(notifications, [fatal], "shutdown reported the same fatal error again");
  }
  const normal = [];
  const broken = defineJob({ deps: [], handler() { throw new Error("application failure"); }, metadata: { retries: { attempts: 1 } } });
  const jobs = createJobSystem({ jobs: { broken }, backend: new MemoryBackend(), onError: (error) => normal.push(error) });
  await assert.rejects(broken(null).result(), /application failure/);
  await jobs.close();
  assert.deepEqual(normal, [], "application failures and ordinary shutdown are not worker failures");
});

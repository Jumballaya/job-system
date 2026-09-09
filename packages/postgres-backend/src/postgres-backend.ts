import { setTimeout as delay } from "node:timers/promises";
import { failure, JobInterruptedError, prepareSubmission } from "core/backend";
import type { JobBackend, JobExecutor, JobMessage, JobOutcome, JobRecord, JobWorker, WaitOptions, WorkerOptions } from "core/backend";
import type { JobSchedules } from "core/scheduling";
import type { Pool } from "pg";
import { migratePostgres } from "./migrations.js";
import { PostgresStore } from "./store.js";
import type { Claim } from "./store.js";

export interface PostgresBackendOptions {
  /** Borrowed from the application; close it only after the backend finishes draining. */
  readonly pool: Pool;
  readonly queue: string;
  /** Defaults to 90 days. Null retains ordinary results indefinitely; idempotency records never expire. */
  readonly resultTTLSeconds?: number | null;
  readonly failureTTLSeconds?: number | null;
  /** Cross-process delivery and result polling interval; defaults to 250 ms. */
  readonly pollIntervalMs?: number;
  /** Renewed every third of this interval; defaults to 30 seconds. */
  readonly leaseDurationMs?: number;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > 2_147_483_647) throw new Error(`${name} must be a positive 32-bit integer`);
  return value;
}

function retention(value: number | null | undefined): number | null {
  if (value === null) return null;
  const milliseconds = (value ?? 90 * 24 * 60 * 60) * 1_000;
  if (!Number.isSafeInteger(milliseconds) || milliseconds < 1) throw new Error("Retention must be positive seconds or null");
  return milliseconds;
}

async function interruptible<T>(pending: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  let abort: () => void;
  const stopped = new Promise<never>((_, reject) => {
    abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
  });
  try { return await Promise.race([pending, stopped]); }
  finally { signal.removeEventListener("abort", abort!); }
}

/** Postgres owns delivery, schedules, leases, and retained outcomes in the job_system schema. */
export class PostgresBackend implements JobBackend {
  private readonly store: PostgresStore;
  private readonly pollInterval: number;
  private readonly leaseDuration: number;
  private readonly closed = new AbortController();
  private readonly workers = new Set<JobWorker>();
  private closing?: Promise<void>;
  readonly schedules: JobSchedules;

  constructor(private readonly options: PostgresBackendOptions) {
    if (typeof options.queue !== "string" || !options.queue.trim()) throw new Error("Postgres queue must be nonempty");
    this.pollInterval = positiveInteger(options.pollIntervalMs ?? 250, "pollIntervalMs");
    this.leaseDuration = positiveInteger(options.leaseDurationMs ?? 30_000, "leaseDurationMs");
    if (this.leaseDuration < 30) throw new Error("leaseDurationMs must be at least 30");
    this.store = new PostgresStore(options.pool, options.queue, {
      result: retention(options.resultTTLSeconds), failure: retention(options.failureTTLSeconds),
    });
    this.schedules = {
      upsert: async (message, rule) => {
        this.closed.signal.throwIfAborted();
        return this.store.saveSchedule(prepareSubmission(message), rule);
      },
      update: async (id, rule) => { this.closed.signal.throwIfAborted(); await this.store.updateSchedule(id, rule); },
      remove: async (id) => { this.closed.signal.throwIfAborted(); await this.store.removeSchedule(id); },
    };
  }

  /** Explicit setup, separate from workers so production connections need no DDL privileges. */
  async migrate(): Promise<void> {
    this.closed.signal.throwIfAborted();
    await migratePostgres(this.options.pool);
  }

  async submit(message: JobMessage): Promise<string> {
    this.closed.signal.throwIfAborted();
    if (typeof message.id !== "string" || !message.id || typeof message.name !== "string" || !message.name ||
      typeof message.input !== "string") throw new Error("A job message requires an ID, a name, and serialized input");
    positiveInteger(message.policy.attempts, "attempts");
    if (!["fixed", "exponential"].includes(message.policy.backoff.type) ||
      !Number.isFinite(message.policy.backoff.delay) || message.policy.backoff.delay < 0) throw new Error("Invalid retry backoff");
    return this.store.submit(prepareSubmission(message));
  }

  async get(id: string, options?: WaitOptions): Promise<JobRecord<string, string> | null> {
    const signal = this.waitSignal(options);
    signal.throwIfAborted();
    return interruptible(this.store.get(id), signal);
  }

  async result(id: string, options?: WaitOptions): Promise<JobOutcome> {
    const signal = this.waitSignal(options);
    for (;;) {
      signal.throwIfAborted();
      const outcome = await interruptible(this.store.outcome(id), signal);
      if (outcome) return outcome;
      await delay(this.pollInterval, undefined, { signal }).catch(() => signal.throwIfAborted());
    }
  }

  private waitSignal(options?: WaitOptions): AbortSignal {
    return options?.signal ? AbortSignal.any([this.closed.signal, options.signal]) : this.closed.signal;
  }

  async work(execute: JobExecutor, options: WorkerOptions = {}): Promise<JobWorker> {
    this.closed.signal.throwIfAborted();
    const concurrency = positiveInteger(options.concurrency ?? 1, "concurrency");
    const limits = new Map(Object.entries(options.concurrencyByJob ?? {}));
    for (const limit of limits.values()) positiveInteger(limit, "Job concurrency");
    await this.store.ready();
    this.closed.signal.throwIfAborted();

    const stopping = new AbortController();
    const active = new Map<string, { name: string; signal: AbortController; done: Promise<void> }>();
    let failed: { error: unknown } | undefined;
    const fail = (error: unknown) => {
      failed ??= { error };
      stopping.abort(error);
      for (const run of active.values()) run.signal.abort(error);
    };
    const run = async (claim: Claim, signal: AbortController): Promise<void> => {
      const heartbeatStop = new AbortController();
      const heartbeat = (async () => {
        while (!heartbeatStop.signal.aborted) {
          try { await delay(Math.floor(this.leaseDuration / 3), undefined, { signal: heartbeatStop.signal }); }
          catch { return; }
          if (!(await this.store.renew(claim, this.leaseDuration))) {
            signal.abort(new Error(`Job lease lost: ${claim.message.id}`));
            return;
          }
        }
      })().catch((error: unknown) => fail(error));
      try {
        let outcome: JobOutcome;
        let infrastructure;
        try { outcome = await execute(claim.message, signal.signal, claim.attempt); }
        catch (error) {
          if (error instanceof JobInterruptedError) {
            await this.store.release(claim);
            return;
          }
          infrastructure = failure(error);
          outcome = { status: "failed", error: infrastructure, retryable: true };
        }
        // A lost lease cannot publish an outcome, even if the old handler ignored cancellation.
        if (!signal.signal.aborted) await this.store.finish(claim, outcome, infrastructure);
      } finally {
        heartbeatStop.abort();
        await heartbeat;
      }
    };

    const done = (async () => {
      let nextPrune = 0;
      try {
        while (!stopping.signal.aborted) {
          await this.store.advanceSchedules();
          if (Date.now() >= nextPrune) { await this.store.prune(); nextPrune = Date.now() + 60_000; }
          while (!stopping.signal.aborted && active.size < concurrency) {
            const counts = new Map<string, number>();
            for (const { name } of active.values()) counts.set(name, (counts.get(name) ?? 0) + 1);
            const excluded = [...limits].filter(([name, limit]) => (counts.get(name) ?? 0) >= limit).map(([name]) => name);
            const claim = await this.store.claim(excluded, this.leaseDuration);
            if (!claim) break;
            if (stopping.signal.aborted) { await this.store.release(claim); break; }
            const signal = new AbortController();
            const running = Promise.resolve().then(() => run(claim, signal)).catch(fail)
              .finally(() => active.delete(claim.token));
            active.set(claim.token, { name: claim.message.name, signal, done: running });
          }
          if (!stopping.signal.aborted) {
            try { await delay(this.pollInterval, undefined, { signal: stopping.signal }); }
            catch { /* Stop wakes polling while active work continues draining. */ }
          }
        }
      } catch (error) { fail(error); }
      await Promise.all([...active.values()].map((run) => run.done));
      if (failed) throw failed.error;
    })();
    const worker: JobWorker = { done, close: () => { stopping.abort(); return done; } };
    this.workers.add(worker);
    void done.then(() => this.workers.delete(worker), () => this.workers.delete(worker));
    return worker;
  }

  /** Deletes up to 1,000 expired terminal records; workers also run this once per minute. */
  async prune(): Promise<number> {
    this.closed.signal.throwIfAborted();
    return this.store.prune();
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed.abort(new Error("Postgres backend is closed"));
    this.closing = (async () => {
      const results = await Promise.allSettled([...this.workers].map((worker) => worker.close()));
      const errors = results.filter((result) => result.status === "rejected").map((result) => result.reason);
      if (errors.length) throw new AggregateError(errors, "Postgres workers failed while draining");
    })();
    return this.closing;
  }
}

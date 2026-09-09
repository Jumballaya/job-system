import { DelayedError, Queue, Worker } from "bullmq";
import type { Job, RedisOptions } from "bullmq";
import { assertSameSubmission, JobInterruptedError, prepareSubmission, ResultUnavailableError } from "../backend.js";
import type { JobBackend, JobExecutor, JobMessage, JobOutcome, JobWorker, WaitOptions, WorkerOptions } from "../backend.js";
import { occurrence, scheduleId, validateRule } from "../scheduling.js";
import type { JobSchedules, ScheduleRule } from "../scheduling.js";
import { ConcurrencyLimits } from "../concurrency.js";

const redisRequestTimeoutMs = 5_000;
const resultPollIntervalMs = 50;
const cleanupBatchSize = 100;
// Return saturated deliveries to Redis so other job types can use the worker immediately.
const capacityDelayMs = 100;
type StoredMessage = JobMessage & { readonly resultTTLSeconds?: number; readonly schedule?: ScheduleRule };

export interface RedisBackendOptions {
  readonly queue: string;
  readonly connection: Pick<RedisOptions, "host" | "port" | "username" | "password" | "db" | "tls">;
  readonly resultTTLSeconds?: number;
}

function abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    operation.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
    if (signal.aborted) abort();
  });
}

function pause(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => {
      signal.removeEventListener("abort", abort);
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, resultPollIntervalMs);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

// Recurring executions already have BullMQ IDs; ordinary IDs are escaped custom IDs.
const idPrefix = "job-";
const recurringId = /^repeat:schedule-[a-f0-9]{64}:\d+$/;
function redisId(id: string): string {
  if (recurringId.test(id)) return id;
  return `${idPrefix}${encodeURIComponent(id)}`;
}
function applicationId(redis: string): string {
  return decodeURIComponent(redis.slice(idPrefix.length));
}

/** BullMQ owns delivery and retained outcomes; each backend owns its connections and workers. */
export class RedisBackend implements JobBackend {
  private readonly queue: Queue<StoredMessage, JobOutcome>;
  private readonly connection: RedisOptions;
  private readonly retention: number;
  private readonly workers = new Set<JobWorker>();
  private readonly closed = new AbortController();
  private closing?: Promise<void>;
  private cleanup?: Promise<void>;
  private readonly cleanupOffsets = { completed: 0, failed: 0 };
  public readonly schedules: JobSchedules = {
    upsert: async (message, rule) => {
      this.closed.signal.throwIfAborted();
      message = prepareSubmission(message);
      rule = validateRule(rule);
      const id = scheduleId(message);
      await this.setSchedule(id, message, rule);
      return id;
    },
    update: async (id, rule) => {
      this.closed.signal.throwIfAborted();
      rule = validateRule(rule);
      const stored = await abortable(this.queue.getJobScheduler(id), this.requestSignal());
      if (!stored?.template?.data) throw new Error(`Schedule not found: ${id}`);
      await this.setSchedule(id, stored.template.data, rule);
    },
    remove: async (id) => {
      this.closed.signal.throwIfAborted();
      await abortable(this.queue.removeJobScheduler(id), this.requestSignal());
    },
  };

  private requestSignal(): AbortSignal {
    return AbortSignal.any([this.closed.signal, AbortSignal.timeout(redisRequestTimeoutMs)]);
  }

  private async setSchedule(id: string, message: JobMessage, rule: ScheduleRule): Promise<void> {
    const signal = this.requestSignal();
    const existing = await abortable(this.queue.getJobScheduler(id), signal);
    if (existing?.template?.data && JSON.stringify(existing.template.data.schedule) === JSON.stringify(rule) &&
      JSON.stringify(existing.template.data.policy) === JSON.stringify(message.policy)) return;
    const repeat = "every" in rule ? { every: rule.every, startDate: Date.now() + rule.every }
      : { pattern: rule.cron, tz: rule.timezone };
    await abortable(this.queue.upsertJobScheduler(id, repeat, {
      name: message.name,
      data: { ...message, schedule: rule, resultTTLSeconds: this.retention },
      opts: { attempts: message.policy.attempts, backoff: { ...message.policy.backoff }, removeOnComplete: false, removeOnFail: false },
    }), signal);
  }

  constructor(options: RedisBackendOptions) {
    if (!options.queue.trim() || options.queue.includes(":")) {
      throw new Error("Redis queue must be nonempty and must not contain ':'");
    }
    this.retention = options.resultTTLSeconds ?? 300;
    if (!Number.isSafeInteger(this.retention) || this.retention < 1) {
      throw new Error("resultTTLSeconds must be a positive safe integer");
    }
    if (options.connection.port !== undefined &&
      (!Number.isInteger(options.connection.port) || options.connection.port < 1 || options.connection.port > 65535)) {
      throw new Error("Redis port must be an integer between 1 and 65535");
    }
    if (options.connection.db !== undefined &&
      (!Number.isSafeInteger(options.connection.db) || options.connection.db < 0)) {
      throw new Error("Redis db must be a nonnegative safe integer");
    }
    this.connection = { ...options.connection, connectTimeout: redisRequestTimeoutMs };
    this.queue = new Queue(options.queue, {
      connection: { ...this.connection, maxRetriesPerRequest: 1, commandTimeout: redisRequestTimeoutMs },
      defaultJobOptions: {
        // BullMQ age/count cleanup sweeps whole terminal sets, including other jobs marked keep-forever.
        removeOnComplete: false,
        removeOnFail: false,
      },
    });
    // Redis reconnect errors also reject requests; EventEmitter still needs a listener.
    this.queue.on("error", () => {});
  }

  public async submit(message: JobMessage): Promise<string> {
    this.closed.signal.throwIfAborted();
    if (typeof message.id !== "string" || !message.id ||
      typeof message.name !== "string" || !message.name || typeof message.input !== "string" ||
      typeof message.policy !== "object" || message.policy === null) {
      throw new Error("A job message requires an ID, a name, a serialized input, and a policy");
    }
    message = prepareSubmission(message);
    if (recurringId.test(message.id)) throw new Error("Job ID is reserved for a scheduled occurrence");
    const id = redisId(message.id);
    const { attempts, backoff, key } = message.policy;
    const signal = AbortSignal.any([this.closed.signal, AbortSignal.timeout(redisRequestTimeoutMs)]);
    const added = await abortable(this.queue.add(message.name, { ...message, resultTTLSeconds: this.retention }, {
      jobId: id,
      attempts,
      backoff: { type: backoff.type, delay: backoff.delay },
      ...(message.availableAt !== undefined ? { delay: Math.max(0, message.availableAt - Date.now()) } : {}),
      // Simple deduplication holds the key while the job is queued, delayed, or active.
      ...(key !== undefined ? { deduplication: { id: key } } : {}),
    }), signal);
    if (added.id !== undefined && added.id !== id) return applicationId(added.id);
    const stored = await abortable(this.queue.getJob(id), signal);
    if (!stored) throw new ResultUnavailableError(message.id);
    this.assertRetained(stored, message.id);
    assertSameSubmission(stored.data, message);
    await abortable(this.prune(), signal);
    return message.id;
  }

  public async result(id: string, options?: WaitOptions): Promise<JobOutcome> {
    const signal = options?.signal
      ? AbortSignal.any([this.closed.signal, options.signal])
      : this.closed.signal;
    signal.throwIfAborted();
    while (true) {
      const job = await abortable(this.queue.getJob(redisId(id)), AbortSignal.any([signal, AbortSignal.timeout(redisRequestTimeoutMs)]));
      if (!job) throw new ResultUnavailableError(id);
      const state = await abortable(job.getState(), AbortSignal.any([signal, AbortSignal.timeout(redisRequestTimeoutMs)]));
      if (state === "completed" || state === "failed") {
        const finished = await abortable(this.queue.getJob(redisId(id)), AbortSignal.any([signal, AbortSignal.timeout(redisRequestTimeoutMs)]));
        if (!finished?.finishedOn) throw new ResultUnavailableError(id);
        this.assertRetained(finished, id);
        if (state === "failed") throw new Error(`Job infrastructure failed: ${finished.failedReason}`);
        return finished.returnvalue;
      }
      if (state === "unknown") throw new ResultUnavailableError(id);
      await pause(signal);
    }
  }

  private expiresAt(job: Job<StoredMessage, JobOutcome>): number {
    if (job.finishedOn === undefined || job.data.policy.idempotencyKey !== undefined) return Infinity;
    const policy = job.opts.removeOnComplete;
    const retention = job.data.resultTTLSeconds ?? (typeof policy === "object" && "age" in policy ? policy.age : this.retention);
    return job.finishedOn + retention * 1_000;
  }

  private assertRetained(job: Job<StoredMessage, JobOutcome>, id: string): void {
    if (Date.now() >= this.expiresAt(job)) throw new ResultUnavailableError(id);
  }

  private prune(): Promise<void> {
    this.cleanup ??= (async () => {
      const signal = AbortSignal.any([this.closed.signal, AbortSignal.timeout(redisRequestTimeoutMs)]);
      for (const state of ["completed", "failed"] as const) {
        const offset = this.cleanupOffsets[state];
        const jobs = await abortable(this.queue.getJobs([state], offset, offset + cleanupBatchSize - 1, true), signal);
        let removed = 0;
        for (const job of jobs) {
          if (Date.now() < this.expiresAt(job)) continue;
          signal.throwIfAborted();
          await abortable(job.remove(), signal);
          removed++;
        }
        // Cycle through permanent records too; concurrent removals are caught on the next pass.
        this.cleanupOffsets[state] = jobs.length < cleanupBatchSize ? 0 : offset + jobs.length - removed;
      }
    })().finally(() => { this.cleanup = undefined; });
    return this.cleanup;
  }

  public async work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker> {
    this.closed.signal.throwIfAborted();
    const concurrency = options?.concurrency ?? 1;
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
      throw new Error("Worker concurrency must be a positive safe integer");
    }
    const limits = new ConcurrencyLimits(options?.concurrencyByJob);
    const execution = new AbortController();
    const worker = new Worker<JobMessage, JobOutcome>(this.queue.name, async (job, token) => {
      const release = limits.acquire(job.data.name);
      if (!release) {
        // moveToDelayed preserves attemptsMade and active deduplication; DelayedError avoids a failure outcome.
        await job.moveToDelayed(Date.now() + capacityDelayMs, token);
        throw new DelayedError();
      }
      try {
        const attempt = job.attemptsMade + 1;
        const message = job.id && recurringId.test(job.id) ? occurrence(job.data, job.id) : job.data;
        // Recurrence can run forever without another producer submission to trigger retention cleanup.
        if (message !== job.data) await this.prune();
        const outcome = await execute(message, execution.signal, attempt);
        if (outcome.status === "failed" && outcome.retryable && attempt < (job.opts.attempts ?? 1)) {
          throw new Error(outcome.error.message);
        }
        return outcome;
      } catch (error) {
        if (!(error instanceof JobInterruptedError)) throw error;
        // The executor has finished cleanup. Hand off with the same ID and attempt budget.
        await worker.pause(true);
        await job.moveToDelayed(Date.now(), token);
        throw new DelayedError();
      } finally { release(); }
    }, {
      connection: { ...this.connection, maxRetriesPerRequest: null },
      concurrency,
      autorun: false,
    });
    // Reconnection is recoverable; run() rejection below identifies a stopped processing loop.
    worker.on("error", () => {});
    let ready = false;
    const startup = abortable(worker.waitUntilReady(),
      AbortSignal.any([this.closed.signal, AbortSignal.timeout(redisRequestTimeoutMs)]));
    const running = startup.then(() => {
      this.closed.signal.throwIfAborted();
      ready = true;
      return worker.run();
    });
    const done = running.then(
      () => worker.close(),
      async (error: unknown) => {
        execution.abort(error);
        await worker.close(true);
        throw error;
      },
    );
    const session: JobWorker = { done, close: () => worker.close(!ready) };
    this.workers.add(session);
    void done.then(() => this.workers.delete(session), () => this.workers.delete(session));
    try {
      await startup;
      this.closed.signal.throwIfAborted();
      return session;
    } catch (error) {
      await done.catch(() => {});
      throw error;
    }
  }

  public close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed.abort(new Error("Redis backend is closed"));
    this.closing = (async () => {
      const results = await Promise.allSettled([...this.workers].map((worker) => worker.close()));
      await this.cleanup?.catch(() => {});
      await this.queue.close();
      const failure = results.find((result) => result.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    })();
    return this.closing;
  }
}

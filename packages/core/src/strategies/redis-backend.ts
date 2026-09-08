import { Queue, Worker } from "bullmq";
import type { Job, RedisOptions } from "bullmq";
import { ResultUnavailableError } from "../backend.js";
import type { JobBackend, JobExecutor, JobMessage, JobOutcome, JobWorker, WaitOptions, WorkerOptions } from "../backend.js";

const redisRequestTimeoutMs = 5_000;
const resultPollIntervalMs = 50;

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

// Prefix and escaping make every application ID legal as a BullMQ custom ID.
const idPrefix = "job-";
function redisId(id: string): string {
  return `${idPrefix}${encodeURIComponent(id)}`;
}
function applicationId(redis: string): string {
  return decodeURIComponent(redis.slice(idPrefix.length));
}

/** BullMQ owns delivery and retained outcomes; each backend owns its connections and workers. */
export class RedisBackend implements JobBackend {
  private readonly queue: Queue<JobMessage, JobOutcome>;
  private readonly connection: RedisOptions;
  private readonly retention: number;
  private readonly workers = new Set<JobWorker>();
  private readonly closed = new AbortController();
  private closing?: Promise<void>;

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
        removeOnComplete: { age: this.retention },
        removeOnFail: { age: this.retention },
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
    const id = redisId(message.id);
    const { attempts, backoff, key } = message.policy;
    const signal = AbortSignal.any([this.closed.signal, AbortSignal.timeout(redisRequestTimeoutMs)]);
    const added = await abortable(this.queue.add(message.name, message, {
      jobId: id,
      attempts,
      backoff: { type: backoff.type, delay: backoff.delay },
      // Simple deduplication holds the key while the job is queued, delayed, or active.
      ...(key !== undefined ? { deduplication: { id: key } } : {}),
    }), signal);
    if (added.id !== undefined && added.id !== id) return applicationId(added.id);
    const stored = await abortable(this.queue.getJob(id), signal);
    if (!stored) throw new ResultUnavailableError(message.id);
    this.assertRetained(stored, message.id);
    if (stored.data.name !== message.name || stored.data.input !== message.input) {
      throw new Error(`Job ID already belongs to another submission: ${message.id}`);
    }
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

  private assertRetained(job: Job<JobMessage, JobOutcome>, id: string): void {
    if (job.finishedOn === undefined) return;
    // Both terminal sets share retention; BullMQ's physical age cleanup is lazy.
    const policy = job.opts.removeOnComplete;
    const retention = typeof policy === "object" && "age" in policy ? policy.age : this.retention;
    if (Date.now() >= job.finishedOn + retention * 1_000) throw new ResultUnavailableError(id);
  }

  public async work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker> {
    this.closed.signal.throwIfAborted();
    const concurrency = options?.concurrency ?? 1;
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
      throw new Error("Worker concurrency must be a positive safe integer");
    }
    const execution = new AbortController();
    const worker = new Worker<JobMessage, JobOutcome>(this.queue.name, async (job) => {
      // BullMQ counts previous failures in attemptsMade; a thrown retryable failure lets it apply the backoff.
      const attempt = job.attemptsMade + 1;
      const outcome = await execute(job.data, execution.signal, attempt);
      if (outcome.status === "failed" && outcome.retryable && attempt < (job.opts.attempts ?? 1)) {
        throw new Error(outcome.error.message);
      }
      return outcome;
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
      await this.queue.close();
      const failure = results.find((result) => result.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    })();
    return this.closing;
  }
}

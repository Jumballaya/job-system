import { assertSameSubmission, backoffDelay, prepareSubmission, ResultUnavailableError } from "../backend.js";
import type { JobBackend, JobExecutor, JobMessage, JobOutcome, JobWorker, WaitOptions, WorkerOptions } from "../backend.js";

const MAX_RETAINED_RESULTS = 1_000;

type Completion = { outcome: JobOutcome } | { error: unknown };
type Entry = {
  message: JobMessage;
  attempt: number;
  retry?: NodeJS.Timeout;
  completion?: Completion;
  expiresAt?: number;
  waiters: Set<(completion: Completion) => void>;
};
type Consumer = {
  execute: JobExecutor;
  concurrency: number;
  active: number;
  stopping: boolean;
  failure?: { error: unknown };
  resolveDone: () => void;
  rejectDone: (error: unknown) => void;
  handle: JobWorker;
};

/** Process-local FIFO jobs; idempotency records last until close, ordinary results have bounded retention. */
export class MemoryBackend implements JobBackend {
  private readonly entries = new Map<string, Entry>();
  /** Active entries by dedupe key; cleared when the entry completes. */
  private readonly keyed = new Map<string, Entry>();
  private readonly queue: Entry[] = [];
  private readonly consumers = new Set<Consumer>();
  private readonly shutdown = new AbortController();
  private readonly resultTTLms: number;
  private closePromise?: Promise<void>;

  constructor(options: { resultTTLms?: number } = {}) {
    this.resultTTLms = options.resultTTLms ?? 60_000;
    if (!Number.isFinite(this.resultTTLms) || this.resultTTLms <= 0) throw new Error("resultTTLms must be positive");
  }

  public async submit(message: JobMessage): Promise<string> {
    this.assertOpen();
    message = prepareSubmission(message);
    this.prune();
    const existing = this.entries.get(message.id);
    if (existing) {
      assertSameSubmission(existing.message, message);
      return message.id;
    }
    const key = message.policy.key;
    if (key !== undefined) {
      const active = this.keyed.get(key);
      if (active) return active.message.id;
    }
    const entry: Entry = { message: Object.freeze({ ...message }), attempt: 0, waiters: new Set() };
    this.entries.set(message.id, entry);
    if (key !== undefined) this.keyed.set(key, entry);
    this.queue.push(entry);
    this.pump();
    return message.id;
  }

  public async result(id: string, options?: WaitOptions): Promise<JobOutcome> {
    this.assertOpen();
    const signal = options?.signal ? AbortSignal.any([options.signal, this.shutdown.signal]) : this.shutdown.signal;
    signal.throwIfAborted();
    this.prune();
    const entry = this.entries.get(id);
    if (!entry) throw new ResultUnavailableError(id);
    if (entry.completion) {
      if ("error" in entry.completion) throw entry.completion.error;
      return structuredClone(entry.completion.outcome);
    }
    return new Promise<JobOutcome>((resolve, reject) => {
      const cleanup = () => { entry.waiters.delete(finish); signal.removeEventListener("abort", abort); };
      const finish = (completion: Completion) => {
        cleanup();
        if ("error" in completion) reject(completion.error);
        else resolve(structuredClone(completion.outcome));
      };
      const abort = () => { cleanup(); reject(signal.reason); };
      entry.waiters.add(finish);
      signal.addEventListener("abort", abort, { once: true });
    });
  }

  public async work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker> {
    this.assertOpen();
    const concurrency = options?.concurrency ?? 1;
    if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error("concurrency must be a positive integer");
    let resolveDone!: () => void;
    let rejectDone!: (error: unknown) => void;
    const done = new Promise<void>((resolve, reject) => { resolveDone = resolve; rejectDone = reject; });
    // Keep failures observable through done without creating an unhandled rejection before callers attach.
    void done.catch(() => {});
    const consumer: Consumer = {
      execute, concurrency, active: 0, stopping: false, resolveDone, rejectDone,
      handle: {
        done,
        close: () => {
          consumer.stopping = true;
          if (consumer.active === 0) this.finishConsumer(consumer);
          return done;
        },
      },
    };
    this.consumers.add(consumer);
    this.pump();
    return consumer.handle;
  }

  public close(): Promise<void> {
    if (!this.closePromise) {
      this.shutdown.abort(new Error("Memory backend is closed"));
      this.closePromise = Promise.resolve().then(async () => {
        try {
          const results = await Promise.allSettled([...this.consumers].map((worker) => worker.handle.close()));
          const failed = results.find((result) => result.status === "rejected");
          if (failed?.status === "rejected") throw failed.reason;
        } finally {
          for (const entry of this.entries.values()) clearTimeout(entry.retry);
          this.entries.clear();
          this.keyed.clear();
          this.queue.length = 0;
        }
      });
    }
    return this.closePromise;
  }

  private assertOpen(): void {
    if (this.shutdown.signal.aborted) throw new Error("Memory backend is closed");
  }

  private pump(): void {
    if (this.shutdown.signal.aborted) return;
    for (const consumer of this.consumers) {
      while (!consumer.stopping && consumer.active < consumer.concurrency && this.queue.length) {
        const entry = this.queue.shift()!;
        consumer.active++;
        void this.execute(consumer, entry);
      }
    }
  }

  private async execute(consumer: Consumer, entry: Entry): Promise<void> {
    entry.attempt++;
    try {
      const outcome = await consumer.execute(entry.message, new AbortController().signal, entry.attempt);
      if (outcome.status === "failed" && outcome.retryable && entry.attempt < entry.message.policy.attempts) {
        this.scheduleRetry(entry);
      } else {
        this.complete(entry, { outcome: structuredClone(outcome) });
      }
    } catch (error) {
      this.complete(entry, { error });
      consumer.stopping = true;
      consumer.failure ??= { error };
    } finally {
      consumer.active--;
      if (consumer.stopping && consumer.active === 0) {
        this.finishConsumer(consumer);
      }
      this.pump();
    }
  }

  private scheduleRetry(entry: Entry): void {
    entry.retry = setTimeout(() => {
      entry.retry = undefined;
      if (this.shutdown.signal.aborted) return;
      this.queue.push(entry);
      this.pump();
    }, backoffDelay(entry.message.policy, entry.attempt));
  }

  private finishConsumer(consumer: Consumer): void {
    this.consumers.delete(consumer);
    if (consumer.failure) consumer.rejectDone(consumer.failure.error);
    else consumer.resolveDone();
  }

  private complete(entry: Entry, completion: Completion): void {
    entry.completion = completion;
    if (entry.message.policy.idempotencyKey === undefined) entry.expiresAt = Date.now() + this.resultTTLms;
    const key = entry.message.policy.key;
    if (key !== undefined && this.keyed.get(key) === entry) this.keyed.delete(key);
    for (const waiter of [...entry.waiters]) waiter(completion);
    this.prune();
  }

  private prune(): void {
    const completed: string[] = [];
    for (const [id, entry] of this.entries) {
      if (entry.expiresAt === undefined) continue;
      if (entry.expiresAt <= Date.now()) this.entries.delete(id);
      else completed.push(id);
    }
    // Bound idle retained state as well as age; pending work is never evicted.
    for (const id of completed.slice(0, -MAX_RETAINED_RESULTS)) this.entries.delete(id);
  }
}

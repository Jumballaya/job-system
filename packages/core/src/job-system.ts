import { Container } from "./dep-inject.js";
import type { Constructor } from "./dep-inject.js";
import { failure, JobExecutionError, JobInterruptedError, NonRetryableError, prepareSubmission, ShutdownTimeoutError } from "./backend.js";
import type { JobBackend, JobMessage, JobOutcome, JobPolicy, JobRecord, JobWorker, WaitOptions } from "./backend.js";
import { JsonCodec } from "./codec.js";
import type { JobCodec } from "./codec.js";
import { delivery } from "./scheduling.js";
import type { Delay, Delivery, Recurrence, ScheduleHandle, Timing } from "./scheduling.js";

type MaybePromise<T> = T | PromiseLike<T>;
type Instances<Deps extends readonly Constructor[]> = {
  -readonly [Index in keyof Deps]: InstanceType<Deps[Index]>;
};

export interface JobContext {
  readonly jobId: string;
  readonly name: string;
  /** Aborted by job timeout, execution loss, or expiry of the shutdown grace period. */
  readonly signal: AbortSignal;
  /** Starts at 1 and counts execution attempts, including retries; capacity waiting doesn't count. */
  readonly attempt: number;
  readonly maxAttempts: number;
}

export type Backoff = "fixed" | "exponential" | { readonly type: "fixed" | "exponential"; readonly delay: number };

/** Every field has a production default; see `defaultMetadata`. */
export type JobMetadata<Input = unknown> = {
  readonly retries?: { readonly attempts?: number; readonly backoff?: Backoff };
  /** Milliseconds from delivery until cancellation is requested; active work must settle before retry. */
  readonly timeout?: number;
  /** Simultaneous executions, including hooks, within this worker; the system's concurrency still applies. */
  readonly concurrency?: number;
} & ({
  /** Derives a key scoped to this job's registration; submitting while active reuses that job. */
  readonly key?: (input: NoInfer<Input>) => string;
  readonly idempotencyKey?: never;
} | {
  readonly key?: never;
  /** Replays the same operation, including completed outcomes; include tenant identity when applicable. */
  readonly idempotencyKey: (input: NoInfer<Input>) => string;
});

export interface JobDefinition<Deps extends readonly Constructor[], Input, Output> {
  readonly deps: Deps;
  readonly metadata?: JobMetadata<Input>;
  readonly handler: (input: Input, ...args: [...Instances<Deps>, AbortSignal]) => Output;
  readonly beforeRun?: (input: NoInfer<Input>, context: JobContext) => MaybePromise<void>;
  readonly onSuccess?: (output: Awaited<NoInfer<Output>>, context: JobContext) => MaybePromise<void>;
  /** Runs after every failed attempt, including the final one. */
  readonly onError?: (error: unknown, context: JobContext) => MaybePromise<void>;
}

/** Accepted work. The result can be awaited repeatedly while the backend retains it. */
export interface JobHandle<Output> {
  readonly id: string;
  result(options?: WaitOptions): Promise<Output>;
}

/** Resolves once the backend accepts the work; `result` also waits for the output. */
export type JobSubmission<Output> = Promise<JobHandle<Output>> & {
  result(options?: WaitOptions): Promise<Output>;
};

/** A definition is called to submit work; it must be attached to a system first. */
export type Job<Deps extends readonly Constructor[], Input, Output> = {
  (input: Input, timing?: Delay): JobSubmission<Awaited<Output>>;
  (input: Input, timing: Recurrence): Promise<ScheduleHandle>;
  (input: Input, timing: Timing): JobSubmission<Awaited<Output>> | Promise<ScheduleHandle>;
  readonly definition: JobDefinition<Deps, Input, Output>;
};

// Heterogeneous definitions are erased only inside dispatch; each job keeps its own input pairing.
type AnyDefinition = {
  readonly deps: readonly Constructor[];
  readonly metadata?: JobMetadata<any>;
  readonly handler: (...args: any[]) => unknown;
  beforeRun?(input: any, context: JobContext): MaybePromise<void>;
  onSuccess?(output: any, context: JobContext): MaybePromise<void>;
  readonly onError?: (error: unknown, context: JobContext) => MaybePromise<void>;
};
type AnyJob = {
  (input: any): JobSubmission<unknown>;
  readonly definition: AnyDefinition;
};

type ResolvedMetadata = {
  readonly attempts: number;
  readonly backoff: JobPolicy["backoff"];
  readonly timeout: number;
  readonly concurrency: number;
  readonly key?: (input: unknown) => string;
  readonly idempotencyKey?: (input: unknown) => string;
};

/** Three attempts a second or so apart, a five-minute ceiling per attempt, and no dedupe unless keyed. */
export const defaultMetadata = Object.freeze({
  retries: Object.freeze({ attempts: 3, backoff: Object.freeze({ type: "exponential", delay: 1_000 }) }),
  timeout: 5 * 60_000,
  concurrency: Infinity,
} as const);

function resolveMetadata(metadata: JobMetadata<any> = {}): ResolvedMetadata {
  const attempts = metadata.retries?.attempts ?? defaultMetadata.retries.attempts;
  const backoff = metadata.retries?.backoff ?? defaultMetadata.retries.backoff;
  const resolved: ResolvedMetadata = {
    attempts,
    backoff: typeof backoff === "string" ? { type: backoff, delay: defaultMetadata.retries.backoff.delay } : backoff,
    timeout: metadata.timeout ?? defaultMetadata.timeout,
    concurrency: metadata.concurrency ?? defaultMetadata.concurrency,
    ...(metadata.key ? { key: metadata.key } : {}),
    ...(metadata.idempotencyKey !== undefined ? { idempotencyKey: metadata.idempotencyKey } : {}),
  };
  const invalid = (field: string, requirement: string) => new Error(`Job metadata ${field} must be ${requirement}`);
  if (!Number.isSafeInteger(resolved.attempts) || resolved.attempts < 1) throw invalid("retries.attempts", "a positive integer");
  if (!["fixed", "exponential"].includes(resolved.backoff.type)) throw invalid("retries.backoff.type", "fixed or exponential");
  if (!Number.isFinite(resolved.backoff.delay) || resolved.backoff.delay < 0) throw invalid("retries.backoff.delay", "a nonnegative number");
  if (Number.isNaN(resolved.timeout) || resolved.timeout <= 0) throw invalid("timeout", "positive");
  if (resolved.concurrency !== Infinity && (!Number.isSafeInteger(resolved.concurrency) || resolved.concurrency < 1)) {
    throw invalid("concurrency", "a positive integer or Infinity");
  }
  if (resolved.key !== undefined && typeof resolved.key !== "function") throw invalid("key", "a function of the input");
  if (resolved.idempotencyKey !== undefined && typeof resolved.idempotencyKey !== "function") throw invalid("idempotencyKey", "a function of the input");
  if (metadata.key !== undefined && metadata.idempotencyKey !== undefined) throw new Error("Choose key or idempotencyKey, not both");
  return Object.freeze(resolved);
}

// Attachment is the only link from a definition to its backend; closing a system releases it.
type Attachment = { owner: JobSystem; submit(input: unknown, timing?: Timing): JobSubmission<unknown> | Promise<ScheduleHandle> };
const attachments = new WeakMap<AnyJob, Attachment>();

function submission<Output>(pending: Promise<JobHandle<Output>>): JobSubmission<Output> {
  return Object.assign(pending, {
    result: (options?: WaitOptions) => pending.then((handle) => handle.result(options)),
  });
}

/** Define a callable job; its registration key supplies the identity used for delivery. */
export function defineJob<const Deps extends readonly Constructor[], Input, Output>(
  definition: JobDefinition<Deps, Input, Output>,
): Job<Deps, Input, Output> {
  resolveMetadata(definition.metadata);
  const frozen = Object.freeze({ ...definition, deps: Object.freeze([...definition.deps]) as unknown as Deps });
  const job = ((input: Input, timing?: Timing) => {
    const attachment = attachments.get(job);
    if (!attachment) {
      return submission(Promise.reject(new Error("Job is not attached to a job system; pass it to createJobSystem first")));
    }
    return attachment.submit(input, timing);
  }) as Job<Deps, Input, Output>;
  Object.defineProperty(job, "definition", { value: frozen, enumerable: true });
  return Object.freeze(job);
}

type Entry = { job: AnyJob; metadata: ResolvedMetadata };

/** Runs its catalog through one backend worker; definitions submit through it once attached. */
export class JobSystem {
  private readonly catalog = new Map<string, Entry>();
  private readonly backend: JobBackend;
  private readonly codec: JobCodec;
  private readonly concurrency: number;
  private readonly shutdown = new AbortController();
  private readonly executionShutdown = new AbortController();
  private readonly shutdownTimeoutMs: number;
  private worker?: Promise<JobWorker>;
  private closing?: Promise<void>;

  constructor(private readonly container: Container, jobs: Readonly<Record<string, AnyJob>>, options: { backend: JobBackend; codec?: JobCodec; concurrency?: number; worker?: boolean; shutdownTimeoutMs?: number }) {
    if (!options?.backend) throw new Error("A job backend is required");
    this.backend = options.backend;
    this.codec = options.codec ?? new JsonCodec();
    this.concurrency = options.concurrency ?? 1;
    this.shutdownTimeoutMs = options.shutdownTimeoutMs ?? 30_000;
    if (!Number.isSafeInteger(this.shutdownTimeoutMs) || this.shutdownTimeoutMs < 0 || this.shutdownTimeoutMs > 2_147_483_647) {
      throw new Error("shutdownTimeoutMs must be an integer between 0 and 2147483647");
    }
    if (options.worker !== undefined && typeof options.worker !== "boolean") throw new Error("worker must be a boolean");
    if (!Number.isSafeInteger(this.concurrency) || this.concurrency < 1) throw new Error("concurrency must be a positive safe integer");
    if (!jobs || typeof jobs !== "object" || Array.isArray(jobs) ||
      Object.getOwnPropertySymbols(jobs).some((key) => Object.prototype.propertyIsEnumerable.call(jobs, key))) {
      throw new Error("Jobs must be an object with string registration keys");
    }
    const registered = new Set<AnyJob>();
    for (const [name, job] of Object.entries(jobs)) {
      if (!name.trim()) throw new Error("Job registration keys must not be empty");
      if (typeof job !== "function" || !job.definition || typeof job.definition !== "object") throw new Error("Jobs must be created with defineJob");
      if (registered.has(job)) throw new Error(`Job registered more than once: ${name}`);
      registered.add(job);
      const owner = attachments.get(job)?.owner;
      if (owner && !owner.closing) throw new Error(`Job "${name}" is already attached to another job system`);
      const metadata = resolveMetadata(job.definition.metadata);
      this.catalog.set(name, { job, metadata });
    }
    // Validate the whole catalog before attaching any of it, so a rejected system attaches nothing.
    for (const [name, { job }] of this.catalog) {
      attachments.set(job, { owner: this, submit: (input, timing) => this.submit(name, input, timing) });
    }
    if (options.worker !== false) {
      this.worker = this.openWorker();
      // Startup can fail before any caller arrives; retain the failure for calls and shutdown.
      void this.worker.catch((error: unknown) => this.shutdown.abort(error));
    }
  }

  private submit(name: string, input: unknown, timing?: Timing): JobSubmission<unknown> | Promise<ScheduleHandle> {
    let when: Delivery | undefined;
    try { when = timing === undefined ? undefined : delivery(timing); }
    catch (error) { return submission(Promise.reject(error)); }
    const pending = (async (): Promise<JobHandle<unknown> | ScheduleHandle> => {
      this.assertOpen();
      const { metadata } = this.catalog.get(name)!;
      const key = metadata.key?.(input);
      const idempotencyKey = metadata.idempotencyKey?.(input);
      if (key !== undefined && (typeof key !== "string" || !key)) throw new Error(`Job "${name}" key must be a nonempty string`);
      if (metadata.idempotencyKey && (typeof idempotencyKey !== "string" || !idempotencyKey)) throw new Error(`Job "${name}" idempotencyKey must be a nonempty string`);
      // Tuple encoding prevents collisions between job names and user keys containing separators.
      const policy: JobPolicy = {
        attempts: metadata.attempts, backoff: metadata.backoff,
        ...(key !== undefined ? { key: JSON.stringify([name, key]) } : {}),
        ...(idempotencyKey !== undefined ? { idempotencyKey } : {}),
      };
      const message: JobMessage = prepareSubmission({ id: crypto.randomUUID(), name, input: this.codec.encode(input), policy,
        ...(when && "at" in when ? { availableAt: when.at } : {}),
      });
      await this.worker;
      this.assertOpen();
      if (when && "repeat" in when) {
        if (!this.backend.schedules) throw new Error("This backend does not support recurring schedules");
        const id = await this.backend.schedules.upsert(message, when.repeat);
        return this.schedule(id);
      }
      const id = await this.backend.submit(message);
      return Object.freeze({ id, result: (options?: WaitOptions) => this.result(id, options) });
    })();
    // The normalized timing selects the return type; callers never receive a schedule's result() method.
    return when && "repeat" in when ? pending as Promise<ScheduleHandle> : submission(pending as Promise<JobHandle<unknown>>);
  }

  /** Reopen a stored schedule by ID, including from a new process. */
  public schedule(id: string): ScheduleHandle {
    this.assertOpen();
    if (typeof id !== "string" || !id) throw new Error("A schedule ID is required");
    const schedules = this.backend.schedules;
    if (!schedules) throw new Error("This backend does not support recurring schedules");
    return Object.freeze({
      id,
      update: async (timing: Recurrence) => {
        this.assertOpen();
        const when = delivery(timing);
        if (!("repeat" in when)) throw new Error("A schedule requires recurring timing");
        await schedules.update(id, when.repeat);
      },
      remove: async () => { this.assertOpen(); await schedules.remove(id); },
    });
  }

  /** Inspect a stored execution by ID; no worker startup or local submission is needed. */
  public async get(id: string): Promise<JobRecord | null> {
    this.assertOpen();
    if (!this.backend.get) throw new Error("This backend does not support execution inspection");
    const record = await this.backend.get(id, { signal: this.shutdown.signal });
    if (!record) return null;
    const input = this.codec.decode(record.input);
    return record.status === "succeeded"
      ? { ...record, input, output: this.codec.decode(record.output) }
      : { ...record, input };
  }

  public close(): Promise<void> {
    if (this.closing) return this.closing;
    const draining = Promise.resolve().then(async () => {
      const errors: unknown[] = [];
      try { await (await this.worker)?.close(); }
      catch (error) { errors.push(error); }
      try { await this.backend.close(); }
      catch (error) { errors.push(error); }
      if (errors.length === 1) throw errors[0];
      if (errors.length > 1) throw new AggregateError(errors, "Worker and backend shutdown both failed");
    });
    this.closing = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        const error = new ShutdownTimeoutError(this.shutdownTimeoutMs);
        this.executionShutdown.abort(error);
        reject(error);
      }, this.shutdownTimeoutMs);
      // Drain in the background after expiry; never release a lease while handler cleanup is still running.
      void draining.then(
        () => { clearTimeout(timer); resolve(); },
        (error: unknown) => { clearTimeout(timer); reject(error); },
      );
    });
    this.shutdown.abort(new Error("Job system is closed"));
    for (const { job } of this.catalog.values()) {
      if (attachments.get(job)?.owner === this) attachments.delete(job);
    }
    return this.closing;
  }

  private async result(id: string, options?: WaitOptions): Promise<unknown> {
    this.assertOpen();
    const signal = options?.signal ? AbortSignal.any([options.signal, this.shutdown.signal]) : this.shutdown.signal;
    signal.throwIfAborted();
    const outcome = await this.backend.result(id, { signal });
    if (outcome.status === "failed") throw new JobExecutionError(id, outcome.error);
    return this.codec.decode(outcome.output);
  }

  private assertOpen(): void {
    if (this.closing) throw new Error("Job system is closed");
    this.shutdown.signal.throwIfAborted();
  }

  private async openWorker(): Promise<JobWorker> {
    const concurrencyByJob = Object.fromEntries([...this.catalog]
      .filter(([, entry]) => Number.isFinite(entry.metadata.concurrency))
      .map(([name, entry]) => [name, entry.metadata.concurrency]));
    const worker = await this.backend.work(
      (message, signal, attempt) => this.execute(message, signal, attempt),
      { concurrency: this.concurrency, concurrencyByJob },
    );
    // Observe terminal worker failure once, so pending and future submissions cannot wait on a dead worker.
    void worker.done.then(
      () => this.shutdown.abort(new Error("Job worker stopped")),
      (error: unknown) => this.shutdown.abort(error),
    );
    return worker;
  }

  private async execute(message: JobMessage, workerSignal: AbortSignal, attempt: number): Promise<JobOutcome> {
    if (this.closing) throw new JobInterruptedError();
    const entry = this.catalog.get(message.name);
    if (!entry) return { status: "failed", error: failure(new Error(`Unknown job: ${message.name}`)), retryable: false };
    const { metadata } = entry;
    const job = entry.job.definition;
    workerSignal = AbortSignal.any([workerSignal, this.executionShutdown.signal]);
    const signal = Number.isFinite(metadata.timeout)
      ? AbortSignal.any([workerSignal, AbortSignal.timeout(metadata.timeout)])
      : workerSignal;
    const context: JobContext = Object.freeze({
      jobId: message.id, name: message.name, signal, attempt, maxAttempts: message.policy.attempts,
    });
    let input: unknown;
    try {
      input = this.codec.decode(message.input);
    } catch (error) {
      return { status: "failed", error: failure(error), retryable: false };
    }
    let output: unknown;
    try {
      signal.throwIfAborted();
      const scope = this.container.createScope();
      const dependencies = job.deps.map((dependency) => scope.resolve(dependency));
      await job.beforeRun?.(input, context);
      signal.throwIfAborted();
      output = await job.handler(input, ...dependencies, signal);
      signal.throwIfAborted();
    } catch (thrown) {
      if (this.executionShutdown.signal.aborted && signal.reason === this.executionShutdown.signal.reason) {
        throw new JobInterruptedError();
      }
      // An aborted signal is the true cause; handlers surface it through library-specific errors.
      const error = signal.aborted ? signal.reason : thrown;
      const retryable = !(error instanceof NonRetryableError);
      try { await job.onError?.(error, context); }
      catch (hookError) {
        return { status: "failed", retryable, error: failure(new AggregateError([error, hookError],
          `${failure(error).message}; onError also failed: ${failure(hookError).message}`)) };
      }
      return { status: "failed", error: failure(error), retryable };
    }
    try {
      // Post-success hook/encoding failures are terminal, never a reason to repeat handler effects.
      await job.onSuccess?.(output, context);
      signal.throwIfAborted();
      return { status: "succeeded", output: this.codec.encode(output) };
    } catch (error) {
      return { status: "failed", error: failure(signal.aborted ? signal.reason : error), retryable: false };
    }
  }
}

/** Attaches jobs and starts consuming immediately; close the system during app shutdown. */
export function createJobSystem(options: {
  container?: Container;
  jobs: Readonly<Record<string, AnyJob>>;
  backend: JobBackend;
  codec?: JobCodec;
  concurrency?: number;
  /** Grace period for close, including startup and resource cleanup; defaults to 30 seconds. */
  shutdownTimeoutMs?: number;
  /** False submits to remote workers without starting a local worker or resolving dependencies. */
  worker?: boolean;
}): JobSystem {
  return new JobSystem(options.container ?? new Container(), options.jobs, options);
}

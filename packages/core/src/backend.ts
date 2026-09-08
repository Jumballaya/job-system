/** Delivery policy fixed at submission; backends enforce it, core decides retryability per attempt. */
export interface JobPolicy {
  /** Total attempts including the first; at least 1. */
  readonly attempts: number;
  readonly backoff: { readonly type: "fixed" | "exponential"; readonly delay: number };
  /** While a job with this key is queued or running, submitting again returns that job's ID. */
  readonly key?: string;
}

export interface JobMessage {
  readonly id: string;
  readonly name: string;
  readonly input: string;
  readonly policy: JobPolicy;
}

export interface JobFailure {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
}

/** A retryable failure before the final attempt is redelivered after `backoffDelay`; otherwise it is retained. */
export type JobOutcome =
  | { readonly status: "succeeded"; readonly output: string }
  | { readonly status: "failed"; readonly error: JobFailure; readonly retryable: boolean };

export interface WaitOptions {
  /** Aborts waiting only; accepted work may still execute. */
  readonly signal?: AbortSignal;
}

export interface WorkerOptions {
  readonly concurrency?: number;
}

export interface JobWorker {
  /** Resolves after shutdown; rejects if the worker cannot continue. */
  readonly done: Promise<void>;
  /** Stop intake and drain active executions. Repeated calls are harmless. */
  close(): Promise<void>;
}

/** `attempt` starts at 1 and counts every delivery of the same message. */
export type JobExecutor = (message: JobMessage, signal: AbortSignal, attempt: number) => Promise<JobOutcome>;

/** Own delivery and retained outcomes. Recover infrastructure failures without promising exactly-once effects. */
export interface JobBackend {
  /** Accept the message and return the ID to wait on: the message's own, or an active job sharing its key. */
  submit(message: JobMessage): Promise<string>;
  /** Return a retained or future outcome; reject if unknown/expired, aborted, or closed. */
  result(id: string, options?: WaitOptions): Promise<JobOutcome>;
  /** Ready on return. Failed outcomes are terminal or retried per policy; executor rejection is infrastructure failure. */
  work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker>;
  /** Drain owned workers, reject pending waits, and release owned connections. */
  close(): Promise<void>;
}

/** Milliseconds to wait after the given failed attempt before the next delivery. */
export function backoffDelay(policy: JobPolicy, attempt: number): number {
  const { type, delay } = policy.backoff;
  return type === "exponential" ? Math.round(delay * 2 ** (attempt - 1)) : delay;
}

/** Throw from a handler to fail immediately regardless of remaining attempts. */
export class NonRetryableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "NonRetryableError";
  }
}

export class ResultUnavailableError extends Error {
  constructor(public readonly jobId: string) {
    super(`Job result is unknown or expired: ${jobId}`);
    this.name = "ResultUnavailableError";
  }
}

export class JobExecutionError extends Error {
  constructor(public readonly jobId: string, public readonly failure: JobFailure) {
    super(failure.message);
    this.name = "JobExecutionError";
  }
}

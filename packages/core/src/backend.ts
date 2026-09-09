import { createHash } from "node:crypto";
import type { JobSchedules } from "./scheduling.js";

/** Delivery policy fixed at submission; backends enforce it, core decides retryability per attempt. */
export interface JobPolicy {
  /** Total attempts including the first; at least 1. */
  readonly attempts: number;
  readonly backoff: { readonly type: "fixed" | "exponential"; readonly delay: number };
  /** While a job with this key is queued or running, submitting again returns that job's ID. */
  readonly key?: string;
  /** Stable operation key, scoped by name. Retain its identity, input, and terminal outcome without expiry. */
  readonly idempotencyKey?: string;
}

export interface JobMessage {
  readonly id: string;
  readonly name: string;
  readonly input: string;
  readonly policy: JobPolicy;
  /** Earliest first delivery, as Unix milliseconds; retry backoff is independent. */
  readonly availableAt?: number;
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

/** Execution snapshot; timestamps are Unix milliseconds and startedAt describes the latest delivery. */
export type JobRecord<Input = unknown, Output = unknown> = {
  readonly id: string;
  readonly name: string;
  readonly input: Input;
  /** Completed attempts plus the current active attempt; capacity waits and handoffs spend none. */
  readonly attempts: number;
  readonly createdAt: number;
} & (
  | { readonly status: "queued"; readonly startedAt?: number }
  | { readonly status: "running"; readonly startedAt: number }
  | { readonly status: "succeeded"; readonly startedAt: number; readonly finishedAt: number; readonly output: Output }
  | { readonly status: "failed"; readonly startedAt: number; readonly finishedAt: number; readonly error: JobFailure }
);

export interface WaitOptions {
  /** Aborts waiting only; accepted work may still execute. */
  readonly signal?: AbortSignal;
}

export interface WorkerOptions {
  readonly concurrency?: number;
  /** Per-registration limits within this worker. Saturated jobs wait without occupying execution slots. */
  readonly concurrencyByJob?: Readonly<Record<string, number>>;
}

export interface JobWorker {
  /** Resolves after shutdown; rejects if the worker cannot continue. */
  readonly done: Promise<void>;
  /** Stop intake and drain active executions. Repeated calls are harmless. */
  close(): Promise<void>;
}

/** `attempt` starts at 1. JobInterruptedError returns settled work to the queue without spending an attempt. */
export type JobExecutor = (message: JobMessage, signal: AbortSignal, attempt: number) => Promise<JobOutcome>;

/** Own delivery and retained outcomes. Recover infrastructure failures without promising exactly-once effects. */
export interface JobBackend {
  /** Optional recurring-schedule capability; ordinary jobs need only the delivery methods below. */
  readonly schedules?: JobSchedules;
  /** Inspect without waiting for completion; null means unknown or expired. Payloads remain encoded. */
  get?(id: string, options?: WaitOptions): Promise<JobRecord<string, string> | null>;
  /** Accept or replay work; idempotency keys reject changed inputs and survive result cleanup. */
  submit(message: JobMessage): Promise<string>;
  /** Return a retained or future outcome; reject if unknown/expired, aborted, or closed. */
  result(id: string, options?: WaitOptions): Promise<JobOutcome>;
  /** Ready on return. Failed outcomes are terminal or retried per policy; executor rejection is infrastructure failure. */
  work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker>;
  /** Drain owned workers, reject pending waits, and release owned connections. */
  close(): Promise<void>;
}

/** Snapshot policy and derive durable identity before the backend accepts any work. */
export function prepareSubmission(message: JobMessage): JobMessage {
  if (message.availableAt !== undefined && (!Number.isSafeInteger(message.availableAt) ||
    !Number.isFinite(new Date(message.availableAt).getTime()))) throw new Error("Job availableAt must be a valid timestamp");
  const { key, idempotencyKey } = message.policy;
  if (idempotencyKey !== undefined && (typeof idempotencyKey !== "string" || !idempotencyKey)) {
    throw new Error("Job idempotencyKey must be a nonempty string");
  }
  if (key !== undefined && idempotencyKey !== undefined) {
    throw new Error("Choose key for active coalescing or idempotencyKey for durable replay, not both");
  }
  const id = idempotencyKey === undefined ? message.id
    : `operation-${createHash("sha256").update(JSON.stringify([message.name, idempotencyKey])).digest("hex")}`;
  return Object.freeze({ ...message, id, policy: Object.freeze({ ...message.policy, backoff: Object.freeze({ ...message.policy.backoff }) }) });
}

export function assertSameSubmission(stored: JobMessage, incoming: JobMessage): void {
  if (stored.name === incoming.name && stored.input === incoming.input &&
    stored.policy.idempotencyKey === incoming.policy.idempotencyKey) return;
  if (incoming.policy.idempotencyKey !== undefined) throw new IdempotencyConflictError(incoming.id);
  throw new Error(`Job ID already belongs to another submission: ${incoming.id}`);
}

export class IdempotencyConflictError extends Error {
  constructor(public readonly jobId: string) {
    super(`Idempotency key already belongs to a different submission: ${jobId}`);
    this.name = "IdempotencyConflictError";
  }
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

/** Executor control flow: work and cleanup have settled; preserve identity and attempts for another worker. */
export class JobInterruptedError extends Error {
  constructor() {
    super("Worker stopped before execution completed");
    this.name = "JobInterruptedError";
  }
}

/** Close exceeded its grace period; cancellation was requested, but live work may still own resources. */
export class ShutdownTimeoutError extends Error {
  constructor(public readonly timeoutMs: number) {
    super(`Job system shutdown exceeded ${timeoutMs} ms; unfinished work is still draining`);
    this.name = "ShutdownTimeoutError";
  }
}

export function failure(error: unknown): JobFailure {
  if (error instanceof Error) return { name: error.name, message: error.message, ...(error.stack ? { stack: error.stack } : {}) };
  return { name: "Error", message: typeof error === "string" ? error : "Job failed with a non-Error value" };
}

export { ConcurrencyLimits } from "./concurrency.js";

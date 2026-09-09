import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { setTimeout as pause } from "node:timers/promises";
import { Context } from "@temporalio/activity";
import { ScheduleAlreadyRunning, ScheduleNotFoundError, WorkflowExecutionAlreadyStartedError, WorkflowNotFoundError } from "@temporalio/client";
import type { Client, ScheduleSpec } from "@temporalio/client";
import { Worker } from "@temporalio/worker";
import type { WorkerOptions as TemporalWorkerOptions } from "@temporalio/worker";
import { assertSameSubmission, failure, prepareSubmission, ResultUnavailableError } from "core/backend";
import type { JobBackend, JobExecutor, JobMessage, JobOutcome, JobRecord, JobWorker, WaitOptions, WorkerOptions } from "core";
import { scheduleId, validateRule } from "core/scheduling";
import type { JobSchedules, ScheduleRule } from "core";
import { ACTIVITY, INSPECT, STARTED, WORKFLOW } from "./protocol.js";
import type { ActivityResult, Snapshot } from "./protocol.js";

export interface TemporalBackendOptions {
  /** Borrowed; close it during application shutdown after the job system closes. */
  client: Client;
  taskQueue: string;
  /** Existing native connection, workflow bundle, activities, interceptors, and other worker configuration. */
  worker?: Omit<TemporalWorkerOptions, "taskQueue" | "namespace">;
}

const digest = (value: string) => createHash("sha256").update(value).digest("hex");

/** Temporal owns durable delivery. Each idempotency key keeps a compact, open workflow as its operation ledger. */
export class TemporalBackend implements JobBackend {
  private readonly shutdown = new AbortController();
  private readonly prefix: string;
  private worker?: Promise<JobWorker>;
  private closing?: Promise<void>;
  readonly schedules: JobSchedules;

  constructor(private readonly options: TemporalBackendOptions) {
    if (!options.taskQueue?.trim()) throw new Error("A Temporal taskQueue is required");
    this.prefix = `jobs-${digest(options.taskQueue).slice(0, 16)}-`;
    this.schedules = {
      upsert: (message, rule) => this.request(async () => {
        message = prepareSubmission(message);
        rule = validateRule(rule);
        const id = this.prefix + scheduleId(message);
        // Temporal assigns a distinct workflow ID to each occurrence; ordinary coalescing must not absorb it.
        const { key, idempotencyKey, ...policy } = message.policy;
        const occurrence = { ...message, policy };
        try {
          await options.client.schedule.create({
            scheduleId: id, spec: this.spec(rule),
            action: { type: "startWorkflow", workflowType: WORKFLOW, taskQueue: options.taskQueue,
              args: [occurrence], memo: { jobSystemMessage: occurrence, jobSystemRule: rule } },
            policies: { overlap: "ALLOW_ALL" },
          });
        } catch (error) {
          if (!(error instanceof ScheduleAlreadyRunning)) throw error;
          await this.setRule(id, rule);
        }
        return id;
      }),
      update: (id, rule) => this.request(() => this.setRule(id, validateRule(rule))),
      remove: (id) => this.request(async () => {
        try { await options.client.schedule.getHandle(id).delete(); }
        catch (error) { if (!(error instanceof ScheduleNotFoundError)) throw error; }
      }),
    };
  }

  async submit(submitted: JobMessage): Promise<string> {
    const message = prepareSubmission(submitted);
    return this.request(async () => {
      const coalescing = message.policy.key !== undefined;
      const workflowId = this.prefix + (coalescing ? `key-${digest(message.policy.key!)}` : message.id);
      try {
        const handle = await this.options.client.workflow.start(WORKFLOW, {
          workflowId, taskQueue: this.options.taskQueue, args: [message], memo: { jobSystemMessage: message },
          workflowIdReusePolicy: coalescing ? "ALLOW_DUPLICATE" : "REJECT_DUPLICATE",
          workflowIdConflictPolicy: "USE_EXISTING",
        });
        if (!coalescing) assertSameSubmission((await handle.describe()).memo?.jobSystemMessage as JobMessage, message);
        return message.policy.idempotencyKey !== undefined ? workflowId : `${workflowId}/${handle.firstExecutionRunId}`;
      } catch (error) {
        if (!(error instanceof WorkflowExecutionAlreadyStartedError)) throw error;
        const existing = await this.options.client.workflow.getHandle(workflowId).describe();
        if (!coalescing) assertSameSubmission(existing.memo?.jobSystemMessage as JobMessage, message);
        return message.policy.idempotencyKey !== undefined ? workflowId : `${workflowId}/${existing.runId}`;
      }
    });
  }

  result(id: string, options?: WaitOptions): Promise<JobOutcome> {
    return this.request(async (signal) => {
      const handle = this.handle(id);
      if (id.includes("/")) return await handle.result() as JobOutcome;
      for (;;) {
        const snapshot = await handle.query<Snapshot>(INSPECT);
        if (snapshot.outcome) return snapshot.outcome;
        await pause(100, undefined, { signal });
      }
    }, options, id);
  }

  get(id: string, options?: WaitOptions): Promise<JobRecord<string, string> | null> {
    return this.request(async () => {
      try {
        let handle = this.handle(id);
        let description = await handle.describe();
        while (description.status.name === "CONTINUED_AS_NEW") {
          const history = await handle.fetchHistory();
          const next = history.events?.at(-1)?.workflowExecutionContinuedAsNewEventAttributes?.newExecutionRunId;
          if (!next) throw new Error("Continued workflow has no successor");
          handle = this.options.client.workflow.getHandle(description.workflowId, next);
          description = await handle.describe();
        }
        const message = description.memo?.jobSystemMessage as JobMessage | undefined;
        if (!message) throw new Error("Workflow does not belong to this job backend");
        const record: JobRecord<string, string> = description.historyLength <= 2
          ? { id, name: message.name, input: message.input, status: "queued", attempts: 0, createdAt: description.startTime.getTime() }
          : (await handle.query<Snapshot>(INSPECT)).record;
        if (record.status === "succeeded" || record.status === "failed") return record;
        if (["FAILED", "CANCELLED", "TERMINATED", "TIMED_OUT"].includes(description.status.name)) {
          try { await handle.result(); }
          catch (error) {
            return { ...record, status: "failed", startedAt: record.startedAt ?? record.createdAt,
              finishedAt: description.closeTime!.getTime(), error: failure(error) };
          }
        }
        return record;
      } catch (error) {
        if (error instanceof WorkflowNotFoundError || error instanceof ResultUnavailableError) return null;
        throw error;
      }
    }, options);
  }

  work(execute: JobExecutor, options: WorkerOptions = {}): Promise<JobWorker> {
    this.shutdown.signal.throwIfAborted();
    if (this.worker) throw new Error("TemporalBackend already has a worker");
    const concurrency = options.concurrency ?? 1;
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) throw new Error("concurrency must be a positive safe integer");
    const limits = new Map(Object.entries(options.concurrencyByJob ?? {}));
    for (const limit of limits.values()) {
      if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("Job concurrency must be a positive safe integer");
    }
    if (!this.options.worker?.connection) throw new Error("Temporal workers require worker.connection; use worker: false for a producer");
    if (this.options.worker.activities && ACTIVITY in this.options.worker.activities) throw new Error(`Activity name reserved: ${ACTIVITY}`);
    const active = new Map<string, number>();
    const jobSystemExecute = async (message: JobMessage, attempt: number): Promise<ActivityResult> => {
      const count = active.get(message.name) ?? 0;
      if (count >= (limits.get(message.name) ?? Infinity)) return { busy: true };
      active.set(message.name, count + 1);
      const context = Context.current();
      const heartbeat = setInterval(() => context.heartbeat(), 5_000);
      try {
        const workflow = context.info.workflowExecution!;
        await this.options.client.workflow.getHandle(workflow.workflowId, workflow.runId).signal(STARTED, attempt, Date.now());
        return { outcome: await execute(message, context.cancellationSignal, attempt) };
      } finally {
        clearInterval(heartbeat);
        const remaining = active.get(message.name)! - 1;
        if (remaining === 0) active.delete(message.name);
        else active.set(message.name, remaining);
      }
    };
    this.worker = (async () => {
      const configured = this.options.worker!;
      const worker = await Worker.create({
        ...configured,
        ...(!configured.workflowBundle && !configured.workflowsPath
          ? { workflowsPath: fileURLToPath(new URL("./workflows.js", import.meta.url)) } : {}),
        namespace: this.options.client.options.namespace, taskQueue: this.options.taskQueue,
        activities: { ...configured.activities, jobSystemExecute },
        maxConcurrentActivityTaskExecutions: concurrency,
        shutdownGraceTime: configured.shutdownGraceTime ?? "365 days",
      });
      const done = worker.run();
      // Observe immediately, including when close races with startup.
      void done.catch(() => {});
      let closing: Promise<void> | undefined;
      return { done, close: () => closing ??= (async () => {
        if (worker.getState() === "RUNNING") worker.shutdown();
        await done;
      })() };
    })();
    return this.worker;
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.shutdown.abort(new Error("Temporal backend is closed"));
    return this.closing = (async () => { await (await this.worker)?.close(); })();
  }

  private handle(id: string) {
    if (!id.startsWith(this.prefix)) throw new ResultUnavailableError(id);
    const [workflowId, runId] = id.split("/");
    return this.options.client.workflow.getHandle(workflowId!, runId);
  }

  private spec(rule: ScheduleRule): ScheduleSpec {
    if ("every" in rule && rule.every % 1_000 !== 0) throw new Error("Temporal schedule intervals must use whole seconds");
    // Anchor intervals at registration, so the first occurrence is one full interval away.
    return "every" in rule
      ? { intervals: [{ every: rule.every, offset: Date.now() % rule.every }], startAt: new Date(Date.now() + rule.every) }
      : { cronExpressions: [rule.cron], timezone: rule.timezone };
  }

  private setRule(id: string, rule: ScheduleRule): Promise<void> {
    const spec = this.spec(rule);
    return this.options.client.schedule.getHandle(id).update((current) => {
      // Re-registration must not reset the next occurrence of an unchanged interval.
      if (JSON.stringify(current.action.memo?.jobSystemRule) === JSON.stringify(rule)) return current;
      return { ...current, spec, action: { ...current.action, memo: { ...current.action.memo, jobSystemRule: rule } } };
    });
  }

  private async request<T>(run: (signal: AbortSignal) => Promise<T>, options?: WaitOptions, id?: string): Promise<T> {
    const signal = options?.signal ? AbortSignal.any([options.signal, this.shutdown.signal]) : this.shutdown.signal;
    signal.throwIfAborted();
    try { return await this.options.client.connection.withAbortSignal(signal, () => run(signal)); }
    catch (error) {
      signal.throwIfAborted();
      if (id && error instanceof WorkflowNotFoundError) throw new ResultUnavailableError(id);
      throw error;
    }
  }
}

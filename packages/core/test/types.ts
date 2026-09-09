import { Container, createJobSystem, defineJob, MemoryBackend } from "../dist/index.js";
import type { JobHandle, JobRecord, ScheduleHandle, Timing } from "../dist/index.js";

class Counter {
  add(amount: number): number { return amount; }
}

class Required {
  constructor(readonly name: string) {}
}
class Inherited extends Required {}

const container = new Container().register(Counter);
container.register(Required, { useFactory: () => new Required("ready") });
// @ts-expect-error Required constructor inputs must be supplied by a provider.
container.register(Required);
// @ts-expect-error Inherited required inputs remain required.
container.register(Inherited);
// @ts-expect-error Factory results must match the class instance.
container.register(Required, { useFactory: () => ({ wrong: true }) });
// @ts-expect-error Provider strategies are mutually exclusive.
container.register(Counter, { useValue: new Counter(), useFactory: () => new Counter() });

const add = defineJob({
  deps: [Counter],
  async handler(input: { amount: number }, counter, signal) {
    const abortSignal: AbortSignal = signal;
    abortSignal.throwIfAborted();
    return counter.add(input.amount);
  },
  onSuccess(output, context) {
    const result: number = output;
    const id: string = context.jobId;
    void result;
    void id;
  },
});
const label = defineJob({
  deps: [],
  metadata: {
    retries: { attempts: 5, backoff: "fixed" },
    timeout: 1_000,
    concurrency: 2,
    key: (input) => input.toLowerCase(),
  },
  handler: (input: string) => input.toUpperCase(),
});
defineJob({
  deps: [],
  // @ts-expect-error The key derives from the declared input type.
  metadata: { key: (input: number) => String(input) },
  handler: (input: string) => input,
});
defineJob({
  deps: [],
  // @ts-expect-error Metadata has a fixed set of fields.
  metadata: { priority: 1 },
  handler: (input: string) => input,
});
const jobs = createJobSystem({ container, jobs: { add, label }, backend: new MemoryBackend() });
const inspection: Promise<JobRecord | null> = jobs.get("persisted-id");
async function inspect() {
  const record = await inspection;
  if (record?.status === "failed") {
    const message: string = record.error.message;
    // @ts-expect-error A failed execution has no successful output.
    record.output;
    return message;
  }
  if (record?.status === "succeeded") {
    const output: unknown = record.output;
    // @ts-expect-error An arbitrary persisted ID cannot promise this catalog's current output type.
    const assumed: number = record.output;
    return output;
  }
}
void inspect;

defineJob({
  deps: [Counter],
  handler: (input: { operationId: string; amount: number }, counter) => counter.add(input.amount),
  metadata: { idempotencyKey: (input) => input.operationId },
});
defineJob({
  deps: [],
  handler: (input: string) => input,
  // @ts-expect-error Coalescing cannot discard a distinct durable operation.
  metadata: { key: () => "active", idempotencyKey: () => "durable" },
});
defineJob({
  deps: [],
  handler: (input: string) => input,
  // @ts-expect-error Operation keys must derive from the declared input.
  metadata: { idempotencyKey: (input: number) => String(input) },
});

const handle: Promise<JobHandle<number>> = add({ amount: 1 });
const count: Promise<number> = add({ amount: 1 }).result();
const text: Promise<string> = label("hello").result();
const delayed: Promise<number> = add({ amount: 1 }, { after: "10m" }).result();
const dated: Promise<JobHandle<number>> = add({ amount: 1 }, { at: new Date() });
const recurring: Promise<ScheduleHandle> = add({ amount: 1 }, { every: "1h" });
const daily: Promise<ScheduleHandle> = add({ amount: 1 }, { daily: "09:00", timezone: "America/New_York" });
const calendar: Promise<ScheduleHandle> = add({ amount: 1 }, { cron: "0 9 * * 1-5", timezone: "UTC" });
const timing: Timing = Math.random() > 0.5 ? { after: "1s" } : { every: "1h" };
const timed: Promise<JobHandle<number> | ScheduleHandle> = add({ amount: 1 }, timing);
const schedule: ScheduleHandle = jobs.schedule("persisted-schedule-id");
schedule.update({ every: "30m" });
schedule.remove();
// @ts-expect-error A recurrence has no single execution result.
add({ amount: 1 }, { every: "1h" }).result();
// @ts-expect-error Timing modes remain exclusive even when passed through a variable.
add({ amount: 1 }, { after: "1m", every: "1h" } as const);
// @ts-expect-error Calendar schedules require an explicit timezone.
add({ amount: 1 }, { daily: "09:00" });
// @ts-expect-error Durations need units.
add({ amount: 1 }, { every: 1_000 });
// @ts-expect-error Updating a recurring schedule requires recurring timing.
schedule.update({ after: "1h" });
void [delayed, dated, recurring, daily, calendar, timed];
// @ts-expect-error Inputs stay paired with their job.
add("wrong");
// @ts-expect-error The handler determines the awaited output.
const wrongOutput: Promise<string> = add({ amount: 1 }).result();
// @ts-expect-error There is no name-based submission API.
jobs.run("counter.add", { amount: 1 });
// @ts-expect-error Initialization starts the worker; there is no separate start step.
jobs.start();
const producer = createJobSystem({ jobs: { add }, backend: new MemoryBackend(), worker: false, shutdownTimeoutMs: 5_000 });
// @ts-expect-error Worker selection must be boolean.
createJobSystem({ jobs: { add }, backend: new MemoryBackend(), worker: "off" });
void producer;
void [handle, count, text, wrongOutput];

// @ts-expect-error Only definitions from defineJob form a catalog.
createJobSystem({ container, jobs: { add, invalid: () => {} }, backend: new MemoryBackend() });
// @ts-expect-error A backend must be selected explicitly.
createJobSystem({ container, jobs: { add } });

// @ts-expect-error Catalog keys provide identity; array registration is unsupported.
createJobSystem({ container, jobs: [add], backend: new MemoryBackend() });
defineJob({
  // @ts-expect-error Job identity belongs to registration, not the definition.
  name: "extra-name",
  deps: [],
  handler: (input: string) => input,
});

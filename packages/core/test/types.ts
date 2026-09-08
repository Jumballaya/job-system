import { Container, createJobSystem, defineJob, MemoryBackend } from "../dist/index.js";
import type { JobHandle } from "../dist/index.js";

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
  name: "counter.add",
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
  name: "label",
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
  name: "typed-key",
  deps: [],
  // @ts-expect-error The key derives from the declared input type.
  metadata: { key: (input: number) => String(input) },
  handler: (input: string) => input,
});
defineJob({
  name: "unknown-field",
  deps: [],
  // @ts-expect-error Metadata has a fixed set of fields.
  metadata: { priority: 1 },
  handler: (input: string) => input,
});
const jobs = createJobSystem({ container, jobs: [add, label], backend: new MemoryBackend() });

const name: "counter.add" = add.name;
const handle: Promise<JobHandle<number>> = add({ amount: 1 });
const count: Promise<number> = add({ amount: 1 }).result();
const text: Promise<string> = label("hello").result();
// @ts-expect-error Inputs stay paired with their job.
add("wrong");
// @ts-expect-error The handler determines the awaited output.
const wrongOutput: Promise<string> = add({ amount: 1 }).result();
// @ts-expect-error There is no name-based submission API.
jobs.run("counter.add", { amount: 1 });
// @ts-expect-error Worker startup is internal.
jobs.start();
void [name, handle, count, text, wrongOutput];

// @ts-expect-error Only definitions from defineJob form a catalog.
createJobSystem({ container, jobs: [add, () => {}], backend: new MemoryBackend() });
// @ts-expect-error A backend must be selected explicitly.
createJobSystem({ container, jobs: [add] });

# Job system

A lightweight TypeScript library for class-based dependency injection and typed jobs
with swappable backends. Requires Node.js 24 and pnpm 11.18.0. Core includes memory and Redis strategies;
the Redis strategy uses BullMQ.

```sh
pnpm install
pnpm test
pnpm start
```

`pnpm build` compiles core and app. `pnpm start` runs the app demo against Redis
on `127.0.0.1:6379` (`docker compose up -d` starts one); see "Assemble the system"
for the scenarios it walks through and the environment switches.

## Define jobs separately

```ts
import { defineJob } from "core";
import { MemoryStore, EmbeddingClient } from "./services.js";

type UpdateMemoryInput = { id: string; text: string };

export const updateMemory = defineJob({
  deps: [MemoryStore, EmbeddingClient],

  async handler(input: UpdateMemoryInput, memory, embeddings, signal) {
    const vector = await embeddings.embed(input.text, { signal });
    return memory.save(input.id, vector);
  },

  async onSuccess(output, context) {
    console.log(context.jobId, output);
  },

  onError(error, context) {
    console.error(context.name, error);
  },
});
```

Handler arguments are input, dependency instances in declaration order, then an
`AbortSignal`. Annotate the input; dependency and output types are inferred without
`as const`. Use `deps: []` for jobs without dependencies. Declare the handler before
`metadata` and the lifecycle hooks for contextual type inference; a `key` function
or hook declared earlier may need its parameter annotated due to TypeScript's
inference order.

`defineJob` snapshots and freezes the definition and dependency list, and returns a
callable job. It does not register services or start work. Calling the job submits
work through whichever system it is attached to; a job belongs to one open system
at a time and is released when that system closes.

## Job metadata

Every job carries a policy. The defaults are meant for production and apply when
`metadata` is omitted:

```ts
export const sendEmail = defineJob({
  deps: [Mailer],

  async handler(input: { userId: string }, mailer, signal) { ... },

  metadata: {
    retries: { attempts: 3, backoff: { type: "exponential", delay: 1_000 } },
    timeout: 5 * 60_000,
    concurrency: Infinity,
    key: (input) => `email:${input.userId}`,
  },
});
```

| Field | Default | Meaning |
| --- | --- | --- |
| `retries.attempts` | 3 | Total deliveries including the first. |
| `retries.backoff` | exponential from 1 s | Wait after a failed attempt: 1 s, then 2 s. `"fixed"` or `"exponential"` uses the default delay. |
| `timeout` | 5 minutes | Requests cancellation from delivery onward; active work must settle before failure/retry. `Infinity` disables. |
| `concurrency` | unlimited | Simultaneous executions of this job in one process, within the system's own limit. A waiting execution holds its worker slot. |
| `key` | none | Derives a dedupe key from the input. Calling the job while a job with that key is queued or running returns the active job's handle instead of a new one. |

Any thrown error is retried until attempts run out, except `NonRetryableError`,
exported from `core`, which fails immediately. Failures after the handler
succeeded, such as an `onSuccess` hook error, never retry. Handlers with external
effects should be idempotent per `jobId`, because a retry repeats the whole handler.
Invalid metadata is rejected by `defineJob`.

## Assemble the system

```ts
import { Container, createJobSystem, MemoryBackend } from "core";
import { MemoryStore, EmbeddingClient } from "./services.js";
import { updateMemory } from "./jobs/update-memory.js";

const container = new Container();
container.register(MemoryStore);
container.register(EmbeddingClient);

const jobs = createJobSystem({ container, jobs: { updateMemory }, backend: new MemoryBackend() });
```

Registration keys are the stable delivery names: `{ updateMemory }` uses
`"updateMemory"` for Redis messages and `context.name`. Definitions need no `name`
field. Producers and workers must use the same registration keys; object property
order does not matter. Renaming a key changes the identity of queued work.
Deduplication keys are automatically scoped to that registration key.

That is the whole composition root. Anywhere else, import the job and call it:

```ts
import { updateMemory } from "./jobs/update-memory.js";

void updateMemory({ id: "one", text: "hello" });                    // fire and forget
const vector = await updateMemory({ id: "two", text: "hi" }).result(); // wait for the output
const handle = await updateMemory({ id: "three", text: "hey" });      // accepted; read later
console.log(handle.id, await handle.result());
```

A call resolves once the backend has accepted the work and rejects if submission
fails, so a discarded call still surfaces submission errors as unhandled rejections.
`result()` waits for the handler's output after lifecycle hooks finish and can be
read repeatedly while the backend retains it. Input and output types come from the
definition; a mismatched input is a compile error. Calling a job that is not
attached to an open system rejects. Registering the same job under multiple keys
or attaching it to another open system is rejected at setup. Runtime input validation is the
application's responsibility when accepting external data. The catalog is fixed at
creation; changing the original registration object does not change it.

The app is a walkthrough of the current behavior over Redis: shared singleton and
per-execution scoped dependencies, fire-and-forget calls, concurrency, a caught
application failure, retries with backoff, a dedupe key, a cancelled wait, codec
rejection, and shutdown. Jobs live in
[`packages/app/src/jobs`](packages/app/src/jobs), services in
[`packages/app/src/services`](packages/app/src/services), and the scenarios in
[`index.ts`](packages/app/src/index.ts). Start Redis with `docker compose up -d`,
then `pnpm start`. Set `JOB_BACKEND=memory` to run the same scenarios in process;
`REDIS_HOST`, `REDIS_PORT`, and `JOB_QUEUE` override the connection.

## Dependency injection

Classes are runtime identities. There are no tokens or string dependency keys.
All dependencies must be registered explicitly. Classes with required constructor
arguments need a value or a factory; TypeScript checks inherited arguments too:

```ts
class Config {
  constructor(readonly prefix: string) {}
}
class Formatter {
  constructor(readonly config: Config) {}
}

const container = new Container();
container.register(Config, { useValue: new Config("worker") });
container.register(Formatter, {
  useFactory: resolver => new Formatter(resolver.resolve(Config)),
});
const formatter = container.resolve(Formatter);
```

Factories provide checked constructor wiring. Static `params` metadata and implicit
class auto-registration have been removed. `find(Class)` remains an alias for
`resolve(Class)`. Class types follow TypeScript's structural typing; explicitly
widening a constructor variable can weaken its instance contract.

A provider has exactly one strategy: `useValue` or `useFactory`. Mixed strategies
are rejected before registration, including an extra `useValue: undefined`.

| Factory lifetime | Behavior |
| --- | --- |
| `singleton` (default) | One instance per registration; resolves dependencies in its registering container. |
| `scoped` | One instance per resolving scope; each job execution gets its own scope. |
| `transient` | A new instance for each resolution, including repeated entries in a job's deps. |

Value providers retain the supplied instance and accept no lifetime setting.
A singleton cannot depend on a scoped provider, including through transients.
Missing dependencies and cycles report their construction paths.

Registration closes for the entire container when it first resolves a dependency
or creates a child scope, even if resolution fails. A child may configure local
overrides before its own first resolution. Parent registrations are already fixed.
This avoids per-key sealing and registration-order surprises. Configure the system's
container before the first job runs.

Factories must resolve dependencies synchronously through their supplied resolver;
capture the resulting instances before returning callbacks. The resolver expires
when the factory returns. Do not use a captured outer container to bypass that
resolution context. Failed synchronous construction can be retried; successful
dependencies remain cached. The container does not automatically dispose resources.

## Choose a backend

A backend is required. Import `MemoryBackend` from `core` and pass
`backend: new MemoryBackend()` for in-process execution. To use Redis, import `RedisBackend`
from `core` and pass it instead; job definitions stay unchanged:

```ts
import { RedisBackend } from "core";

const jobs = createJobSystem({
  container,
  jobs: { updateMemory },
  concurrency: 4,
  backend: new RedisBackend({
    queue: "my-app",
    connection: { host: "127.0.0.1", port: 6379 },
  }),
});

try {
  console.log(await updateMemory({ id: "one", text: "hello" }).result());
} finally {
  await jobs.close();
}
```

Calling a job is the only way to execute it. The first call starts one worker,
then every call submits through the selected backend. Concurrent first calls share
startup; inputs are snapshotted before waiting. Set `concurrency` at creation
(default: 1).

Each system owns its backend and worker. Systems using the same Redis queue can
share work; they must have compatible catalogs and codecs, and each needs its own
configured dependency container. Worker startup and terminal worker failures reject
calls and pending results; create a new system after worker failure. There is no
separate producer-only or worker-only mode on `JobSystem`.

`jobs.close()` stops intake, drains active work, rejects local result waits, and
releases backend resources. Closing is idempotent and prevents new runs. It cannot
retract work already accepted remotely, and draining can wait indefinitely for a
handler that never finishes. Closing an unused system does not start a worker.

The memory backend retains results for 60 seconds and at most 1,000 completed jobs;
configure its age using `new MemoryBackend({ resultTTLms: ... })`. Redis retains
results for 300 seconds by default (`resultTTLSeconds`), enforced on reads even
when BullMQ's physical cleanup is deferred. Unknown or expired results reject with
`ResultUnavailableError`. This is result retention, not permanent job history.

## Serialization and cancellation

Both backends use the same `JsonCodec` by default. It supports plain JSON records,
arrays, strings, booleans, finite numbers, null, and a top-level undefined value.
It rejects values whose meaning JSON would change: Date/class instances, functions,
bigints, symbols, nonfinite numbers, negative zero, sparse arrays, extra array
properties, nested undefined, and cycles. Record prototype identity is not part of
the JSON data contract. A custom `codec` can support additional values; its
`encode(value): string` and `decode(encoded): unknown` must preserve those values.

Only an ID, job name, and encoded input cross the backend boundary. Classes,
service instances and AbortSignal objects stay in the worker. Invalid input encoding
fails before submission; invalid output encoding becomes a terminal failed outcome.

```ts
const output = await updateMemory(input).result({ signal: AbortSignal.timeout(5_000) });
```

Caller signals cancel waiting only. Submission has no signal: once a job is called,
the work is submitted unless startup or the backend fails. An accepted job may
execute even if the caller stops waiting or closes, and the same handle can wait
again later. There is no remote cancellation protocol in this version.
The handler's final AbortSignal belongs to the worker/backend, not the waiting
caller. Providers may use it for observed execution loss; it cannot undo effects
or forcibly interrupt JavaScript.

The job deadline starts at delivery, including time waiting for a per-job
concurrency permit. An expired waiter releases its worker slot without entering
the handler. Active hooks and handlers must cooperate with cancellation, for
example by passing the signal to `fetch`. Retries wait for the previous handler
and its cleanup to settle; a handler ignoring its signal can continue producing
effects after the deadline. Graceful shutdown drains that work rather than
aborting it. A success hook completing after the deadline produces a terminal
failure, without repeating the handler's effects.

## Execution outcomes and hooks

Each execution gets a fresh DI scope and a context containing the stable submitted
`jobId`, `name`, the `signal` that aborts on execution loss or timeout, and the
1-based `attempt` with `maxAttempts`. Hooks can be synchronous or asynchronous and
are awaited:

1. Check worker cancellation, decode input, and resolve dependencies.
2. Await `beforeRun(input, context)`, then check cancellation again.
3. Await the handler and check cancellation before reporting success.
4. Await `onSuccess(output, context)`, encode output, and record the outcome.

Dependency, beforeRun, handler, timeout and execution-cancellation failures call
`onError(error, context)` on every attempt; when the context signal has aborted,
the reported error is its reason (a `TimeoutError` or the execution-loss reason), not
whatever the handler threw in response. Once attempts are exhausted or the
error is `NonRetryableError`, become terminal failed outcomes. If onError also
fails, both error messages are retained as an AggregateError description. Decode
failures, onSuccess failures, and output-encoding failures are terminal without a
retry; the last two do not invoke onError or repeat the handler. On the caller side, failed outcomes reject with
`JobExecutionError`, whose `failure` contains the remote name/message/stack.
Original exception prototypes and arbitrary thrown objects do not cross processes.

Retries follow the job's `metadata`. On Redis they are BullMQ attempts and backoff:
a retryable failure before the final attempt is thrown to BullMQ, which delays the
next delivery; the final outcome is recorded as completed queue processing even
when the application outcome is failed. An executor/infrastructure rejection uses
the same attempt budget, and BullMQ provides stalled-worker recovery. Connection
errors are retried by its clients; producer/result commands have finite request
bounds. A network submission failure can leave acceptance uncertain.

Redis recovery can execute work more than once, especially after a crash between
an external side effect and outcome persistence. Handlers with external effects
should use the stable job ID for application-level idempotency. Neither backend
promises exactly-once external effects. Redis durability also depends on the Redis
server's persistence configuration.

## Implement another provider

```ts
interface JobBackend {
  submit(message: JobMessage): Promise<string>;   // the ID to wait on
  result(id: string, options?: WaitOptions): Promise<JobOutcome>;
  work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker>;
  close(): Promise<void>;
}
type JobExecutor = (message: JobMessage, signal: AbortSignal, attempt: number) => Promise<JobOutcome>;
```

This is the entire strategy seam. Backend inputs/outputs are data-only envelopes;
core keeps catalog typing, serialization, DI, hooks, timeouts and per-job
concurrency. A backend owns acceptance, delivery, retries, recovery, retention,
result waiting, acknowledgments and its connections. Each message carries its
`policy`: total attempts, backoff, and an optional dedupe key. `submit` returns the
message's own ID, or the ID of a queued or running job that holds the same key.
`work` is ready when it resolves and calls the executor with a 1-based attempt
number. A failed outcome marked `retryable` before the final attempt is redelivered
after `backoffDelay(policy, attempt)`, exported from `core`; any other outcome is
retained. `JobWorker.done` exposes later terminal processing failure; `close`
drains active callbacks. A callback rejection is an infrastructure failure, with
recovery documented by the adapter. Unknown/expired IDs reject rather than waiting
forever.

Memory and Redis/BullMQ are implemented. An SQS or RabbitMQ adapter may need an
additional result store; a queue alone is not the complete backend. The core does
not assume a universal visibility lease, transaction protocol or pub/sub mechanism.

See [the four-design comparison and coding-style review](docs/designs/job-backends.md)
for the selected boundary and rejected alternatives. BullMQ's [connection behavior](https://docs.bullmq.io/guide/connections)
and [worker shutdown](https://docs.bullmq.io/guide/workers/graceful-shutdown) inform
the Redis adapter's owned connections and draining lifecycle.

## Verification

`pnpm test` builds the packages and runs core behavior and emitted-type checks.
Redis integration tests skip unless a dedicated Redis is supplied:

```sh
JOB_SYSTEM_REDIS_PORT=16379 pnpm --filter core test
```

Use an isolated test Redis. Tests use unique queue names and delete only their own
queues. They include separate producer/worker processes, late result retrieval,
terminal failures, cancellation and shutdown. No tests or infrastructure setup
are added to the tiny app's source directory.

The earlier DI review in `docs/reviews` is historical and predates this API.

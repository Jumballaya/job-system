# Job system

A lightweight TypeScript library for class-based dependency injection and typed jobs
with swappable backends. This workspace uses Node.js 24 and pnpm 11.18.0.
Core includes the memory strategy. Redis/BullMQ, Postgres, and Temporal each live in
their own optional backend package; each backend owns execution, schedules, and results.

```sh
pnpm install
pnpm test
pnpm start
```

`pnpm build` compiles core, app, and all backend packages. `pnpm test` also runs Postgres
and Temporal integration tests using isolated servers. Embedded Postgres is a test-only
dependency; Temporal downloads its server executables on first use.
`pnpm start` runs the app demo against Redis
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

`defineJob` shallowly copies and freezes the definition and a copied dependency list,
and returns a callable job. Nested metadata is not deeply frozen; configure it before
creating the system and do not mutate it afterward. It does not register services or start work. Calling the job submits
work through whichever system it is attached to; a job belongs to one open system
at a time and is released when that system closes.

## Job metadata

Every job carries a policy. These defaults apply when `metadata` is omitted:

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
| `retries.attempts` | 3 | Execution-attempt budget, including the first; infrastructure redelivery accounting is provider-specific. |
| `retries.backoff` | exponential from 1 s | Wait after a failed attempt: 1 s, then 2 s. `"fixed"` or `"exponential"` uses the default delay. |
| `timeout` | 5 minutes | Requests cancellation from execution start; active work must settle before a business retry. `Infinity` disables the core timeout. |
| `concurrency` | unlimited | Simultaneous executions of this job per worker, including hooks, within the system's own limit. Saturated work stays queued so other job types can use available slots. |
| `key` | none | Derives a dedupe key from the input. Calling the job while a job with that key is queued or running returns the active job's handle instead of a new one. |
| `idempotencyKey` | none | Identifies one retained operation: identical submissions reuse its ID and outcome, including after completion. Provider retention is described below. Mutually exclusive with `key`. |

Dependency construction, `beforeRun`, and handler errors retry until attempts run out, except `NonRetryableError`,
exported from `core`, which fails immediately. Failures after the handler
succeeded, such as an `onSuccess` hook error, never retry. Handlers with external
effects should use a persisted operation ID, because a retry repeats the whole handler.
Invalid metadata is rejected by `defineJob`.

For durable operations, declare `metadata.idempotencyKey: input => input.operationId`
and call the job normally. Include tenant identity in the key when applicable.
Redis retains these operations and their outcomes without expiry; memory retains
them only until the backend closes. Temporal keeps ordinary keyed operations in open
workflows that periodically compact their history; scheduled occurrences use normal
namespace retention instead. A different input for the same operation rejects
with `IdempotencyConflictError`. See [durable idempotency](docs/idempotency.md) for
downstream keys, database transactions, and the crash-recovery guarantees.

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

| Package | Execution and storage |
| --- | --- |
| `core` | In-process memory; no external service. |
| `postgres-backend` | Postgres queue, leases, schedules, and long-term results; borrows an existing pool. |
| `redis-backend` | Redis/BullMQ; owns its Redis connections. |
| `temporal-backend` | Temporal workflows/activities; borrows existing connections. |

For Postgres, use `PostgresBackend` from `postgres-backend`, run `await backend.migrate()`
during application setup, then pass it to `createJobSystem`. It can share Adapt v2's
existing database through `database.$client`; migrations belong to the backend package.
See the [Postgres guide](packages/postgres-backend/README.md) for setup, retention, and recovery.
There is no separate history-store interface: the selected backend owns its full lifecycle.


For an existing Temporal deployment, use `TemporalBackend` from the optional
[`temporal-backend` package](packages/temporal-backend/README.md). It accepts your
Temporal client and worker connection; job definitions and calls stay the same.

A backend is required. Import `MemoryBackend` from `core` and pass
`backend: new MemoryBackend()` for in-process execution. To use Redis, import `RedisBackend`
from `redis-backend` and pass it instead; job definitions stay unchanged:

```ts
import { RedisBackend } from "redis-backend";

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

`createJobSystem(...)` attaches the catalog and starts one local worker immediately.
Initialization is synchronous; backend startup happens in the background. The worker
consumes existing and future work without a local submission. Calling a job snapshots
its input, waits for startup internally, and submits through the backend.
Set `concurrency` at creation (default: 1).

Each system owns its backend and worker. Systems using the same Redis queue can
share work; they must have compatible catalogs and codecs, and each needs its own
configured dependency container. Worker startup and terminal worker failures reject
calls and pending results; create a new system after worker failure. A container is
optional for jobs without dependencies and for submit-only systems. Background startup
failures are retained and reject subsequent calls and `close()`.
Temporal borrows its client and native connection; the application owns their cleanup.

Provide an optional system `onError` to hear about startup or terminal worker failure
even while idle:

```ts
const jobs = createJobSystem({
  jobs: { updateMemory },
  backend: new RedisBackend(redisOptions),
  onError(error) { console.error("Job worker failed", error); },
});
```

The hook receives the original failure once. An unexpected clean worker exit reports
`Error("Job worker stopped")`. Job-handler errors, transient backend reconnects, and
shutdown initiated by `close()` don't trigger it. Async hooks may call `close()`;
shutdown never waits for reporting. Hook exceptions/rejections are ignored so they
cannot replace the worker failure; handle delivery failures inside your reporter.

## Run workers separately

In an API or other producer process, register the catalog with `worker: false`:

```ts
const jobs = createJobSystem({
  jobs: { updateMemory },
  backend: new RedisBackend(redisOptions),
  worker: false,
});

const handle = await updateMemory(input);
const output = await handle.result(); // optional; waits for a remote worker
```

This process never starts a consumer or resolves handler dependencies. It can
submit while workers are offline, then close once acceptance is acknowledged.
Closing a producer stops its local result waits; jobs already accepted by Redis
remain queued for other processes.

In the worker process, use the same queue, catalog names, and codec, and register
the services needed by the handlers:

```ts
const jobs = createJobSystem({
  container,
  jobs: { updateMemory },
  backend: new RedisBackend(redisOptions),
  concurrency: 4,
});

// The worker starts during initialization and consumes queued work automatically.
// Call this from the app's existing shutdown handler:
await jobs.close();
```

There is no separate startup call or worker handle to manage. A restarted process
begins consuming as soon as it initializes its system. Call `jobs.close()` from your
app's shutdown handler; see the runnable [demo worker](packages/app/src/worker.ts).

To try it with two terminals after `docker compose up -d`:

```sh
# Terminal 1: consume jobs continuously
pnpm worker

# Terminal 2: the existing demo submits everything to the remote worker
JOB_WORKER=remote pnpm start
```

The demo uses the same catalog in both processes. A single worker makes its
process-local counter and retry examples predictable. `JOB_BACKEND=memory` remains
available for the combined demo; memory does not share work across processes.

`jobs.close()` stops intake and local result waits, then lets active work drain.
Configure `shutdownTimeoutMs` at initialization; the default grace period is 30
seconds, including startup and resource cleanup. If it expires, `close()` rejects
with `ShutdownTimeoutError` and requests cancellation through execution signals.
Repeated calls return the same close promise. Timing bounds require a responsive
JavaScript event loop.

Cleanup continues after the deadline. Live handlers retain worker capacity, and
Redis workers continue lease renewal while their event loop and connection remain responsive.
Owned connections close once handlers and cleanup settle; Temporal's borrowed connections
remain open. Execution loss can still cause provider redelivery. Interrupted handlers return to the queue with their original IDs and
remaining attempts, without invoking the business-error hook. A success hook that
outlives the deadline produces a terminal failure instead of repeating its handler.
Closing during startup still closes the worker if it arrives after the deadline.

A timeout does not forcibly stop JavaScript or end the process. The app's shutdown
path decides when to terminate a process that cannot finish; Redis then recovers
unfinished work through its lease protocol. Memory has no recovery after backend
close or process exit. Accepted remote work remains queued for other workers.

For jobs without `idempotencyKey`, successful results last 60 seconds in memory
(`resultTTLms`) or 300 seconds in Redis (`resultTTLSeconds`). Failures default to
seven days in both backends (`failureTTLms` / `failureTTLSeconds`). Memory caps
successes and failures independently at 1,000 each, so successful traffic cannot
evict failure history. Retention is enforced on reads even when physical cleanup
is deferred. Unknown or expired results reject with
`ResultUnavailableError`. Idempotent operations are exempt from both age and count
cleanup; their retained records grow with the number of distinct operation keys.
Temporal's ordinary results follow namespace history retention; its open idempotency
records are retained separately. See the [Temporal guide](packages/temporal-backend/README.md).

## Inspect executions

```ts
const record = await jobs.get(savedJobId);
if (record?.status === "failed") console.error(record.error.message);
if (record?.status === "succeeded") console.log(record.output);
```

`get(id)` reads the existing backend record without submitting work or waiting for
completion. Redis supports lookup from a different process with the same queue
and codec, including a producer using `worker: false`. No additional database is
needed. Unknown or expired IDs return `null`; connection and decoding errors reject.
Temporal inspection usually requires a compatible worker to answer workflow queries;
never-started queued records can be read without one.

Records contain `id`, `name`, decoded `input`, `status`, `attempts`, and `createdAt`.
Status is `queued`, `running`, `succeeded`, or `failed`. `startedAt` describes the
latest delivery and is absent before the first one. Terminal records add
`finishedAt` and either `output` or `error` (`name`, `message`, optional `stack`).
Timestamps are Unix milliseconds. Attempts count completed attempts plus the
currently active attempt; capacity waits and shutdown handoffs spend none.
Delayed jobs and retries waiting for their next delivery are `queued`.

Redis reads state and data atomically. Each response is a snapshot; the job may
advance after the read. `JobRecord` uses a status union; payloads are `unknown`
because an arbitrary stored ID may come from a different catalog or deployment.
Lookup uses the same retention as `result()`. It provides retained execution
details; permanent automation history belongs in the application's data store.

## Delays and schedules

Timing belongs to a call, keeping the job definition reusable:

```ts
const delayed = await updateMemory(input, { after: "10m" });
const output = await delayed.result();
await updateMemory(input, { at: new Date("2026-10-01T09:00:00Z") });

const schedule = await updateMemory(input, { every: "1h" });
await schedule.update({ daily: "09:00", timezone: "America/New_York" });
await schedule.remove();

// Reopen from another process using the same backend location.
await jobs.schedule(savedScheduleId).remove();
```

One-off calls return the existing execution handle. Recurrence returns a schedule
handle with `id`, `update(timing)`, and `remove()`. Re-registering the same job and
encoded input addresses the same schedule across producers and workers; changing
its timing updates that registration. Store the returned ID to manage it later.
Different input creates another schedule. `JsonCodec` canonicalizes object keys;
custom codecs must encode equivalent inputs identically for stable schedule identity.

The handle's methods are local closures; they are not serialized. Redis or Temporal
stores the job registration name, encoded input, delivery policy, and schedule rule.
Persist `schedule.id` in application storage if you need to manage that schedule later:

```ts
// After redeploy: initialize the same backend and job catalog, then reopen the handle.
const schedule = jobs.schedule(savedScheduleId);
await schedule.update({ every: "2h" });
```

Reopening a handle neither creates nor changes the stored schedule; existence is checked
when its methods contact the backend. Redis/Temporal schedules keep existing without the
producer process. New workers resolve the stored job name against the deployed catalog,
decode the input, and construct dependencies locally. Preserve registration names and
payload compatibility across deployments. Re-registering an old timing rule on startup
can overwrite a later edit; reopen by ID to preserve it.

Durations use `ms`, `s`, `m`, `h`, `d`, or `w`; intervals first run after one interval.
`at` accepts a Date, Unix milliseconds, or an ISO timestamp with an explicit offset.
These times specify earliest delivery; busy or offline workers can deliver later.
`daily` uses 24-hour `HH:mm`. For more complex calendars, use
`{ cron: "0 9 * * 1-5", timezone: "America/New_York" }`. Cron accepts five fields or
six including seconds. Calendar rules require an explicit IANA timezone. Memory and Redis
use cron-parser's DST rules; Temporal uses its own scheduler semantics and requires
whole-second intervals. Elapsed intervals such as `"1d"` mean 24 hours.

Redis persists registrations and pending deliveries. Workers resume them on restart
without producer code running again. After downtime, the pending occurrence runs
and scheduling continues without replaying every missed interval. Identical
registration preserves the pending occurrence; changed timing replaces it.
Removal is idempotent and stops future occurrences; already-started work and its
retries finish normally. Updating a removed schedule rejects.

Each occurrence gets a distinct execution ID that survives its retries. Memory and Redis
scope active deduplication and durable idempotency to that occurrence; their idempotent
occurrence records retain the same retention policy as ordinary idempotent jobs.
Temporal gives each occurrence a separate workflow and removes ordinary dedupe/idempotency
keys from its policy, so its scheduled outcomes follow namespace history retention.
Use the execution ID from hooks for occurrence-specific external receipts. Handler overlap
is bounded by the existing per-worker concurrency settings.

Temporal's schedule service can enqueue occurrences while application workers are offline.
Catch-up after a Temporal service outage follows its schedule policy; the adapter leaves
the provider's catch-up setting at its default. This differs from Redis's pending-occurrence behavior.

Memory supports the same timing API, but its schedules disappear on backend close
or process exit. Redis durability depends on its configured persistence.

## Serialization and cancellation

All job systems use `JsonCodec` by default, independently of their backend. It supports plain JSON records,
arrays, strings, booleans, finite numbers, null, and a top-level undefined value.
It rejects values whose meaning JSON would change: Date/class instances, functions,
bigints, symbols, nonfinite numbers, negative zero, sparse arrays, extra array
properties, nested undefined, and cycles. Record prototype identity is not part of
the JSON data contract. Object keys are encoded in sorted order so equivalent
records do not produce false idempotency conflicts. A custom `codec` can support additional values; its
`encode(value): string` and `decode(encoded): unknown` must preserve those values.
Its encoding must also be deterministic when used with idempotency keys.

Messages crossing the backend boundary contain an ID, job name, encoded input, delivery
policy, and optional delivery timestamp. Recurring registrations also carry a timing rule. Classes,
service instances and AbortSignal objects stay in the worker. Invalid input encoding
fails before submission; invalid output encoding becomes a terminal failed outcome.

```ts
const output = await updateMemory(input).result({ signal: AbortSignal.timeout(5_000) });
```

Caller signals cancel waiting only. Submission has no signal: once a job is called,
the work is submitted unless startup or the backend fails. An accepted job may
execute even if the caller stops waiting or closes, and the same handle can wait
again later while the system remains open. The core API has no remote cancellation method.
Temporal users can request workflow cancellation through their Temporal client; the adapter
passes Activity cancellation to the handler's signal.
The handler's final AbortSignal belongs to the worker/backend, not the waiting
caller. Providers may use it for observed execution loss; it cannot undo effects
or forcibly interrupt JavaScript.

The job deadline starts when worker capacity is reserved for execution. Waiting
for per-job capacity stays queued, consumes no retry, and starts no execution
deadline. Active hooks and handlers must cooperate with cancellation, for
example by passing the signal to `fetch`. Retries wait for the previous handler
and its cleanup to settle; a handler ignoring its signal can continue producing
effects after the deadline. Shutdown allows a grace period, then requests cancellation
while continuing to wait for safe cleanup in the background. A success hook completing after the deadline produces a terminal
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
should pass the persisted operation ID to downstream idempotency mechanisms. No backend
promises exactly-once external effects. Redis durability also depends on the Redis
server's persistence configuration.

## Implement another provider

```ts
interface JobBackend {
  readonly schedules?: JobSchedules;            // recurring registration and management
  get?(id: string, options?: WaitOptions): Promise<JobRecord<string, string> | null>;
  submit(message: JobMessage): Promise<string>;   // the ID to wait on
  result(id: string, options?: WaitOptions): Promise<JobOutcome>;
  work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker>;
  close(): Promise<void>;
}
type JobExecutor = (message: JobMessage, signal: AbortSignal, attempt: number) => Promise<JobOutcome>;
```

Backend inputs/outputs are data-only envelopes;
core keeps catalog typing, serialization, DI, hooks, and timeouts. A backend owns
worker admission, acceptance, delivery, retries, recovery, retention,
result waiting, acknowledgments and owned connections. Borrowed connections remain the
application's responsibility. Each message carries its
`policy`: total attempts, backoff, and an optional active dedupe key or durable
idempotency key. Core derives a stable ID from the registration name and
idempotency key. A backend must atomically reserve that ID, reject changed inputs,
and retain idempotent records and terminal outcomes without ordinary result eviction.
`submit` returns the accepted ID, or the ID of a queued or running job holding an active dedupe key.
`work` is ready when it resolves and calls the executor with a 1-based attempt
number. A failed outcome marked `retryable` before the final attempt is redelivered
after `backoffDelay(policy, attempt)`, exported from `core`; any other outcome is
retained. `JobWorker.done` exposes later terminal processing failure; `close`
drains active callbacks and keeps their leases until they settle. `JobInterruptedError`
is executor control flow: stop this worker and return the settled execution to the
queue with its ID, deduplication reservation, and attempt budget intact. Other
callback rejections are infrastructure failures, with
recovery documented by the adapter. Unknown/expired IDs reject rather than waiting
forever.

Providers supporting inspection implement `get` with a consistent state snapshot,
encoded input/output, and retention matching `result()`. Core decodes the payloads.
An adapter without inspection rejects `jobs.get` with an unsupported-capability error.

`WorkerOptions.concurrencyByJob` carries the catalog's finite per-job limits,
keyed by registration name. Backends reserve capacity before invoking the executor
and release it after the whole callback settles, including hooks and cleanup.
Saturated job types must leave execution slots available for other types, retaining
their IDs and deduplication reservations without consuming execution attempts.
Limits apply independently to each worker; a fully occupied worker waits for an
execution to finish before starting another.

`JobMessage.availableAt`, when present, is the earliest first delivery in Unix
milliseconds; delay must not occupy an execution slot. Retry backoff remains
independent. Providers supporting recurrence implement `JobSchedules.upsert`,
`update`, and `remove` with normalized `ScheduleRule` values (elapsed milliseconds
or cron plus timezone). The provider owns durable registration identity and
occurrence advancement. Providers without this capability reject recurring calls.

Memory, Redis/BullMQ, Postgres, and Temporal are implemented. An SQS or RabbitMQ adapter may need an
additional result store; a queue alone is not the complete backend. The core does
not assume a universal visibility lease, transaction protocol or pub/sub mechanism.

See [the four-design comparison and coding-style review](docs/designs/job-backends.md)
for the selected boundary and rejected alternatives. BullMQ's [connection behavior](https://docs.bullmq.io/guide/connections)
and [worker shutdown](https://docs.bullmq.io/guide/workers/graceful-shutdown) inform
the Redis adapter's owned connections and draining lifecycle.

## Verification

`pnpm test` builds all packages, runs core behavior and emitted-type checks, and runs
Postgres and Temporal integration tests. Both start local servers and need permission
to open local sockets. Temporal downloads server executables on first use; neither suite skips.
Postgres can instead use a dedicated database through `JOB_SYSTEM_POSTGRES_URL`.
To run only core after building, use `pnpm --filter core test`.

Most Redis integration tests skip unless a dedicated Redis port is supplied.
The Redis server-crash test separately requires the server executable:

```sh
JOB_SYSTEM_REDIS_PORT=16379 JOB_SYSTEM_REDIS_SERVER=/path/to/redis-server pnpm --filter redis-backend test
```

Use an isolated test Redis. Tests use unique queue names and delete only their own
queues. They include separate producer/worker processes, late result retrieval,
terminal failures, cancellation and shutdown. No tests or infrastructure setup
are added to the tiny app's source directory.

The six [V1 replacement acceptance contracts](packages/redis-backend/test/acceptance/README.md)
run separately:

```sh
JOB_SYSTEM_REDIS_SERVER=/path/to/redis-server pnpm test:acceptance
```

This suite owns its Redis and worker processes. Each numbered gap has one top-level
test; the acceptance README documents its scenario and interfaces.

The earlier DI review in `docs/reviews` is historical and predates this API.

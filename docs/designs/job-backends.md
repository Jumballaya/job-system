# Job backend design comparison

This comparison applies the complete `coding-style` skill to four messaging
boundaries. Each design was revised to address its own complexity costs before
selection. The selected boundary is A, with explicit worker readiness and
completion borrowed from D. This is a design record, not a claim that every
discussed broker has an implemented adapter.

The backend comparison below records the original design. Current callers register
`jobs: { sendEmail, syncUser }` and invoke the exported jobs directly. Registration
keys supply delivery identity and deduplication scope. The historical API sketches
below illustrate the design alternatives. The final caller API
was subsequently simplified to `run(name, input)` plus resource cleanup with
`close()`. Startup, submission, and result waiting are now internal; concurrency
is configured at construction. A backend must be passed explicitly; there is no
default strategy. Backend methods remain unchanged.

The job definition owns dependencies, handler, and lifecycle hooks. The registration
object supplies each job's stable delivery name; definitions have no explicit name. The job system owns typed dispatch and a dependency scope per execution.
The question is where delivery, retained results, and worker resources belong.

| Design | Owns the difficult policy | What callers can forget | Main cost |
| --- | --- | --- | --- |
| A: complete backend | Backend owns delivery and retained outcomes together. | Receipts, acknowledgment ordering, result notification races, broker recovery. | An adapter must supply a complete job lifecycle, potentially using more than one service. |
| B: leased delivery and attempt store | Core owns attempts; adapters supply lease and atomic storage primitives. | Broker-specific delivery operations. | Core becomes a distributed execution engine with fencing and recovery policy. |
| C: transport and outcome store | Core coordinates independently replaceable messaging and result storage. | Concrete transport and storage clients. | The cross-component completion protocol remains core's responsibility. |
| D: opened sessions | A strategy creates resource-owning producer and worker sessions. | Connection acquisition and session-specific resource ownership. | Another public layer, unless sessions hide a real acquisition boundary. |

## A: complete job backend

The first sketch exposed submission, polling, acknowledgment, retry, completion,
result lookup, and result subscriptions. That was shallow: callers would need to
understand the same delivery transaction as each adapter author.

The first skill pass moved those decisions into one backend. Core supplies an
executor; the backend delivers a message to it and retains the outcome. A result
wait covers both an already completed job and future completion, so callers do
not implement a read-then-subscribe race.

The second pass made ownership and failure categories precise:

- Submission and execution share one stable job ID.
- Application failure is a tagged terminal outcome. An executor rejection is an
  infrastructure failure, not an instruction to record an ordinary job failure.
- Waiting cancellation does not cancel accepted work.
- Worker startup resolves only when ready. A separate completion promise exposes
  failures that happen after startup.
- Shutdown stops intake and drains active executions. Repeated closure is harmless.
- The backend handles serialized strings; the codec and typed dispatch belong to
  core. Redis does not need to know how a job's TypeScript input was declared.

The resulting interface is implemented in
[`backend.ts`](../../packages/core/src/backend.ts):

```ts
interface JobBackend {
  submit(message: JobMessage): Promise<void>;
  result(id: string, options?: WaitOptions): Promise<JobOutcome>;
  work(execute: JobExecutor, options?: WorkerOptions): Promise<JobWorker>;
  close(): Promise<void>;
}

interface JobWorker {
  readonly done: Promise<void>;
  close(): Promise<void>;
}
```

`JobMessage` contains `id`, `name`, and encoded `input`. `JobOutcome` is a tagged
union of successful encoded output or a failure description. It cannot contain
both success and failure. `result` rejects with `ResultUnavailableError` when an
ID is unknown or its result has expired; it also rejects when waiting is aborted
or the backend closes. The initial sketch returned `undefined` for absence. The
selected API instead treats an unavailable requested result as an explicit failure
of result retrieval, keeping the successful return type terminal and complete.

This boundary is deep because its four methods hide a coherent delivery/result
policy. It does not expose queue receipts or add job retry flags to DI providers.
It also has a real cost: a message broker without retained job results needs a
compound backend implementation. A small interface does not make that distributed
protocol disappear.

## B: leased delivery and an atomic attempt store

The initial design gave core a lease from the queue plus generic attempt storage.
Core would invoke the handler, store the outcome, and acknowledge the message.
That seemed to make adapters small, but generic `get`/`set` storage could not keep
two deliveries from both acting as the current attempt.

The first skill pass replaced generic storage with explicit atomic operations:
`claim`, `renew`, `finish`, `read`, and `cancel`. An attempt needs an ownership
token, and finishing must verify that token. Renewal and completion must reject
stale owners without allowing them to overwrite the current result. Cancellation,
if exposed, must participate in the same atomic state transition.

The second pass separated stable job identity from attempt ownership and grouped
the state into valid variants rather than optional lease/result fields. It also
identified the policy core would now own: renewal timing, lost ownership,
duplicate delivery, completion-versus-cancellation races, retry exhaustion, and
recovery after storing a result but before acknowledgment.

This is more honest than the first sketch, but it is a substantially larger
system. Fencing stored outcomes cannot make arbitrary handler side effects
exactly once; a stale worker may already have contacted another service.

B is appropriate when owning one portable execution protocol is itself a product
requirement. It was rejected here because it would move broker lifecycle policy
into this lightweight core and reproduce work supplied by a job engine such as
BullMQ. The skill's instruction to pull complexity downward does not mean pulling
all distributed-system complexity into the library's central module.

## C: transport plus outcome storage

The initial sketch split a replaceable transport from a result store, with a
result `get` followed by `watch`. That exposes a race: completion can happen
between the initial read and installing the observer.

The first skill pass collapsed result publication into `settle` and result
retrieval into `wait`. The outcome store must make waiting work whether completion
occurred before, during, or after the call. A tagged outcome keeps ordinary
handler failure separate from transport errors.

The second pass examined completion ordering across the two modules. If core
acknowledges delivery before persisting the result, it can lose the outcome. If
it persists first, a crash before acknowledgment permits redelivery. Core then
needs a coherent duplicate/result-reuse policy, and the independently replaceable
interfaces must agree on identity, retention, and terminal state.

C can fit an application that already has a durable outcome store and must share
it across several transports. It is less suitable for wrapping an integrated job
engine: splitting BullMQ's delivery and result lifecycle into independent generic
ports would discard that integration and make core reconstruct it. The refined
`settle`/`wait` boundary removes one race but does not remove the cross-module
protocol. This is the central information-leakage cost, not a file-count issue.

## D: explicit producer and worker sessions

The initial strategy was `run(request, execute)`. It looked like a small execution
decorator but coupled each submission to executable worker code. A remote producer
must send a job message, not a JavaScript callback.

The first pass moved executor binding to startup:
`open({ role, execute?, concurrency })` returned a session with submission,
result waiting, and closure. This hid resource acquisition, but its optional fields
admitted contradictory configurations: a worker without an executor or a producer
with execution options.

The second pass separated producer and worker opening, or equivalently required
discriminated overloads. A producer session offers `submit`, `result`, and `close`;
a worker session offers `done` and `close`. The completion promise matters because
successful startup does not establish that a worker will keep running.

The same pass rejected lazy opening on first use. Lazy acquisition introduces
simultaneous first calls, opening failures, close-during-open races, and submissions
after closure merely to avoid an explicit startup `await`. Explicit asynchronous
startup gives those transitions an observable owner.

D earns its extra layer when a reusable strategy creates independently owned
connections or sessions. If the strategy already owns the connections and the
session only forwards its methods, it is a shallow wrapper. The selected design
therefore borrows ready-on-return `work()` and observable `JobWorker.done` without
adding a strategy/session hierarchy to every backend.

## Selection and implementation boundaries

A keeps delivery and outcomes together, matching the intended Redis adapter's
BullMQ lifecycle. D contributes its useful lifecycle guarantees. B and C remain
alternatives for different requirements, rather than additional optional modes in
the same interface.

The implementation separates concerns by knowledge:

- Core defines the wire envelope, codec, terminal outcomes, typed job dispatch,
  hook behavior, and dependency scope creation.
- The memory backend provides process-local delivery and results using the same
  backend contract. It is not durable storage.
- [`RedisBackend`](../../packages/core/src/strategies/redis-backend.ts) implements
  the contract using BullMQ. Both strategies live in core; Redis configuration
  and connections remain encapsulated in its strategy.
- Application code calls `run(name, input)` and closes owned resources when done.
  The first valid run opens one worker; concurrent runs share its startup. Core
  supervises worker completion and rejects waits if processing stops. Each system
  needs its own configured container and the queue's complete job catalog.

The job-system surface is `run` and `close`. `run` snapshots input, awaits worker
readiness, submits, and waits for the outcome. There is no public job handle or
separate startup step. Concurrency is fixed in system configuration. Codec and
backend configuration remain separate from job definitions.

This revises D's preference for explicit caller startup: first-use coordination
is complexity the library absorbs to simplify every caller. Startup failure,
concurrent first calls, closure during startup, and worker failure are verified
internally. The backend still exposes readiness to core through `work()`.

The backend contract has one queue/catalog boundary, not a per-worker name filter.
Workers sharing that queue must be able to execute its submitted job names. Use
separate backend queues for disjoint catalogs; do not assume a worker can safely
discard names it does not recognize.

Two semantic constraints require particular care in implementation. First, a
handler can finish its external effects before a success hook fails. The error
must not silently trigger another handler execution. Second, graceful closure
must expose worker failure and release pending result waiters; shutting down is
more than closing the underlying connection.

Redis is the concrete external provider in this implementation. SQS and RabbitMQ
were portability checks, not implemented adapters. A future SQS backend must
account for visibility and duplicate delivery; a RabbitMQ backend must account
for publisher confirmation and consumer acknowledgment. Both must additionally
satisfy this library's retained-result contract. Those obligations may require
another service. See the primary documentation on
[SQS visibility](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
and [RabbitMQ acknowledgment](https://www.rabbitmq.com/docs/confirms).

## Coding-style checks that constrain further changes

Keep the interface comment short enough to describe the responsibility: own
delivery and retained outcomes. Do not grow it into a catalogue of provider
exceptions. Keep codec decisions out of adapters and receipt/lease decisions out
of handlers. Keep related lifecycle state together rather than creating a file
for every execution step.

Use tagged outcomes and explicit worker ownership; do not add combinations of
`running`, `failed`, `closed`, and optional results that can contradict each other.
Use idempotent closure to remove routine shutdown errors. Preserve errors for
unavailable results and genuine infrastructure failures rather than translating
them into successful job output.

Behavioral verification should cover submitted ID preservation, waiting before
and after completion, application failure, malformed payloads, canceled waiters,
worker startup failure, failure after readiness, shutdown with active work, and
result retention. Backend-specific tests must establish actual delivery/recovery
behavior; passing the memory suite alone cannot establish Redis durability.

No part of this comparison justifies adding scheduling, a universal retry DSL,
decorators, global job cancellation, or an exactly-once promise. Add those only
when their caller benefit exceeds the permanent interface and state-machine cost.

# Redis backend

`RedisBackend` connects the core job system to a BullMQ queue. It owns its Redis
connections; pass connection settings, not an existing Redis client.

```ts
import { createJobSystem } from "core";
import { RedisBackend } from "redis-backend";

const jobs = createJobSystem({
  container,
  jobs: { updateMemory },
  concurrency: 4,
  backend: new RedisBackend({
    queue: "memory-jobs",
    connection: { host: "127.0.0.1", port: 6379 },
    resultTTLSeconds: 300,
    failureTTLSeconds: 7 * 24 * 60 * 60,
  }),
});

const output = await updateMemory(input).result();
await jobs.close();
```

Calling `updateMemory` sends the job through Redis; `result()` awaits its output.
`createJobSystem(...)` starts a local worker in the background immediately, so fresh
or restarted processes consume queued work without a local submission. Job calls
wait for startup internally. Systems on the same queue must use compatible catalogs
and codecs; workers supply their own dependency containers.

Pass `worker: false` to `createJobSystem` in producer processes. They keep the same
typed calls and result handles, need no dependency container, and never consume
jobs locally. Workers can initialize after the producer has exited. Call
`jobs.close()` during app shutdown to drain work and release connections.
See [separate workers](../README.md#run-workers-separately) for bootstrap examples
and the two-process demo.

Each job's `metadata.retries` becomes BullMQ `attempts` and `backoff` at submission,
and `metadata.key`, scoped by the registration key, becomes a BullMQ deduplication id that is held while the job is
queued, delayed, or active. A retryable failure before the final attempt is thrown
to BullMQ so it schedules the next delivery; the final outcome is retained like a
successful output. Executor rejection is infrastructure failure and shares the same
attempt budget. Delivery can repeat; handlers must account for repeated side effects.

Workers enforce the catalog's per-job concurrency before starting an execution.
If that job type is saturated, the worker returns the delivery to Redis with a
100 ms capacity delay and immediately continues looking for other work. This keeps
the ID and deduplication reservation, spends no execution attempt, and starts no
handler timeout. Capacity remains reserved through hooks and asynchronous cleanup.
Limits apply per worker; a fully occupied worker waits for an execution to settle.
Large saturated backlogs require repeated Redis admission checks.

Successful results remain available for `resultTTLSeconds` after completion.
Application and infrastructure failures use `failureTTLSeconds`, seven days by
default. Both policies are stored with the submission, so a fresh reader's settings
do not change its history. Older records without a failure policy retain their
original result TTL. Update all workers and producers before relying on the longer
retention; older adapters can still delete failures using the former cleanup policy.
Expired or unknown result waits reject with `ResultUnavailableError`.
The adapter scans bounded batches of completed and failed jobs on later submissions and recurring deliveries,
removing expired ordinary records. Reads enforce expiry even before cleanup runs.
BullMQ's queue-wide age/count removal is disabled because it can also delete records
that individual jobs asked to retain permanently.
Identical submissions with the same ID are idempotent before its outcome expires;
reusing the ID for a different message rejects. A retained expired ID rejects too.
Use a fresh ID for new work; the core job system generates these automatically.

`jobs.get(id)` reads this same record and returns decoded input/output, state,
attempts, timestamps, and failure details, or `null` for unknown/expired IDs. It works
for ordinary execution IDs and scheduled occurrence IDs from hooks. One Redis
transaction reads the job hash and active membership; completion cannot interleave
between those reads. Application failures are exposed as `failed` even though
BullMQ stores their final outcomes as completed processing. Infrastructure failures
are exposed with their saved message and stack. No separate history database or
application-side status writes are required.

`metadata.idempotencyKey` opts a job into durable replay. Its registration name and
operation key determine a stable custom job ID, reserved atomically by BullMQ.
Concurrent submissions and resubmissions after completion reuse that record;
different encoded input rejects with `IdempotencyConflictError`. The original
delivery policy wins, including exhausted retries and terminal failures.
These records and outcomes never expire through adapter cleanup. See
[idempotency](idempotency.md) for the application-side effect contract.

Before enabling this on an existing queue, drain workers and replace all old
producers/workers using queue-wide age/count cleanup. Mixed versions or external
BullMQ cleanup can erase the retained records. Deleting the queue or losing its
Redis data also loses queue-side idempotency; retain database/provider receipts.

Aborting a result wait affects only that wait. Accepted jobs continue running.
The job system bounds `jobs.close()` using `shutdownTimeoutMs` (30 seconds by
default). It stops intake and allows a grace period; expiry requests execution
cancellation and rejects with `ShutdownTimeoutError`. The underlying worker keeps
renewing leases while live handlers or cleanup remain. It never force-closes their
connections merely because the caller's deadline expired.

Once interrupted cleanup settles, the adapter returns that execution to Redis
without spending an attempt, allowing another worker to resume it immediately.
Handlers that remain live hold their leases until they settle or their process
dies. Process death uses BullMQ stalled-worker recovery. Once all active callbacks
settle, background shutdown closes the worker and queue connections. Direct
backend `close()` remains a draining operation; the system owns the grace period.
Redis requests and worker startup retain their five-second bounds.

## Scheduling

The [timing API](../README.md#delays-and-schedules) uses BullMQ delayed jobs and
native Job Schedulers. No separate scheduler process or application timer is needed.
Schedule identity hashes the catalog name and encoded input; concurrent producers
upsert the same Redis registration. Identical registration leaves its pending run
in place. Changing an interval starts the new cadence one interval after the update;
changing a calendar uses its next matching time. Concurrent management calls are
last-writer-wins; coordinate competing edits in the application when order matters.

BullMQ creates the successor when a worker claims an occurrence. Retries keep the
same occurrence ID; later occurrences use different IDs. Removal deletes the
registration and pending occurrence, leaving active occurrences and their retries
alone. Redis persistence retains schedules across process restarts. A worker that
was offline resumes the pending occurrence, then continues future scheduling.

Build and test against an isolated Redis server:

```sh
pnpm --filter redis-backend... build
JOB_SYSTEM_REDIS_PORT=16379 pnpm --filter redis-backend test
```

Integration tests create unique queues and remove only their own queues. The suite
includes submit-only producer processes that exit before fresh and replacement
workers start, a fresh reader of a completed result, conflicting operation
submissions, retention, and a worker killed after a
database commit. The crash test waits for BullMQ's real stalled-worker recovery,
usually about a minute. Redis integration tests skip without the port variable.

To also test a Redis process crash and recovery from AOF, supply a local binary:

```sh
JOB_SYSTEM_REDIS_PORT=16379 JOB_SYSTEM_REDIS_SERVER=/path/to/redis-server pnpm --filter redis-backend test
```

That test owns a separate temporary Redis process, port, and data directory. It
uses `appendfsync always`; it does not restart the server supplied by the port variable.

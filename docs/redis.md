# Redis backend

`RedisBackend` connects the core job system to a BullMQ queue. It owns its Redis
connections; pass connection settings, not an existing Redis client.

```ts
import { createJobSystem, RedisBackend } from "core";

const jobs = createJobSystem({
  container,
  jobs: { updateMemory },
  concurrency: 4,
  backend: new RedisBackend({
    queue: "memory-jobs",
    connection: { host: "127.0.0.1", port: 6379 },
    resultTTLSeconds: 300,
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

Ordinary results remain available for the configured TTL after completion, including to
new backend instances. Expired or unknown IDs reject with `ResultUnavailableError`.
The adapter scans bounded batches of completed and failed jobs on later submissions and recurring deliveries,
removing expired ordinary records. Reads enforce expiry even before cleanup runs.
BullMQ's queue-wide age/count removal is disabled because it can also delete records
that individual jobs asked to retain permanently.
Identical submissions with the same ID are idempotent before its outcome expires;
reusing the ID for a different message rejects. A retained expired ID rejects too.
Use a fresh ID for new work; the core job system generates these automatically.

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
Closing drains this backend's workers, rejects its waits, and closes its owned
connections. Other processes and accepted queued jobs are unaffected. Redis
requests and worker startup have a five-second bound; active handlers must settle
for graceful worker shutdown to finish.

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
pnpm --filter core build
JOB_SYSTEM_REDIS_PORT=16379 pnpm --filter core test
```

Integration tests create unique queues and remove only their own queues. The suite
includes submit-only producer processes that exit before fresh and replacement
workers start, a fresh reader of a completed result, conflicting operation
submissions, retention, and a worker killed after a
database commit. The crash test waits for BullMQ's real stalled-worker recovery,
usually about a minute. Redis integration tests skip without the port variable.

To also test a Redis process crash and recovery from AOF, supply a local binary:

```sh
JOB_SYSTEM_REDIS_PORT=16379 JOB_SYSTEM_REDIS_SERVER=/path/to/redis-server pnpm --filter core test
```

That test owns a separate temporary Redis process, port, and data directory. It
uses `appendfsync always`; it does not restart the server supplied by the port variable.

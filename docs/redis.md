# Redis backend

`RedisBackend` connects the core job system to a BullMQ queue. It owns its Redis
connections; pass connection settings, not an existing Redis client.

```ts
import { createJobSystem, RedisBackend } from "core";

const jobs = createJobSystem({
  container,
  jobs: [updateMemory],
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

Calling `updateMemory` starts processing on first use and sends the job through
Redis; `result()` awaits its output. Each system owns a worker; systems on the same queue share jobs and must
use compatible catalogs and codecs with their own dependency containers. The
lower-level backend contract also supports separate producer and worker processes.

Each job's `metadata.retries` becomes BullMQ `attempts` and `backoff` at submission,
and `metadata.key` becomes a BullMQ deduplication id that is held while the job is
queued, delayed, or active. A retryable failure before the final attempt is thrown
to BullMQ so it schedules the next delivery; the final outcome is retained like a
successful output. Executor rejection is infrastructure failure and shares the same
attempt budget. Delivery can repeat; handlers must account for repeated side effects.

Results remain available for the configured TTL after completion, including to
new backend instances. Expired or unknown IDs reject with `ResultUnavailableError`.
BullMQ cleans up old records opportunistically when later jobs finish; the adapter
checks expiry on every result read even when physical cleanup has not run yet.
Identical submissions with the same ID are idempotent before its outcome expires;
reusing the ID for a different message rejects. A retained expired ID rejects too.
Use a fresh ID for new work; the core job system generates these automatically.

Aborting a result wait affects only that wait. Accepted jobs continue running.
Closing drains this backend's workers, rejects its waits, and closes its owned
connections. Other processes and accepted queued jobs are unaffected. Redis
requests and worker startup have a five-second bound; active handlers must settle
for graceful worker shutdown to finish.

Build and test against an isolated Redis server:

```sh
pnpm --filter core build
JOB_SYSTEM_REDIS_PORT=16379 pnpm --filter core test
```

Integration tests create unique queues and remove only their own queues. The suite
includes separate producer and worker processes and a fresh reader of a completed
result. Redis integration tests are skipped when the environment variable is absent.

# PostgresBackend

Postgres handles the entire backend: delivery, worker leases, retries, delayed jobs,
recurring schedules, inspection, and retained outcomes. No Redis or Temporal service
is required. The package depends on `core` and `pg`; it has no Adapt application dependency.

```ts
import { Pool } from "pg";
import { createJobSystem, defineJob } from "core";
import { PostgresBackend } from "postgres-backend";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 5_000,
  statement_timeout: 10_000,
});
const backend = new PostgresBackend({ pool, queue: "adapt" });
await backend.migrate();

const double = defineJob({ deps: [], handler: (input: number) => input * 2 });
const jobs = createJobSystem({ jobs: { double }, backend, concurrency: 4 });
try {
  console.log(await double(21).result());
} finally {
  await jobs.close();
  await pool.end();
}
```

In Adapt v2, pass the existing `database.$client` pool instead of creating another
one. The backend borrows it: `close()` drains workers and cancels local result waits,
but never closes the application's pool. Size the pool for application queries and
job claims/heartbeats; configure connection and statement timeouts on the pool.

## Schema and initialization

`backend.migrate()` applies versioned migrations inside the `job_system` schema.
Concurrent migration calls serialize using a transaction advisory lock. The package
owns `jobs`, `schedules`, and `migrations`; application tables are untouched.
Run migrations before constructing a consuming job system. Migration credentials
need schema/table creation privileges; ordinary producers and workers need only the
appropriate access to these tables. The migrations are packaged with the adapter,
not with v2's application schema.

Initialization verifies table access. `createJobSystem({ worker: false, ... })` is
a producer only; workers sharing its database and queue must register a compatible
catalog and codec. Different queue names isolate execution and idempotency IDs.

## Delivery and recovery

Workers poll every 250 ms by default (`pollIntervalMs`). They atomically claim due
jobs with `FOR UPDATE SKIP LOCKED`; saturated job types remain queued so other types
can use available capacity. Concurrency limits apply per worker, including hooks and cleanup.

Each claim has a fresh token and a 30-second lease (`leaseDurationMs`), renewed every
third of that interval. Only an unexpired matching token can renew, finish, or release
the job. Expired claims are recoverable by another worker without spending a business
attempt. A lost lease aborts the old handler's signal; its result cannot overwrite the
new owner's record. Database errors stop the affected worker and reach the system's
`onError` callback after its active work drains. Restart the system after worker failure.

Normal shutdown stops intake and drains active executions while continuing lease
renewal. If the job-system grace period expires, cooperative handlers settle and hand
their jobs back without spending an attempt. An ignoring handler retains its lease while
the process and database connection can renew it. Process death or a lost connection
eventually permits recovery. This is at-least-once execution: fencing database results
cannot prevent a disconnected old worker from making external effects. Use stable
operation IDs and transactional receipts/provider idempotency for those effects.

Handler failures use their submitted retry budget and backoff. Executor exceptions
also spend that budget; exhausted infrastructure failures reject `result()` and remain
visible as failed records through `get()`. A crash or settled shutdown handoff spends none.

## Retention

Successful and failed executions both default to 90 days. Set `resultTTLSeconds`
and `failureTTLSeconds` independently; `null` retains that category indefinitely.
Policies are saved at submission, so a new reader cannot change an existing record's
expiry. `idempotencyKey` records always retain their input and outcome indefinitely.
The same key with changed input rejects, including after completion.

`get()` and `result()` read Postgres directly, including from fresh processes after a
restart. Records include input, final output/error, total attempts, creation time,
latest start time, and finish time. Individual attempt logs and a history-listing API
are not part of the current backend contract.

Reads enforce expiry immediately. Workers prune up to 1,000 expired records per minute;
`backend.prune()` allows additional cleanup, including in a producer-only deployment.
Idempotency records and records with indefinite retention are never pruned automatically.

## Schedules

The existing `after`, `at`, `every`, `daily`, and `cron` call options work unchanged.
Store `schedule.id` and reopen it with `jobs.schedule(id)` to update or remove it.
The same registration name and canonical input identify one recurring schedule.
Changing its rule updates future timing; repeating the same registration preserves
the next occurrence. Queue names isolate schedules too.

Workers create each due occurrence and advance the schedule in one transaction.
Concurrent workers cannot produce the same occurrence twice. Each occurrence has an
independent execution ID and deduplication/idempotency scope. Recurrence can overlap
within the worker's configured concurrency. After downtime, one overdue occurrence
is accepted and the schedule advances to the next future time; missed intervals are
not replayed individually. Without workers, schedules remain stored but do not emit work.

Removal stops future emissions and leaves already accepted jobs and their retries
alone. Updating a removed schedule rejects. Calendar rules use the same cron-parser
and explicit IANA timezone handling as core, including daylight-saving transitions.

## Tests

```sh
pnpm --filter postgres-backend... build
pnpm --filter postgres-backend test
```

Tests start an isolated embedded Postgres by default, using a temporary data directory.
`embedded-postgres` and its native build-script permissions are test-only dependencies.
To use a dedicated existing test database instead, set `JOB_SYSTEM_POSTGRES_URL`.
Tests create the backend schema and remove only their generated queue records. They
also create a `job_system.test_effects` fixture table. Do not point them at an application database.

The suite exercises concurrent producers/workers, DI, retry backoff, caller cancellation,
schedules, long-term retention, graceful handoff, stale-owner fencing, and a child worker
killed after a database effect commits. No Postgres scenarios silently skip.

Implementation references: PostgreSQL [row locking](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
and node-postgres [transactions](https://node-postgres.com/features/transactions).

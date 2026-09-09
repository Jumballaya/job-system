# V1 replacement acceptance contracts

Exactly six top-level tests, numbered to match the agreed gaps. Tests 1–3 pass;
4–6 remain intentionally red until implemented. They are separate from the existing regression
suite; no tests use `skip`, `todo`, unconditional failure, or feature-detection fallbacks.
Passing establishes these scenarios, not proof against every possible failure.

```sh
JOB_SYSTEM_REDIS_SERVER=/path/to/redis-server pnpm test:acceptance

# Build first, then run one contract while implementing it.
pnpm --filter core build
JOB_SYSTEM_REDIS_SERVER=/path/to/redis-server node --test --test-name-pattern='^1 ' packages/core/test/acceptance/v1.test.mjs
```

Tests 1–5 each own a Redis process with AOF, a temporary directory, and separate
producer/worker processes. No existing Redis instance is used or modified. Missing
Redis is a failing prerequisite, never a silent skip. Test 6 injects a fatal failure
at the public backend boundary. Process cleanup runs even when an assertion fails.

| # | Passing scenario |
|---|---|
| 1 | Delayed and absolute deliveries are not early; immediate work can pass them. Concurrent producers register one schedule with canonical input identity. Recurrence continues after producer exit and Redis/worker restart. A fresh process changes interval/daily/cron rules and removes it by ID; removal survives another restart and later updates reject. A controlled-clock process checks hourly intervals, daily DST adjustment, weekday cron, and removal. |
| 2 | A blocked recurring occurrence allows a distinct successor. Ordinary same-key work still coalesces while running and during retry, without conflating different job definitions. Accepted occurrences drain after schedule removal. |
| 3 | A backlog limited to one concurrent handler cannot prevent another job type from executing. Capacity waits preserve deduplication and spend no execution attempts. The backlog later drains exactly once without exceeding its job limit or total worker capacity. |
| 4 | Shutdown stops intake, signals cooperative cancellation, and reports its deadline rather than waiting forever. A replacement cannot execute work still owned by live handlers or unfinished cleanup. After the old process dies, unfinished work recovers without spending a business retry; the replacement shuts down without retained resources. |
| 5 | A new process inspects work submitted by an exited producer through queued/running/succeeded/failed states, including inputs, timestamps, attempts, outputs, and errors. Failure history outlives ordinary result retention and Redis restart. Unknown/expired records return null. |
| 6 | Startup failure and later idle-worker failure each notify the host once, without needing a failing submission. Future callers see the original error; connections close. Job errors and ordinary shutdown do not trigger the worker-error hook. |

## Interfaces used by the tests

Timing and schedule handles are implemented. Shutdown deadlines, inspection,
failure retention, and worker error reporting below remain draft interfaces for
future implementation. Refine those spellings while retaining the behavior above.

- `job(input, { after: "500ms" })` and `job(input, { at: isoTimestamp })` return existing job handles.
- `job(input, { every: "1h" })`, `{ daily: "09:00", timezone }`, and `{ cron, timezone }`
  return schedule handles. Re-registering the same job and canonical input addresses
  the same schedule. Intervals first fire after one interval.
- `jobs.schedule(id)` reacquires a schedule handle with `update(timing)` and `remove()`.
  Removal is idempotent and stops future occurrences; already-running work drains.
- `createJobSystem({ shutdownTimeoutMs: 100, ... })`; `close()` rejects with
  `ShutdownTimeoutError` when live work cannot drain by that deadline. The timeout
  is not permission to run overlapping replacements. Host process termination and
  backend recovery complete the handoff.
- `jobs.get(id)` returns `null` or a record with `id`, `name`, `input`, `status`,
  `attempts`, numeric Unix-millisecond `createdAt`/`startedAt`/`finishedAt`, and
  `output` or `error` when terminal. Status is queued/running/succeeded/failed.
- `RedisBackend({ resultTTLSeconds, failureTTLSeconds, ... })` separates ordinary
  successful-result retention from failed-execution history.
- `createJobSystem({ onError(error), ... })` reports infrastructure failure to the host.

The calendar helper is part of test 1, not another top-level test. Its clock is
isolated in a child process; Redis scenarios use actual time and process lifetimes.
Test 4 may take about a minute once implemented because it exercises real lease recovery.

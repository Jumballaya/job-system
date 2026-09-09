# TemporalBackend

Use the same callable jobs with an existing Temporal deployment. Temporal SDK dependencies live in this optional package.

```ts
import { createJobSystem, defineJob } from "core";
import { TemporalBackend } from "temporal-backend";

const double = defineJob({ deps: [], handler: (input: number) => input * 2 });
const jobs = createJobSystem({
  jobs: { double },
  backend: new TemporalBackend({
    client, // Your @temporalio/client Client, already configured for its namespace.
    taskQueue: "adapt-jobs",
    worker: { connection: nativeConnection }, // Your @temporalio/worker NativeConnection.
  }),
});

await double(21).result();
await double(21, { after: "10m" });
const schedule = await double(21, { every: "1h" });
await schedule.update({ daily: "09:00", timezone: "America/New_York" });
await schedule.remove();

await jobs.close(); // During application shutdown.
```

Both connections are borrowed and must target the same deployment. The namespace comes from `client`.
The application closes its connections after its workers finish. A producer uses
`createJobSystem({ worker: false, ... })` and needs only `client` and `taskQueue` in the backend.
Initialization starts consumption; there is no separate job-system startup call.

## Existing Temporal worker code

The adapter creates and drains a Temporal Worker. Pass existing activities, interceptors,
deployment settings, and other Temporal worker options through `worker`. To share the worker
with your workflows, include this export in your existing workflow entry:

```ts
export { jobSystemWorkflow } from "temporal-backend/workflows";
export { myExistingWorkflow } from "./my-workflow.js";
```

Then pass your `workflowsPath` or prebuilt `workflowBundle` and `activities` in `worker`.
The adapter adds the reserved `jobSystemExecute` activity. It sets task queue, namespace,
and activity concurrency from the backend/job-system configuration. Configure versioning
and custom data conversion consistently across your client and workers.

A running Temporal Worker cannot acquire new registrations dynamically. Add these exports
when constructing/deploying the worker; this adapter does not attach to an already-running
Worker instance. All consumers of this task queue must include the job workflow/activity.

## Delivery and retention

- Handlers and DI run in Activities. Workflows contain only durable coordination, timers,
  and serializable state. Never import core's Node runtime into a workflow bundle.
- Job outcomes control business retries and backoff inside the Workflow. Activity failures
  recover infrastructure interruptions without spending a business attempt. Applications
  should throw `NonRetryableError` for permanent handler failures.
- `metadata.key` coalesces active workflows. Handles include a run ID, so a subsequent
  same-key job cannot change an earlier handle's result.
- `metadata.idempotencyKey` keeps one open workflow per operation, including its input and
  terminal outcome. Every 30 days it continues as new, retaining that record without rerunning
  the handler. This deliberately incurs one open execution and a periodic timer per key;
  deleting/terminating these records or removing their namespace removes that protection.
- Ordinary completed results follow the namespace's workflow history retention. Success and
  business failure are retained outcomes; inspect with `jobs.get(id)`. Business failures appear
  as completed Temporal workflows carrying a failed job outcome.
- Inspection uses workflow queries, so a compatible worker must be available for replay.
  Never-started queued records can be inspected without one; ordinary `result()` reads retained
  workflow results without a worker. Open idempotency records require a worker to answer queries.
- Schedules use Temporal Schedules with overlapping occurrences allowed. Each occurrence has
  separate execution identity. Interval schedules require whole seconds; cron and timezone
  interpretation follows Temporal. Removing a schedule leaves accepted occurrences intact.

## Cancellation and shutdown

Canceling a result wait only cancels that caller's RPC. It does not cancel the workflow.
Job timeout and the job-system shutdown deadline abort the handler's signal cooperatively.
The Activity waits for handler cleanup before handing off or allowing a business retry.

Activities request heartbeats every five seconds (SDK-throttled), with a 30-second timeout. The Activity's
start-to-close ceiling is 365 days; ordinary job deadlines still come from job metadata.
Worker shutdown defaults to draining while `jobs.close()` enforces its configured deadline.
Overriding Temporal shutdown settings can request cancellation earlier.

Temporal recovery is at-least-once: worker crashes or lost heartbeats can cause redelivery.
Idempotency records cannot atomically commit an external side effect together with Activity
completion. Use downstream operation keys or database constraints for those effects.

## Verification

```sh
pnpm build
pnpm --filter temporal-backend test
```

Tests start isolated Temporal servers, including a time-skipping server for month-long
idempotency retention. The SDK downloads their executables on first use. Missing server
support fails the tests; none silently skip. SDK version tested: 1.23.0.

The package skips dependency declaration checking because the SDK's `ms` dependency omits
NodeNext type exports; the adapter itself remains strictly type-checked.

Temporal references: [Workers](https://docs.temporal.io/develop/typescript/core-application#run-a-worker),
[Schedules](https://docs.temporal.io/develop/typescript/schedules),
[Continue-As-New](https://docs.temporal.io/develop/typescript/continue-as-new).

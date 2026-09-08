import { Container, createJobSystem, defineJob, JobExecutionError } from "core";
import { createBackend } from "./backend.js";
import { add } from "./jobs/add.js";
import { audit } from "./jobs/audit.js";
import { explode } from "./jobs/explode.js";
import { flaky } from "./jobs/flaky.js";
import { sleep } from "./jobs/sleep.js";
import { stall } from "./jobs/stall.js";
import { syncUser } from "./jobs/sync.js";
import { Counter } from "./services/counter.js";
import { Auditor, RequestScope } from "./services/request-scope.js";

const container = new Container();
container.register(Counter);
container.register(RequestScope, { useFactory: () => new RequestScope(), lifetime: "scoped" });
container.register(Auditor, {
  useFactory: (resolver) => new Auditor(resolver.resolve(RequestScope)),
  lifetime: "scoped",
});

const { backend, label } = createBackend();
const jobs = createJobSystem({ container, jobs: [add, audit, sleep, explode, flaky, stall, syncUser], backend, concurrency: 4 });
console.log(`Backend: ${label}`);

function section(title: string): void {
  console.log(`\n== ${title}`);
}

try {
  section("Singleton dependency: both calls share one Counter");
  console.log("  total:", await add({ amount: 1 }).result());
  console.log("  total:", await add({ amount: 2 }).result());

  section("Fire and forget: the call returns once the backend accepts the work");
  const handle = await add({ amount: 10 });
  console.log(`  accepted job ${handle.id.slice(0, 8)}; result read later:`, await handle.result());
  void add({ amount: 100 });
  await new Promise((resolve) => setTimeout(resolve, 100));

  section("Scoped dependency: one RequestScope per execution, shared by its Auditor");
  const first = await audit({ action: "login" }).result();
  const second = await audit({ action: "logout" }).result();
  console.log("  first: ", first);
  console.log("  second:", second);
  console.log("  distinct scopes:", first.scopeId !== second.scopeId);

  section("Concurrency 4: four 500 ms jobs finish together");
  const started = Date.now();
  const labels = await Promise.all(
    ["a", "b", "c", "d"].map((name) => sleep({ ms: 500, label: name }).result()),
  );
  console.log(`  ${labels.join(",")} in ${Date.now() - started} ms`);

  section("Retries: fixed 200 ms backoff, succeeds on the third attempt");
  console.log("  result:", await flaky({ succeedOn: 3 }).result());

  section("Retries exhausted: the last failure reaches the caller as JobExecutionError");
  try {
    await flaky({ succeedOn: 99 }).result();
  } catch (error) {
    if (!(error instanceof JobExecutionError)) throw error;
    console.log(`  caught ${error.failure.name}: ${error.failure.message} (job ${error.jobId.slice(0, 8)})`);
  }

  section("NonRetryableError: fails on the first attempt despite the default three");
  try {
    await explode({ quota: 10 }).result();
  } catch (error) {
    if (!(error instanceof JobExecutionError)) throw error;
    console.log(`  caught ${error.failure.name}: ${error.failure.message}`);
  }

  section("Timeout: 150 ms budget aborts the handler's signal, then one retry");
  try {
    await stall({ label: "stuck" }).result();
  } catch (error) {
    if (!(error instanceof JobExecutionError)) throw error;
    console.log(`  caught ${error.failure.name}: ${error.failure.message}`);
  }

  section("Dedupe key: a second call for the same user reuses the active job");
  const one = await syncUser({ userId: 1 });
  const again = await syncUser({ userId: 1 });
  const two = await syncUser({ userId: 2 });
  console.log(`  same job: ${one.id === again.id}, different user: ${one.id !== two.id}`);
  console.log("  results:", await Promise.all([again.result(), two.result()]));

  section("Per-job concurrency 1: three users sync one at a time inside a 4-slot worker");
  const syncStarted = Date.now();
  await Promise.all([3, 4, 5].map((userId) => syncUser({ userId }).result()));
  console.log(`  three 200 ms syncs took ${Date.now() - syncStarted} ms`);

  section("Cancelled wait: the caller stops waiting, the accepted job still completes");
  const controller = new AbortController();
  const orphan = await sleep({ ms: 300, label: "orphan" });
  setTimeout(() => controller.abort(new Error("caller gave up")), 50);
  try {
    await orphan.result({ signal: controller.signal });
  } catch (error) {
    console.log("  wait rejected:", error instanceof Error ? error.message : error);
  }
  console.log("  same handle, patient wait:", await orphan.result());

  section("Codec guard: non-JSON input is rejected before submission");
  try {
    await add({ amount: new Date() as unknown as number });
  } catch (error) {
    console.log("  rejected:", error instanceof Error ? error.message : error);
  }

  section("Unattached definition: not part of this system's catalog");
  const stray = defineJob({ name: "stray", deps: [], handler: () => "never" });
  try {
    await stray(undefined);
  } catch (error) {
    console.log("  rejected:", error instanceof Error ? error.message : error);
  }
} finally {
  section("Close: drain the worker and release connections");
  await jobs.close();
  console.log("  closed");
}

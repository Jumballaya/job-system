import { Container, createJobSystem } from "core";
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

export function createDemoSystem(worker = true) {
  if (!worker && process.env.JOB_BACKEND === "memory") throw new Error("Remote workers require Redis in this demo");
  const container = new Container();
  if (worker) {
    container.register(Counter);
    container.register(RequestScope, {
      useFactory: () => new RequestScope(),
      lifetime: "scoped",
    });
    container.register(Auditor, {
      useFactory: (resolver) => new Auditor(resolver.resolve(RequestScope)),
      lifetime: "scoped",
    });
  }
  const { backend, label } = createBackend();
  const jobs = createJobSystem({
    container, backend, worker, concurrency: 4,
    jobs: { add, audit, sleep, explode, flaky, stall, syncUser },
  });
  return { jobs, label };
}

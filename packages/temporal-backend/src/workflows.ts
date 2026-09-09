import { condition, continueAsNew, defineQuery, defineSignal, proxyActivities, setHandler, sleep, workflowInfo } from "@temporalio/workflow";
import type { JobMessage, JobOutcome } from "core";
import { INSPECT, STARTED } from "./protocol.js";
import type { Activities, Snapshot } from "./protocol.js";

/** Import this entry in an existing workflow bundle; it contains no application or Node runtime code. */
export async function jobSystemWorkflow(submitted: JobMessage, retained?: Snapshot): Promise<JobOutcome> {
  const info = workflowInfo();
  const message = { ...submitted, id: submitted.policy.idempotencyKey !== undefined
    ? info.workflowId : `${info.workflowId}/${info.firstExecutionRunId}` };
  const snapshot: Snapshot = retained ?? { message, nextAttempt: 1, record: {
    id: message.id, name: message.name, input: message.input, attempts: 0, createdAt: info.startTime.getTime(), status: "queued",
  } };
  setHandler(defineQuery<Snapshot>(INSPECT), () => snapshot);
  setHandler(defineSignal<[number, number]>(STARTED), (attempt, startedAt) => {
    if (snapshot.outcome) return;
    snapshot.record = { ...snapshot.record, status: "running", attempts: attempt, startedAt };
  });
  const activities = proxyActivities<Activities>({
    startToCloseTimeout: "365 days", heartbeatTimeout: "30 seconds",
    retry: { initialInterval: "1 second", maximumInterval: "30 seconds" },
    cancellationType: "WAIT_CANCELLATION_COMPLETED",
  });
  if (!snapshot.outcome) {
    if (message.availableAt && message.availableAt > Date.now()) await sleep(message.availableAt - Date.now());
    for (;;) {
      if (workflowInfo().continueAsNewSuggested) return continueAsNew<typeof jobSystemWorkflow>(submitted, snapshot);
      const attempt = snapshot.nextAttempt;
      const result = await activities.jobSystemExecute(message, attempt);
      if ("busy" in result) {
        await sleep(100);
        continue;
      }
      const { outcome } = result;
      if (outcome.status === "failed" && outcome.retryable && attempt < message.policy.attempts) {
        snapshot.record = { ...snapshot.record, status: "queued" };
        const { delay, type } = message.policy.backoff;
        await sleep(type === "fixed" ? delay : Math.round(delay * 2 ** (attempt - 1)));
        snapshot.nextAttempt++;
        continue;
      }
      snapshot.outcome = outcome;
      const terminal = { ...snapshot.record, startedAt: snapshot.record.startedAt ?? Date.now(), finishedAt: Date.now() };
      snapshot.record = outcome.status === "succeeded"
        ? { ...terminal, status: "succeeded", output: outcome.output }
        : { ...terminal, status: "failed", error: outcome.error };
      break;
    }
  }
  if (message.policy.idempotencyKey !== undefined) {
    // Keep operation records outside namespace closed-history retention; compact without executing again.
    await condition(() => false, "30 days");
    return continueAsNew<typeof jobSystemWorkflow>(submitted, snapshot);
  }
  return snapshot.outcome!;
}

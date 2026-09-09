import { proxyActivities } from "@temporalio/workflow";
export { jobSystemWorkflow } from "../dist/workflows.js";

export async function existingWorkflow(input) {
  return proxyActivities({ startToCloseTimeout: "1 minute" }).existingActivity(input);
}

import type { JobMessage, JobOutcome, JobRecord } from "core";

export const WORKFLOW = "jobSystemWorkflow";
export const ACTIVITY = "jobSystemExecute";
export const INSPECT = "jobSystemInspect";
export const STARTED = "jobSystemStarted";

export type Snapshot = { message: JobMessage; record: JobRecord<string, string>; nextAttempt: number; outcome?: JobOutcome };
export type ActivityResult = { busy: true } | { outcome: JobOutcome };
export type Activities = { jobSystemExecute(message: JobMessage, attempt: number): Promise<ActivityResult> };

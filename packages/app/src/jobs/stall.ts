import { setTimeout as delay } from "node:timers/promises";
import { defineJob } from "core";

/** Never finishes on its own; the timeout aborts the signal and the attempt fails. */
export const stall = defineJob({
  deps: [],

  async handler(_input: { label: string }, signal) {
    await delay(60_000, undefined, { signal });
    return "unreachable";
  },

  metadata: { timeout: 150, retries: { attempts: 2, backoff: { type: "fixed", delay: 50 } } },

  onError(error, context) {
    const name = error instanceof Error ? error.name : String(error);
    console.log(`  [hook] stall attempt ${context.attempt}/${context.maxAttempts}: ${name}, signal aborted=${context.signal.aborted}`);
  },
});

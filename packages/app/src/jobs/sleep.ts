import { setTimeout as delay } from "node:timers/promises";
import { defineJob } from "core";

/** Cooperative sleep: the worker's signal interrupts the timer. */
export const sleep = defineJob({
  deps: [],

  async handler(input: { ms: number; label: string }, signal) {
    await delay(input.ms, undefined, { signal });
    return input.label;
  },

  metadata: { retries: { attempts: 1 } },

  onSuccess(output) {
    console.log(`  [hook] sleep "${output}" finished`);
  },
});

import { defineJob } from "core";
import { Counter } from "../services/counter.js";

export const add = defineJob({
  name: "counter.add",
  deps: [Counter],

  async handler(input: { amount: number }, counter, signal) {
    signal.throwIfAborted();
    return counter.add(input.amount);
  },

  onSuccess(output, context) {
    console.log(`  [hook] counter.add ${context.jobId.slice(0, 8)} succeeded with ${output}`);
  },
});

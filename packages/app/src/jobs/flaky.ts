import { defineJob } from "core";

/** Fails until the requested attempt; retried by policy without any handler code. */
export const flaky = defineJob({
  deps: [],

  async handler(input: { succeedOn: number }, signal) {
    signal.throwIfAborted();
    return `succeeded on attempt ${input.succeedOn}`;
  },

  metadata: {
    retries: { attempts: 3, backoff: { type: "fixed", delay: 200 } },
  },

  beforeRun(input, context) {
    if (context.attempt < input.succeedOn)
      throw new Error(`not yet (attempt ${context.attempt})`);
  },

  onError(error, context) {
    const state =
      context.attempt < context.maxAttempts ? "will retry" : "giving up";
    console.log(
      `  [hook] flaky attempt ${context.attempt}/${context.maxAttempts} failed, ${state}`,
    );
  },
});

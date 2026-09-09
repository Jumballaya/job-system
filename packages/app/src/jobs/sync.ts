import { setTimeout as delay } from "node:timers/promises";
import { defineJob } from "core";

/** One active sync per user, and only one sync running at a time in this process. */
export const syncUser = defineJob({
  deps: [],

  async handler(input: { userId: number }, signal) {
    await delay(200, undefined, { signal });
    return `synced ${input.userId}`;
  },

  metadata: { key: (input) => `user:${input.userId}`, concurrency: 1 },
});

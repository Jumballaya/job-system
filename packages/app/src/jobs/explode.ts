import { defineJob, NonRetryableError } from "core";

/** A permanent failure: extending NonRetryableError skips the retry budget. */
export class QuotaError extends NonRetryableError {
  constructor(public readonly quota: number) {
    super(`Quota of ${quota} exceeded`);
    this.name = "QuotaError";
  }
}

export const explode = defineJob({
  deps: [],

  async handler(input: { quota: number }): Promise<never> {
    throw new QuotaError(input.quota);
  },

  onError(error, context) {
    const detail = error instanceof QuotaError ? `quota=${error.quota}` : String(error);
    console.log(`  [hook] explode attempt ${context.attempt}/${context.maxAttempts} failed: ${detail}`);
  },
});

/** Per-worker admission limits. A reservation lasts through hooks and asynchronous cleanup. */
export class ConcurrencyLimits {
  private readonly jobs = new Map<string, { limit: number; active: number }>();

  constructor(limits: Readonly<Record<string, number>> = {}) {
    for (const [name, limit] of Object.entries(limits)) {
      if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("Job concurrency must be a positive safe integer");
      this.jobs.set(name, { limit, active: 0 });
    }
  }

  /** Undefined leaves work queued; a release function reserves capacity without waiting. */
  public acquire(name: string): (() => void) | undefined {
    const job = this.jobs.get(name);
    if (!job) return () => {};
    if (job.active === job.limit) return undefined;
    job.active++;
    return () => { job.active--; };
  }
}

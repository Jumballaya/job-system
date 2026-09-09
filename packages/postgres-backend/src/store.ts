import { randomUUID } from "node:crypto";
import { assertSameSubmission, backoffDelay, prepareSubmission, ResultUnavailableError } from "core/backend";
import type { JobFailure, JobMessage, JobOutcome, JobPolicy, JobRecord } from "core/backend";
import { nextRun, occurrence, scheduleId, validateRule } from "core/scheduling";
import type { ScheduleRule } from "core/scheduling";
import type { Pool, PoolClient } from "pg";

export type Retention = { result: number | null; failure: number | null };
type StoredJob = {
  queue: string; id: string; name: string; input: string; policy: JobPolicy;
  state: JobRecord["status"]; attempts: number;
  created_at: Date; available_at: Date; started_at: Date | null; finished_at: Date | null;
  lease_token: string | null; outcome: JobOutcome | null; infrastructure_error: JobFailure | null;
  expired: boolean;
};
export type Claim = { message: JobMessage; token: string; attempt: number };

/** Owns row identity, scheduling transactions, and fenced state transitions. */
export class PostgresStore {
  constructor(private readonly pool: Pool, private readonly queue: string, private readonly retention: Retention) {}

  private async transaction<T>(run: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await run(client);
      await client.query("COMMIT");
      client.release();
      return result;
    } catch (error) {
      try { await client.query("ROLLBACK"); }
      catch (rollbackError) {
        client.release(true);
        throw new AggregateError([error, rollbackError], "Job transaction and rollback failed");
      }
      client.release();
      throw error;
    }
  }

  async ready(): Promise<void> {
    await this.pool.query("SELECT id FROM job_system.jobs LIMIT 0");
    await this.pool.query("SELECT id FROM job_system.schedules LIMIT 0");
  }

  async submit(message: JobMessage): Promise<string> {
    return this.transaction(async (client) => {
      const key = JSON.stringify([this.queue, message.name, message.policy.key ?? message.id]);
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [key]);
      for (;;) {
        if (await this.insert(client, message, this.retention)) return message.id;
        const { rows: [existing] } = await client.query<StoredJob>(`SELECT *,
          coalesce(expires_at <= clock_timestamp(), false) AS expired FROM job_system.jobs
          WHERE queue = $1 AND (id = $2 OR (name = $3 AND dedupe_key = $4 AND state IN ('queued', 'running')))
          ORDER BY (id = $2) DESC LIMIT 1`, [this.queue, message.id, message.name, message.policy.key ?? null]);
        // Completion can release the partial unique key between INSERT and SELECT; retry insertion then.
        if (!existing) continue;
        if (existing.id !== message.id) return existing.id;
        if (existing.expired) throw new ResultUnavailableError(message.id);
        assertSameSubmission({ ...message, name: existing.name, input: existing.input, policy: existing.policy }, message);
        return existing.id;
      }
    });
  }

  private async insert(client: PoolClient, message: JobMessage, retention: Retention): Promise<boolean> {
    const result = await client.query(`INSERT INTO job_system.jobs
      (queue, id, name, input, policy, dedupe_key, available_at, result_retention_ms, failure_retention_ms)
      VALUES ($1, $2, $3, $4, $5, $6, coalesce($7::timestamptz, clock_timestamp()), $8, $9)
      ON CONFLICT DO NOTHING`, [this.queue, message.id, message.name, message.input, message.policy,
      message.policy.key ?? null, message.availableAt === undefined ? null : new Date(message.availableAt),
      retention.result, retention.failure]);
    return result.rowCount === 1;
  }

  private async read(id: string): Promise<StoredJob | null> {
    const { rows: [row] } = await this.pool.query<StoredJob>(`SELECT *,
      coalesce(expires_at <= clock_timestamp(), false) AS expired
      FROM job_system.jobs WHERE queue = $1 AND id = $2`, [this.queue, id]);
    return row && !row.expired ? row : null;
  }

  async get(id: string): Promise<JobRecord<string, string> | null> {
    const row = await this.read(id);
    return row ? this.record(row) : null;
  }

  async outcome(id: string): Promise<JobOutcome | null> {
    const row = await this.read(id);
    if (!row) throw new ResultUnavailableError(id);
    if (row.infrastructure_error) throw new Error(`Job infrastructure failed: ${row.infrastructure_error.message}`);
    return row.outcome;
  }

  private record(row: StoredJob): JobRecord<string, string> {
    const base = { id: row.id, name: row.name, input: row.input, attempts: row.attempts, createdAt: row.created_at.getTime() };
    if (row.state === "queued") return { ...base, status: "queued", ...(row.started_at ? { startedAt: row.started_at.getTime() } : {}) };
    const startedAt = row.started_at!.getTime();
    if (row.state === "running") return { ...base, status: "running", startedAt };
    const terminal = { ...base, startedAt, finishedAt: row.finished_at!.getTime() };
    if (row.outcome?.status === "succeeded") return { ...terminal, status: "succeeded", output: row.outcome.output };
    return { ...terminal, status: "failed", error: row.infrastructure_error ??
      (row.outcome?.status === "failed" ? row.outcome.error : { name: "Error", message: "Missing job outcome" }) };
  }

  async claim(excluded: string[], leaseMs: number): Promise<Claim | null> {
    const token = randomUUID();
    const { rows: [row] } = await this.pool.query<StoredJob>(`WITH candidate AS (
      SELECT id FROM job_system.jobs WHERE queue = $1 AND NOT (name = ANY($2::text[])) AND
        ((state = 'queued' AND available_at <= clock_timestamp()) OR
         (state = 'running' AND lease_until <= clock_timestamp()))
      ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT 1
    ) UPDATE job_system.jobs AS job SET state = 'running', lease_token = $3,
      lease_until = clock_timestamp() + $4 * interval '1 millisecond', started_at = clock_timestamp(),
      attempts = attempts + CASE WHEN state = 'queued' THEN 1 ELSE 0 END
      FROM candidate WHERE job.queue = $1 AND job.id = candidate.id RETURNING job.*`,
    [this.queue, excluded, token, leaseMs]);
    return row ? { message: { id: row.id, name: row.name, input: row.input, policy: row.policy,
      availableAt: row.available_at.getTime() }, token, attempt: row.attempts } : null;
  }

  async renew(claim: Claim, leaseMs: number): Promise<boolean> {
    const result = await this.pool.query(`UPDATE job_system.jobs
      SET lease_until = clock_timestamp() + $4 * interval '1 millisecond'
      WHERE queue = $1 AND id = $2 AND lease_token = $3 AND lease_until > clock_timestamp()`,
    [this.queue, claim.message.id, claim.token, leaseMs]);
    return result.rowCount === 1;
  }

  async release(claim: Claim): Promise<void> {
    await this.pool.query(`UPDATE job_system.jobs SET state = 'queued', lease_token = NULL, lease_until = NULL,
      available_at = clock_timestamp(), attempts = greatest(attempts - 1, 0)
      WHERE queue = $1 AND id = $2 AND lease_token = $3 AND lease_until > clock_timestamp()`,
    [this.queue, claim.message.id, claim.token]);
  }

  async finish(claim: Claim, outcome: JobOutcome, infrastructure?: JobFailure): Promise<void> {
    const retry = outcome.status === "failed" && outcome.retryable && claim.attempt < claim.message.policy.attempts;
    if (retry) {
      await this.pool.query(`UPDATE job_system.jobs SET state = 'queued', lease_token = NULL, lease_until = NULL,
        available_at = clock_timestamp() + $4 * interval '1 millisecond'
        WHERE queue = $1 AND id = $2 AND lease_token = $3 AND lease_until > clock_timestamp()`,
      [this.queue, claim.message.id, claim.token, backoffDelay(claim.message.policy, claim.attempt)]);
      return;
    }
    await this.pool.query(`UPDATE job_system.jobs SET state = $4, outcome = $5, infrastructure_error = $6,
      finished_at = clock_timestamp(), lease_token = NULL, lease_until = NULL,
      expires_at = CASE WHEN policy->>'idempotencyKey' IS NOT NULL THEN NULL ELSE clock_timestamp() +
        (CASE WHEN $4 = 'succeeded' THEN result_retention_ms ELSE failure_retention_ms END) * interval '1 millisecond' END
      WHERE queue = $1 AND id = $2 AND lease_token = $3 AND lease_until > clock_timestamp()`,
    [this.queue, claim.message.id, claim.token, outcome.status, infrastructure ? null : outcome, infrastructure ?? null]);
  }

  async saveSchedule(message: JobMessage, rule: ScheduleRule): Promise<string> {
    rule = validateRule(rule);
    const id = scheduleId(message);
    await this.transaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [JSON.stringify([this.queue, id])]);
      const { rows: [clock] } = await client.query<{ now: Date }>("SELECT clock_timestamp() AS now");
      await client.query(`INSERT INTO job_system.schedules
        (queue, id, message, rule, next_at, result_retention_ms, failure_retention_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (queue, id) DO UPDATE SET message = EXCLUDED.message, rule = EXCLUDED.rule,
          next_at = CASE WHEN job_system.schedules.rule = EXCLUDED.rule THEN job_system.schedules.next_at ELSE EXCLUDED.next_at END
        WHERE job_system.schedules.rule <> EXCLUDED.rule OR job_system.schedules.message->'policy' <> EXCLUDED.message->'policy'`,
      [this.queue, id, message, rule, new Date(nextRun(rule, clock.now.getTime())), this.retention.result, this.retention.failure]);
    });
    return id;
  }

  async updateSchedule(id: string, rule: ScheduleRule): Promise<void> {
    rule = validateRule(rule);
    await this.transaction(async (client) => {
      const { rows: [row] } = await client.query<{ now: Date }>(`SELECT clock_timestamp() AS now
        FROM job_system.schedules WHERE queue = $1 AND id = $2 FOR UPDATE`, [this.queue, id]);
      if (!row) throw new Error(`Schedule not found: ${id}`);
      await client.query("UPDATE job_system.schedules SET rule = $3, next_at = $4 WHERE queue = $1 AND id = $2",
        [this.queue, id, rule, new Date(nextRun(rule, row.now.getTime()))]);
    });
  }

  async removeSchedule(id: string): Promise<void> {
    await this.pool.query("DELETE FROM job_system.schedules WHERE queue = $1 AND id = $2", [this.queue, id]);
  }

  async advanceSchedules(): Promise<void> {
    await this.transaction(async (client) => {
      const { rows } = await client.query<{ id: string; message: JobMessage; rule: ScheduleRule; next_at: Date;
        now: Date; result_retention_ms: number | null; failure_retention_ms: number | null }>(`SELECT *, clock_timestamp() AS now
        FROM job_system.schedules WHERE queue = $1 AND next_at <= clock_timestamp()
        ORDER BY next_at FOR UPDATE SKIP LOCKED LIMIT 100`, [this.queue]);
      for (const row of rows) {
        const message = prepareSubmission(occurrence(row.message, `${row.id}:${row.next_at.getTime()}`));
        await this.insert(client, { ...message, availableAt: row.next_at.getTime() },
          { result: row.result_retention_ms, failure: row.failure_retention_ms });
        await client.query("UPDATE job_system.schedules SET next_at = $3 WHERE queue = $1 AND id = $2",
          [this.queue, row.id, new Date(nextRun(row.rule, row.now.getTime()))]);
      }
    });
  }

  async prune(): Promise<number> {
    const result = await this.pool.query(`WITH expired AS (
      SELECT id FROM job_system.jobs WHERE queue = $1 AND expires_at <= clock_timestamp()
      ORDER BY expires_at FOR UPDATE SKIP LOCKED LIMIT 1000
    ) DELETE FROM job_system.jobs AS job USING expired WHERE job.queue = $1 AND job.id = expired.id`, [this.queue]);
    return result.rowCount ?? 0;
  }
}

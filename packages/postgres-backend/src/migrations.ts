import type { Pool } from "pg";

const migrations = [`
  CREATE TABLE job_system.jobs (
    queue text NOT NULL,
    id text NOT NULL,
    name text NOT NULL,
    input text NOT NULL,
    policy jsonb NOT NULL,
    dedupe_key text,
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    lease_token uuid,
    lease_until timestamptz,
    outcome jsonb,
    infrastructure_error jsonb,
    result_retention_ms double precision,
    failure_retention_ms double precision,
    expires_at timestamptz,
    PRIMARY KEY (queue, id),
    CHECK ((state = 'running') = (lease_token IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK ((state IN ('succeeded', 'failed')) = (finished_at IS NOT NULL))
  );
  CREATE UNIQUE INDEX jobs_active_key ON job_system.jobs (queue, name, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND state IN ('queued', 'running');
  CREATE INDEX jobs_available ON job_system.jobs (queue, available_at, id) WHERE state = 'queued';
  CREATE INDEX jobs_expired_lease ON job_system.jobs (queue, lease_until) WHERE state = 'running';
  CREATE INDEX jobs_retention ON job_system.jobs (queue, expires_at) WHERE expires_at IS NOT NULL;
  CREATE TABLE job_system.schedules (
    queue text NOT NULL,
    id text NOT NULL,
    message jsonb NOT NULL,
    rule jsonb NOT NULL,
    next_at timestamptz NOT NULL,
    result_retention_ms double precision,
    failure_retention_ms double precision,
    PRIMARY KEY (queue, id)
  );
  CREATE INDEX schedules_due ON job_system.schedules (queue, next_at);
`];

/** Run once during application setup; concurrent callers serialize on the migration lock. */
export async function migratePostgres(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended('job-system:migrations', 0))");
    await client.query(`CREATE SCHEMA IF NOT EXISTS job_system;
      CREATE TABLE IF NOT EXISTS job_system.migrations (version integer PRIMARY KEY)`);
    const { rows: [row] } = await client.query<{ version: number }>(
      "SELECT coalesce(max(version), 0) AS version FROM job_system.migrations",
    );
    if (row.version > migrations.length) throw new Error("Postgres job schema is newer than this backend");
    for (let version = row.version; version < migrations.length; version++) {
      await client.query(migrations[version]);
      await client.query("INSERT INTO job_system.migrations VALUES ($1)", [version + 1]);
    }
    await client.query("COMMIT");
  } catch (error) {
    try { await client.query("ROLLBACK"); }
    catch (rollbackError) {
      client.release(true);
      throw new AggregateError([error, rollbackError], "Job migration and rollback failed");
    }
    client.release();
    throw error;
  }
  client.release();
}

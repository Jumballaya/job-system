import { MemoryBackend, RedisBackend } from "core";
import type { JobBackend } from "core";

/** JOB_BACKEND=memory switches to in-process execution; Redis is the default. */
export function createBackend(): { backend: JobBackend; label: string } {
  if (process.env.JOB_BACKEND === "memory") {
    return { backend: new MemoryBackend(), label: "memory" };
  }
  const host = process.env.REDIS_HOST ?? "127.0.0.1";
  const port = Number(process.env.REDIS_PORT ?? 6379);
  const backend = new RedisBackend({
    queue: process.env.JOB_QUEUE ?? "app-demo",
    connection: { host, port },
    resultTTLSeconds: 120,
  });
  return { backend, label: `redis ${host}:${port}` };
}

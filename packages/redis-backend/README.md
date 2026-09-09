# RedisBackend

Redis/BullMQ delivery, schedules, and retained outcomes live in this optional package.
Core no longer imports BullMQ or exports `RedisBackend`.

```ts
import { createJobSystem, defineJob } from "core";
import { RedisBackend } from "redis-backend";

const double = defineJob({ deps: [], handler: (input: number) => input * 2 });
const jobs = createJobSystem({
  jobs: { double },
  backend: new RedisBackend({ queue: "example", connection: { host: "127.0.0.1", port: 6379 } }),
});
try {
  console.log(await double(21).result());
} finally {
  await jobs.close();
}
```

See the [Redis backend guide](../../docs/redis.md) for delivery, retention, schedules,
and connection ownership. Existing callers need only change the `RedisBackend` import
and add this package as a dependency; behavior and stored records are unchanged.

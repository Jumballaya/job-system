import assert from "node:assert/strict";
import { mock } from "node:test";
import { createJobSystem, defineJob, MemoryBackend } from "core";

const flush = () => new Promise((resolve) => setImmediate(resolve));
const cases = [
  {
    start: "2026-03-07T13:59:00Z", timing: { daily: "09:00", timezone: "America/New_York" },
    expected: ["2026-03-07T14:00:00.000Z", "2026-03-08T13:00:00.000Z"],
  },
  {
    start: "2026-03-06T21:59:00Z", timing: { cron: "0 17 * * 1-5", timezone: "America/New_York" },
    expected: ["2026-03-06T22:00:00.000Z", "2026-03-09T21:00:00.000Z"],
  },
  {
    start: "2026-01-01T00:00:00Z", timing: { every: "1h" },
    expected: ["2026-01-01T01:00:00.000Z", "2026-01-01T02:00:00.000Z"],
  },
];

for (const scenario of cases) {
  mock.timers.enable({ apis: ["Date", "setTimeout"], now: new Date(scenario.start) });
  const executions = [];
  const record = defineJob({ deps: [], handler() { executions.push(new Date().toISOString()); } });
  const jobs = createJobSystem({ jobs: { record }, backend: new MemoryBackend() });
  try {
    const schedule = await record(null, scenario.timing);
    for (let index = 0; index < scenario.expected.length; index++) {
      const remaining = Date.parse(scenario.expected[index]) - Date.now();
      mock.timers.tick(remaining - 1);
      await flush();
      assert.deepEqual(executions, scenario.expected.slice(0, index), "calendar execution arrived early");
      mock.timers.tick(1);
      await flush();
      assert.deepEqual(executions, scenario.expected.slice(0, index + 1));
    }
    await schedule.remove();
    mock.timers.tick(7 * 24 * 60 * 60 * 1000);
    await flush();
    assert.deepEqual(executions, scenario.expected, "removed schedule kept executing");
  } finally {
    await jobs.close();
    mock.timers.reset();
  }
}

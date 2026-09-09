import { createHash } from "node:crypto";
import cronParser from "cron-parser";
import type { JobMessage } from "./backend.js";

export type Duration = `${number}${"ms" | "s" | "m" | "h" | "d" | "w"}`;
type Exclusive<Choices, All = Choices> = Choices extends unknown
  ? Choices & Partial<Record<Exclude<Keys<All>, keyof Choices>, never>> : never;
type Keys<T> = T extends unknown ? keyof T : never;
type TimingChoices = { readonly after: Duration } | { readonly at: Date | string | number } |
  { readonly every: Duration } | { readonly daily: string; readonly timezone: string } |
  { readonly cron: string; readonly timezone: string };

/** Choose one timing rule. Calendar rules require an explicit IANA timezone. */
export type Timing = Exclusive<TimingChoices>;
export type Recurrence = Extract<Timing, { every: Duration } | { daily: string } | { cron: string }>;
export type Delay = Exclude<Timing, Recurrence>;

/** A recurring registration, not an execution result. Removal leaves already-started work alone. */
export interface ScheduleHandle {
  readonly id: string;
  update(timing: Recurrence): Promise<void>;
  remove(): Promise<void>;
}

/** Normalized, serializable calendar or elapsed-time rule used by strategies. */
export type ScheduleRule = { readonly every: number } | { readonly cron: string; readonly timezone: string };
export interface JobSchedules {
  /** Upsert one schedule per job name and encoded input; repeated registration preserves its pending occurrence. */
  upsert(message: JobMessage, rule: ScheduleRule): Promise<string>;
  /** Change future occurrences; reject if the schedule no longer exists. */
  update(id: string, rule: ScheduleRule): Promise<void>;
  remove(id: string): Promise<void>;
}

export type Delivery = { readonly at: number } | { readonly repeat: ScheduleRule };
const units = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 };

function milliseconds(value: unknown): number {
  const match = typeof value === "string" && /^(\d+(?:\.\d+)?)(ms|s|m|h|d|w)$/.exec(value);
  if (!match) throw new TypeError("Duration must include a unit: ms, s, m, h, d, or w");
  const duration = Number(match[1]) * units[match[2] as keyof typeof units];
  if (!Number.isSafeInteger(duration) || duration <= 0) throw new RangeError("Duration must be a positive whole number of milliseconds");
  return duration;
}

function timestamp(value: unknown): number {
  if (typeof value === "string" && !/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    throw new TypeError("at must use an ISO timestamp with an explicit timezone");
  }
  const at = value instanceof Date ? value.getTime() : typeof value === "string" ? Date.parse(value) : value;
  if (typeof at !== "number" || !Number.isSafeInteger(at) || !Number.isFinite(new Date(at).getTime())) {
    throw new RangeError("at must be a valid timestamp");
  }
  return at;
}

/** Snapshot timing before submission can yield; malformed timing never becomes an immediate job. */
export function delivery(timing: Timing, now = Date.now()): Delivery {
  if (!timing || typeof timing !== "object" || Array.isArray(timing)) throw new TypeError("Timing must be an object");
  const modes = ["after", "at", "every", "daily", "cron"].filter((key) => Object.hasOwn(timing, key));
  const mode = modes[0];
  const calendar = mode === "daily" || mode === "cron";
  const allowed = calendar ? [mode, "timezone"] : [mode];
  if (modes.length !== 1 || Reflect.ownKeys(timing).some((key) => typeof key !== "string" || !allowed.includes(key))) {
    throw new TypeError("Choose exactly one timing rule: after, at, every, daily, or cron");
  }
  if (mode === "after") return { at: timestamp(now + milliseconds(timing.after)) };
  if (mode === "at") return { at: timestamp(timing.at) };
  if (mode === "every") {
    const every = milliseconds(timing.every);
    timestamp(now + every);
    return { repeat: Object.freeze({ every }) };
  }
  if (typeof timing.timezone !== "string" || !timing.timezone) throw new TypeError("Calendar schedules require a timezone");
  new Intl.DateTimeFormat("en", { timeZone: timing.timezone });
  let cron = timing.cron;
  if (mode === "daily") {
    if (typeof timing.daily !== "string" || !/^([01]\d|2[0-3]):[0-5]\d$/.test(timing.daily)) {
      throw new TypeError("daily must use HH:mm in 24-hour time");
    }
    const [hour, minute] = timing.daily.split(":").map(Number);
    cron = `${minute} ${hour} * * *`;
  }
  if (typeof cron !== "string" || ![5, 6].includes(cron.trim().split(/\s+/).length)) {
    throw new TypeError("cron must have five fields, or six including seconds");
  }
  const rule = Object.freeze({ cron: cron.trim().replace(/\s+/g, " "), timezone: timing.timezone });
  nextRun(rule, now);
  return { repeat: rule };
}

export function nextRun(rule: ScheduleRule, after: number): number {
  return "every" in rule ? after + rule.every
    : cronParser.parseExpression(rule.cron, { currentDate: after, tz: rule.timezone }).next().getTime();
}

export function validateRule(rule: ScheduleRule): ScheduleRule {
  const resolved = delivery(("every" in rule ? { ...rule, every: `${rule.every}ms` } : rule) as Recurrence);
  if (!("repeat" in resolved)) throw new Error("A schedule requires recurring timing");
  return resolved.repeat;
}

export function scheduleId(message: JobMessage): string {
  return `schedule-${createHash("sha256").update(JSON.stringify([message.name, message.input])).digest("hex")}`;
}

/** Each occurrence keeps its identity across retries, independent of other runs and ordinary submissions. */
export function occurrence(message: JobMessage, id: string): JobMessage {
  const { key, idempotencyKey, ...policy } = message.policy;
  return Object.freeze({ ...message, id, policy: Object.freeze({ ...policy,
    ...(key !== undefined ? { key: JSON.stringify([key, id]) } : {}),
    ...(idempotencyKey !== undefined ? { idempotencyKey: JSON.stringify([idempotencyKey, id]) } : {}),
  }) });
}

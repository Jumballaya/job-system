/** A codec must round-trip supported values; reject values it cannot preserve. */
export interface JobCodec {
  encode(value: unknown): string;
  decode(encoded: string): unknown;
}

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

function snapshot(value: unknown, ancestors = new Set<object>()): Json {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value) && !Object.is(value, -0)) return value;
  if (typeof value !== "object" || value === null) throw new TypeError("Job data must contain only JSON values");
  if (ancestors.has(value)) throw new TypeError("Job data must not contain cycles");
  const array = Array.isArray(value);
  const prototype = Object.getPrototypeOf(value);
  if (array ? prototype !== Array.prototype : ![Object.prototype, null].includes(prototype)) {
    throw new TypeError("Job data must use plain objects; provide a custom codec for class instances");
  }
  if (array && Object.keys(value).some((key) => !/^(0|[1-9]\d*)$/.test(key) || Number(key) >= value.length)) {
    throw new TypeError("Job arrays must not contain extra properties");
  }
  if (Object.getOwnPropertySymbols(value).some((key) => Object.prototype.propertyIsEnumerable.call(value, key))) {
    throw new TypeError("Job data must not contain symbol properties");
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) return Array.from(value, (item) => snapshot(item, ancestors));
    const result: { [key: string]: Json } = Object.create(null);
    for (const [key, item] of Object.entries(value)) result[key] = snapshot(item, ancestors);
    return result;
  } finally {
    ancestors.delete(value);
  }
}

/** JSON data plus top-level undefined, with rejection instead of silent type conversion. */
export class JsonCodec implements JobCodec {
  public encode(value: unknown): string {
    return JSON.stringify(value === undefined ? { kind: "undefined" } : { kind: "json", value: snapshot(value) });
  }

  public decode(encoded: string): unknown {
    const envelope: unknown = JSON.parse(encoded);
    if (typeof envelope !== "object" || envelope === null || !("kind" in envelope)) {
      throw new TypeError("Invalid job data envelope");
    }
    if (envelope.kind === "undefined") return undefined;
    if (envelope.kind === "json" && "value" in envelope) {
      snapshot(envelope.value);
      return envelope.value;
    }
    throw new TypeError("Invalid job data envelope");
  }
}

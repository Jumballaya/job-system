import assert from "node:assert/strict";
import { test } from "node:test";
import { JsonCodec } from "../dist/index.js";

test("JSON values and a top-level undefined survive encoding without shared references", () => {
  const codec = new JsonCodec();
  for (const value of [undefined, null, false, 0, "", [1, "two", null], { nested: [true] }]) {
    const decoded = codec.decode(codec.encode(value));
    assert.deepEqual(decoded, value);
    if (value && typeof value === "object") assert.notEqual(decoded, value);
  }
});

test("unsupported values are rejected instead of silently changing their meaning", () => {
  const codec = new JsonCodec();
  const cycle = {};
  cycle.self = cycle;
  for (const value of [NaN, Infinity, 1n, Symbol("value"), () => 1, new Date(), { missing: undefined }, [undefined], cycle]) {
    assert.throws(() => codec.encode(value));
  }
});

test("malformed encoded data is rejected", () => {
  const codec = new JsonCodec();
  assert.throws(() => codec.decode("this is not encoded JSON"));
});

test("numbers and arrays that JSON would silently change are rejected", () => {
  const codec = new JsonCodec();
  class NamedArray extends Array {}
  const extraProperty = [1];
  extraProperty.name = "preserve me";
  for (const value of [-0, new NamedArray(1), extraProperty]) {
    assert.throws(() => codec.encode(value));
  }
});

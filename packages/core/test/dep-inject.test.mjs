import assert from "node:assert/strict";
import { test } from "node:test";
import { Container } from "../dist/index.js";

test("registration is lazy and repeated resolution shares a singleton", () => {
  class Service {}
  let calls = 0;
  const container = new Container().register(Service, { useFactory: () => { calls++; return new Service(); } });
  assert.equal(calls, 0);
  assert.equal(container.resolve(Service), container.resolve(Service));
  assert.equal(calls, 1);
});

test("ancestor singletons use ancestor dependencies even when a child resolves first", () => {
  class Config { constructor(name) { this.name = name; } }
  class Service { constructor(config) { this.config = config; } }
  const root = new Container()
    .register(Config, { useValue: new Config("root") })
    .register(Service, { useFactory: (resolver) => new Service(resolver.resolve(Config)) });
  const child = root.createScope().register(Config, { useValue: new Config("child") });
  assert.equal(child.resolve(Config).name, "child");
  assert.equal(child.resolve(Service).config.name, "root");
  assert.equal(child.resolve(Service), root.resolve(Service));
});

test("creating a scope closes parent registration while allowing local overrides", () => {
  class Config {}
  class Late {}
  const root = new Container().register(Config);
  const child = root.createScope();
  assert.throws(() => root.register(Late), /registration is closed/);
  const local = new Config();
  child.register(Config, { useValue: local });
  assert.equal(child.resolve(Config), local);
  assert.throws(() => child.register(Late), /registration is closed/);
});

test("cycles and singleton captures report the complete dependency chain", () => {
  class First {}
  class Second {}
  const cyclic = new Container()
    .register(First, { useFactory: (r) => r.resolve(Second) })
    .register(Second, { useFactory: (r) => r.resolve(First) });
  assert.throws(() => cyclic.resolve(First), /Circular dependency: First -> Second -> First/);
  const captured = new Container()
    .register(First, { useFactory: (r) => r.resolve(Second) })
    .register(Second, { lifetime: "scoped", useFactory: () => new Second() });
  captured.resolve(Second);
  assert.throws(() => captured.resolve(First), /Singleton cannot depend on scoped dependency: First -> Second/);
});

test("failed construction retries but a factory resolver cannot escape its call", () => {
  class Flaky {}
  let attempts = 0;
  let escaped;
  const container = new Container().register(Flaky, {
    useFactory: (resolver) => {
      escaped = resolver;
      if (++attempts === 1) throw new Error("not ready");
      return new Flaky();
    },
  });
  assert.throws(() => container.resolve(Flaky), /not ready/);
  assert.throws(() => escaped.resolve(Flaky), /before returning or awaiting/);
  assert.equal(container.resolve(Flaky), container.resolve(Flaky));
  assert.equal(attempts, 2);
});

test("runtime callers cannot supply contradictory provider strategies", () => {
  class Service {}
  for (const provider of [
    { useValue: undefined, useFactory: () => new Service() },
    { useValue: new Service(), lifetime: "scoped" },
    { useFactory: () => new Service(), lifetime: "unknown" },
    {},
  ]) {
    assert.throws(() => new Container().register(Service, provider));
  }
});

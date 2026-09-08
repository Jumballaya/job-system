// Constructor signatures vary; instance types remain attached to their class keys.
export type Constructor<T = unknown> = new (...args: any[]) => T;
export type Lifetime = "singleton" | "scoped" | "transient";

export interface Resolver {
  resolve<C extends Constructor>(key: C): InstanceType<C>;
}

/** Factories default to singleton; their resolver is valid only during construction. */
export type Provider<T> =
  | { useValue: T; useFactory?: never; lifetime?: never }
  | { useFactory: (resolver: Resolver) => T; lifetime?: Lifetime; useValue?: never };

type Registration<C extends Constructor> = [] extends ConstructorParameters<C>
  ? [provider?: Provider<InstanceType<C>>]
  : [provider: Provider<InstanceType<C>>];

type Binding = {
  key: Constructor;
  owner: Container;
  lifetime: Lifetime;
  create: (resolver: Resolver) => unknown;
};

function describe(path: readonly Binding[], key: Constructor): string {
  return [...path.map((binding) => binding.key.name), key.name].join(" -> ");
}

/** Configure before resolving. Creating a child freezes its parent's registrations. */
export class Container implements Resolver {
  private readonly bindings = new Map<Constructor, Binding>();
  private readonly instances = new Map<Binding, unknown>();
  private locked = false;

  constructor(private readonly parent?: Container) {
    if (parent) parent.locked = true;
  }

  public register<C extends Constructor>(key: C, ...args: Registration<NoInfer<C>>): this {
    if (this.locked) throw new Error("Container registration is closed; configure a new container or child scope.");
    if (this.bindings.has(key)) throw new Error(`Dependency already registered: ${key.name}`);

    const [provider] = args;
    let create: Binding["create"];
    let lifetime: Lifetime = "singleton";
    if (provider === undefined) {
      create = () => new key();
    } else {
      // Presence matters: an undefined value is valid, but two strategies are not.
      const value = "useValue" in provider;
      const factory = "useFactory" in provider;
      if (value === factory) throw new Error("Provide exactly one of useValue or useFactory");
      if (value) {
        if ("lifetime" in provider) throw new Error("Value providers do not have a lifetime setting");
        const instance = provider.useValue;
        create = () => instance;
      } else {
        const make = provider.useFactory;
        if (typeof make !== "function") throw new Error("useFactory must be a function");
        create = make;
        lifetime = provider.lifetime ?? "singleton";
        if (!["singleton", "scoped", "transient"].includes(lifetime)) {
          throw new Error(`Unknown dependency lifetime: ${lifetime}`);
        }
      }
    }
    this.bindings.set(key, { key, owner: this, lifetime, create });
    return this;
  }

  public createScope(): Container {
    return new Container(this);
  }

  public resolve<C extends Constructor>(key: C): InstanceType<C> {
    return this.resolveDependency(key, []);
  }

  public find<C extends Constructor>(key: C): InstanceType<C> {
    return this.resolve(key);
  }

  private lookup(key: Constructor): Binding | undefined {
    return this.bindings.get(key) ?? this.parent?.lookup(key);
  }

  private resolveDependency<C extends Constructor>(key: C, path: readonly Binding[]): InstanceType<C> {
    this.locked = true;
    const binding = this.lookup(key);
    if (!binding) throw new Error(`Missing dependency: ${describe(path, key)}`);
    if (path.includes(binding)) throw new Error(`Circular dependency: ${describe(path, key)}`);
    if (binding.lifetime === "scoped" && path.some((entry) => entry.lifetime === "singleton")) {
      throw new Error(`Singleton cannot depend on scoped dependency: ${describe(path, key)}`);
    }

    const scope = binding.lifetime === "singleton" ? binding.owner : this;
    // Registration fixes the instance type; the heterogeneous cache erases it internally.
    if (binding.lifetime !== "transient" && scope.instances.has(binding)) {
      return scope.instances.get(binding) as InstanceType<C>;
    }
    const nextPath = [...path, binding];
    let active = true;
    const resolver: Resolver = {
      resolve: <D extends Constructor>(dependency: D): InstanceType<D> => {
        if (!active) throw new Error("Resolve factory dependencies before returning or awaiting");
        return scope.resolveDependency(dependency, nextPath);
      },
    };
    try {
      const instance = binding.create(resolver);
      if (binding.lifetime !== "transient") scope.instances.set(binding, instance);
      return instance as InstanceType<C>;
    } finally {
      active = false;
    }
  }
}

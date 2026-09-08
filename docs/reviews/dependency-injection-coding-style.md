# Dependency injection: coding-style review

Historical review of the implementation before the class-based DI and job-system
rewrite. The current API and behavior are documented in the root README.

Reviewed the working tree on September 6, 2026 using the complete `coding-style`
skill. Scope: DI implementation, public exports and emitted declarations,
consumer application, examples, tests, build configuration, and README.
Implementation files were not changed during this review.

Three subagents performed nine focused passes in three waves: module depth,
type invariants, runtime invariants, naming/comments, testing, error/example
design, and three adversarial checks of the synthesis and proposed remedies.
The parent reviewer independently reproduced key findings and consolidated them.
The session's agent limit required reusing the three reviewers for later passes.

**Conclusion.** The container hides useful work behind a small interface, but its
type boundary is unsound and its configuration model hides substantial temporal
coupling. Those are more consequential than naming or formatting. The module's
size is not the problem. Strengthen the boundary and simplify the ownership and
registration rules before expanding the feature set.

The existing 21 tests pass. Additional strict compiler and runtime probes expose
failures those tests do not cover. Passing compile-time checks in the earlier
implementation report did not establish the stronger claim that the exported
dependency identity contract was sound.

**Evidence and priority.** High means a pervasive failure of the published package
boundary. Medium means narrower unsound inputs, concrete correctness problems or
consequential interactions needing attention before broader adoption. Design concerns
are identified separately; they are not automatically defects.

| ID | Priority | Finding | Location |
| --- | --- | --- | --- |
| F1 | High | Emitted declarations erase the token's generic distinction. | `packages/core/src/dep-inject.ts:3`; emitted `dist/dep-inject.d.ts:4` |
| F2 | Medium | Source tokens can be widened, allowing incompatible registration. | `dep-inject.ts:2`, `dep-inject.ts:57` |
| F3 | Medium | Provider unions accept contradictory strategies, including a wrong runtime result type. | `dep-inject.ts:26`, `dep-inject.ts:97` |
| F4 | Medium | Constructor arity heuristics silently allow missing required dependencies. | `dep-inject.ts:108` |
| F5 | Medium | An empty collection lookup disables later class auto-resolution. | `dep-inject.ts:126`, `dep-inject.ts:135`, `dep-inject.ts:155` |
| F6 | Medium, app example | Joining an organization overwrites a user with an organization. | `packages/app/src/index.ts:91` |
| F7 | Medium, app example | Constructing another service deletes existing table contents. | `packages/app/src/index.ts:8`, `packages/app/src/index.ts:49` |
| F8 | Medium, app example | Inserting one object twice aliases rows and changes the first row's ID. | `packages/app/src/index.ts:17` |

**F1: declaration emission breaks typed identity.** The source has a private
phantom field of type `T`, but the emitted declaration is only
`private readonly valueType;`. The type parameter is no longer present in that
member. This assertion-free consumer program compiles in strict mode against the
built package and produces a number where its type promises a string:

```ts
import { Container, token, type InjectionToken } from "core";

const text = token<string>("text");
const count: InjectionToken<number> = text;
const container = new Container().register(count, { useValue: 123 });
const result: string = container.resolve(text);
console.log(typeof result); // number
```

The equivalent unrelated assignment is rejected against the source. Checking
only source types therefore misses this boundary failure. A declaration-preserved
invariant brand is the appropriate correction. An isolated prototype using
`declare protected valueType: (value: T) => T` retained the type in the generated
declaration and rejected unrelated assignment and widening. Verify the eventual
implementation through the package entry point, not only its source file.

Use a function property, not a method, for the invariant brand. Invariance also
means the private `label(Token<unknown>)` helper needs an intentionally erased
identity input; do not weaken the public brand to satisfy that internal helper.

**F2: covariance permits unsafe registration even before emission.** A token
participates in both producing and accepting `T`, so covariance is insufficient:

```ts
const text = token<string>("text");
const wide: InjectionToken<unknown> = text;
const container = new Container().register(wide, { useValue: 123 });
const result: string = container.resolve(text); // actually a number
```

Explicit `register<unknown>(text, ...)` also bypasses the intended restriction.
`NoInfer` prevents inference from the provider; it does not prevent widening.
Fix F1 and F2 together with invariance. Raw constructor tokens remain structurally
typed and need a separately stated compatibility limit; do not advertise them as
having the same nominal guarantees as invariant interface tokens.

**F3: the provider shape does not encode one construction strategy.** This accepted
`Provider<string>` produces `undefined`:

```ts
const text = token<string>("text");
const provider: Provider<string> = {
  useValue: undefined,
  useFactory: () => "good",
};
const result: string = new Container().register(text, provider).resolve(text);
```

Normalization tests for the presence of `useValue`, silently selecting it over
the factory. Other mixed inputs silently discard class/factory choices and
lifetime settings. This is exactly the skill's contradictory-state warning.
Prefer an explicit discriminant if changing the API. If preserving the current
syntax, enforce exclusive keys and reject mixed strategies at registration.
Optional `never` properties alone are insufficient without considering optional
`undefined` semantics. Do not silently choose a winner.

Library compiler options do not impose exact optional-property semantics on
consumers. If adding a discriminant, normalization must dispatch on it; if
preserving the current shapes, reject multiple present strategy keys, including
keys whose values are `undefined`. A lone `{ useValue: undefined }` remains valid
for an undefined-valued token. Validation and dispatch must agree about presence.

**F4: constructor metadata inference promises more than it knows.** A subclass
without its own constructor has runtime `length === 0` even when its base requires
an argument. A required argument after a default argument can also escape the
heuristic. Both construct with missing values. Explicitly wrong or empty `params`
arrays likewise compile, though the README already acknowledges unchecked static
metadata. Prefer ordinary, compiler-checked constructor calls in factories.
Preserve legacy metadata deliberately; do not add more reflection heuristics to
recover erased TypeScript information.

```ts
class Base {
  constructor(readonly name: string) {}
}
class Derived extends Base {}
const value = new Container().resolve(Derived);
console.log(Derived.length, value.name); // 0, undefined
```

There is also a recovery problem: failed implicit registration leaves a binding
registered and sealed. The error recommends an explicit factory, but attempting
to register that factory in the same container fails. Validate detectable metadata
errors before committing inferred registration, or explain that configuration
must be rebuilt. Runtime validation cannot prove arbitrary constructor compatibility.

**F5: two read-looking operations have incompatible side effects.** Reproduced:

```ts
class Empty {}
const first = new Container();
first.resolveAll(Empty); // []
first.resolve(Empty);    // Cannot change dependency after resolution

const second = new Container();
const instance = second.resolve(Empty);
second.resolveAll(Empty)[0] === instance; // true
```

Empty collections sealing and class auto-registration are individually documented.
The problem is their combined contract: discovery changes the validity of later
resolution. Choose one policy for class identities across both operations, or
remove implicit registration in a future API. Adding another branch to the
sealing machinery without defining that policy would increase complexity.

**F6–F8: the app example leaks storage decisions into services.** Isolated runtime
probes against the compiled application classes confirmed that after `join`,
`users.get(user.id)` returns the organization. Constructing a second `UserService`
then makes that read return `null`. Inserting the same object twice aliases both rows and rewrites its
ID. These are example-layer defects, not failures of DI resolution.

Table strings, collection ownership, object mutation, initialization, and the
claimed return type are distributed across three classes. `read<T>` lets a caller
claim any row shape; `any` masks the wrong-table write. Returning a live row also
lets `org.users.push(...)` mutate stored state before `update` is called, concealing
the write bug. If retaining this example, establish typed table ownership,
idempotent initialization, and explicit mutation semantics. Alternatively reduce
it to the smallest useful class-injection example; it need not become a database
framework to demonstrate DI.

**The largest design concern is hidden configuration state.** To understand
whether registration is still allowed, a maintainer must combine `addBinding`,
`lookup`, `seal`, `resolveOne`, `resolveMany`, and `instantiate`. These methods share
one policy but traverse ancestry and mark state differently. `resolved` includes
failed attempts and missing collections, so it is more accurately `sealedKeys`.
This is conjoined-method complexity and an unknown-unknowns risk, not merely a
long function. A child lookup can change its ancestors' future configuration.

Factories also receive an ordinary `Resolver` whose lifetime ends when the
factory returns, whereas `Container` implements that same interface indefinitely.
The provided resolver is a useful restricted capability, but its time limit
belongs in the interface contract. A natural returned handler that retains it
fails later. Capturing the outer container instead bypasses path-based checks;
the README prohibits this, so it is a documented limitation rather than a newly
discovered supported-use bug. Dependency values should be captured during
construction, as the jobs example does.

One token also represents both a singular dependency and an ordered collection.
Adding a second provider can invalidate a consumer's singular lookup. This is a
reasonable centrally configured policy, but independent plugin composition may
justify explicit collection identities. Do not introduce multiple competing ways
to express cardinality until that use case is established.

**What should stay.** Keep one cohesive DI module and a stateful `Container`.
Normalizing providers to `Binding` lets the resolver forget how construction was
specified. Binding-identity caches correctly distinguish collection providers.
Owner-based singleton caches prevent child context from contaminating shared
services, while scoped caches isolate jobs. `Map.has` correctly caches falsy
values. Synchronous factory failure does not cache a failed result; successful
dependencies survive retries. `finally` expires construction resolvers on both
success and failure. Returned collections do not expose the registration array.

Keep identity-based tokens, explicit factories, and the small `Resolver`
capability. Keep `find` as a compatibility alias while compatibility matters; its
one-line forwarding is not an architectural defect. The `token` helper earns its
cost in ergonomics. Localized casts over a heterogeneous store are reasonable
once the public typing invariant is actually enforced. Deleting every cast or
forwarder would apply the skill mechanically instead of reducing complexity.

Routing selection, tool schemas, job scheduling, retries, cancellation policy,
and persistence should remain application concerns. The container's role is
composition and instance ownership. Explicit resource cleanup and synchronous
resolution with optional Promise-valued tokens are documented scope choices.
Rejected Promise caching is not a synchronous-factory retry bug.

**Every coding-style principle, applied.**

| Skill aspect | Assessment and concrete consequence |
| --- | --- |
| Match surrounding code | ESM, explicit types, Node tests and class-oriented examples are established. No formatter/linter convention warrants a style migration. |
| Dependencies as complexity | Constructor metadata repeats wiring; storage strings and row types cross service boundaries. Prefer factories and one owner for storage decisions. |
| Obscurity as complexity | Implicit root registration, sealing and expiring resolvers require knowledge outside the apparent operation. Make their contract coherent and visible. |
| Change amplification | Changing constructor parameters can require matching metadata edits; changing table ownership affects unrelated methods. Remove duplicate knowledge. |
| Cognitive load | Three lifetimes are useful; combining them with mutable ancestry and implicit cardinality raises caller cost. Simplify the interactions before adding options. |
| Unknown unknowns | Published type erasure and read-order effects are the strongest examples. Test consumer declarations and feature interactions. |
| Incremental complexity | Small conveniences accumulated into a hidden registration state machine. Judge new convenience against its lasting rule cost. |
| Design twice | Compare immutable configuration with explicit dependency recipes below, including migration and caller costs. |
| Deep modules | DI hides construction, caching and scope ownership well. Its weak point is behavioral interface cost, not file length. |
| Information hiding | Binding normalization is strong; class metadata and generic database reads leak decisions. Keep one resolution engine. |
| Pull complexity downward | Enforce identity/provider validity centrally. Do not require every service author to compensate for invalid construction or hidden resolver state. |
| Illegal states | Mixed providers and weak token variance violate the rule. A representation-level correction beats more downstream assertions. |
| Define errors out of existence | Remove ordering contradictions by design. Keep errors for cycles, missing required dependencies and ambiguous singular resolution. Empty collections already represent normal absence. |
| Somewhat general-purpose | Values, factories, scopes and collections suit tools/jobs/routes. Do not embed a scheduler, router or tool protocol in DI. |
| Different layers | Application dispatch and DI resolution are appropriately different. The stable/temporary Resolver distinction deserves clearer treatment. |
| Related knowledge together | Keep normalization, ancestry and cache invariants cohesive; avoid a file per helper or execution phase. |
| Flat happy path | Most guards are good. Provider normalization's nesting would improve through an exclusive shape; superficial helper extraction would not repair validity. |
| Obvious code | `sealedKeys`, precise resolved-value terminology and context-aware errors explain more than extra prose around misleading names. |
| Minimal comments | Existing source comments meet the two-line limit. Keep rationale for type erasure boundaries and singleton ownership. |
| Interface comments first | Explain construction-time Resolver validity and actual resolveAll return values at the API. README-only constraints are harder to discover. |
| Implementation versus interface | The result cast's heterogeneous-store explanation belongs internally. Provider lifetime and collection semantics belong at the interface. |
| Magic-number comments | No unexplained DI policy constants require comments. Do not manufacture annotations for ordinary loop/index values. |
| Workaround comments | No external workaround is present. The declaration-brand correction should describe its reason briefly if the type representation is nonobvious. |
| Surprising intentional behavior | Root ownership, shadowing and resolver expiration need precise contracts. If the explanation grows, revisit the design. |
| Hard-to-follow logic | Sealing is the main case. Fix the representation/policy rather than adding a long narrative to each traversal. |
| Why comments and genuine debt | Existing rationale is mostly useful; no need to invent TODOs. Record concrete review debt here until implemented. |
| No restatement or stale comments | `resolveAll` says "providers" but returns values. Correct that term. Avoid documenting obvious forwarding mechanics except compatibility intent. |
| Precise names | `resolved` understates sealing; app `data`, `t`, `new`, and placeholder names obscure storage purpose. Keep `Binding`, `Provider`, `Resolver`, and `Lifetime`. |
| One concept per file | DI is cohesive. The core entry point still includes unrelated starter `greet`; retire it when replacing its smoke test, not as a prerequisite to correctness fixes. |
| Classes versus functions | Container needs state; token/label and dispatch glue fit functions. No getters or wrapper classes are needed. |
| Behavioral testing | Existing tests cover real identity/error behavior. Missing declaration assignability, mixed strategies, interaction order and app state tests are consequential gaps. |
| Test names | Most describe outcomes. The concurrency example proves output separation, but needs controlled overlap to substantiate stronger async-isolation claims. |
| Let tests serve design | Do not preserve surprising sealing behavior solely because a test asserts it. Change the contract and its regression together. |
| Consistency | Adopt one construction/cardinality policy. Syntactic consistency is secondary; avoid broad formatting changes. |
| Strategic investment | Fix the type boundary and composition model before adding lifetime flags or reflection. A larger framework is not automatically a strategic improvement. |

**The red flags checklist.** Shallow wrappers are acceptable where they preserve
compatibility or ergonomics. Information leakage is real in metadata and the
database example. The main temporal dependency is implicit sealing, not the
physical order of methods. Overexposure appears in undocumented-at-interface
resolver lifetime rules. Repeated ancestry walks and error formatting deserve
one coherent policy, not necessarily more methods. Automatic class registration
is the special case mixed into general resolution. Sealing methods are conjoined.
Providers admit contradictory states. Ordinary absence is already handled without
throwing for collections and absent-record update/delete operations. Missing-table
setup states should disappear behind idempotent table acquisition.
Comment vocabulary and misleading state names need modest corrections. These are
targeted observations, not a mandate to check every box with a code change.

No harmful temporal file decomposition was found. The `path` parameter carries
construction provenance used across resolution, cycle checks, and lifetime checks;
it is not merely an unused value forwarded through layers. Revisit its shape with
the resolution policy, rather than deleting it to satisfy the pass-through rule.

The current tree cannot establish whether interface comments were historically
written before implementations. That process rule is assessed through whether the
resulting interface can be described simply; no authorship-order claim is made.

**Error-policy audit.**

| Situation | Assessment |
| --- | --- |
| Duplicate registration | Appropriate configuration error; do not silently replace instances. The advice to use `add` needs qualification when already sealed. |
| Registration after sealing | Necessary under current semantics; an immutable configuration boundary could remove this runtime state instead of weakening the guard. |
| Interface token without a provider | Appropriate invalid-use error, already rejected by the typed overload. |
| Missing constructor metadata | Appropriate category, unreliable arity inference; failed auto-registration also prevents the suggested in-place remedy. |
| Missing required dependency | Throwing with a dependency path preserves `resolve<T>(): T`. Returning an optional value would move checks into every consumer. |
| Missing collection | Returning `[]` correctly handles normal absence. The sealing side effect, not the empty result, creates F5. |
| Multiple providers during singular resolution | Appropriate ambiguity error. Include the requesting dependency path, as missing and cyclic cases already do. |
| Cyclic dependencies | Appropriate programmer error; adding lazy cycle support would be a separate capability. |
| Singleton retaining a scoped dependency | Appropriate lifetime error. Checks correctly precede cache return and inspect transitive dependencies. |
| Expired factory resolver | Appropriate enforcement of documented construction limits; the limitation belongs beside the factory interface. |
| Synchronous factory failure | Preserve the original error; do not cache failure; expire its resolver in `finally`. Retain successful dependencies deliberately. |
| Rejected Promise-valued dependency | Documented value caching, not implicit async retry. The caller observes rejection. |
| Missing table during creation | Eliminate through owned/idempotent table acquisition rather than nullable setup results everywhere. |
| Undefined insertion ID | Impossible after successful array push; remove the branch instead of testing it. |
| Invalid/frozen input record during insertion | `any` and caller-object mutation create these states. Typed, owned records eliminate much of the problem. |
| Missing record during read | Normal absence; `null` is reasonable. A missing table is a different setup issue. |
| Missing update/delete target | Both no-op consistently; acceptable if intentional. Do not add exceptions without caller needs. |
| Missing user or organization during join | An unspecified application policy, not necessarily a DI-demo bug. Return a meaningful outcome if callers need one. |
| Main/example setup failure | Current nullable setup can silently skip the job example. Eliminate impossible setup states. |
| Rejection escaping main | Currently unhandled. Aggregate operational reporting at the executable boundary if expected failures are introduced. |
| Job/handler failure in `Promise.all` | Valid application policy: rejection is observed while other work may continue. Change only if application requirements demand collected outcomes or cancellation. |

Other returns are normal construction results, cache hits, forwarding, or ancestry
termination; they create no additional error policy. The skill does not advocate
catching or suppressing every exception.

**Design it twice.**

| Choice | A: immutable configured resolver | B: typed dependency recipes |
| --- | --- | --- |
| Core idea | Configure registrations, then build a resolver; supply local context when creating a scope. | Declare each recipe's typed dependency tuple; factories receive dependency values instead of a resolver. |
| What callers can forget | Per-key and per-ancestor sealing after ordinary lookup. | Expiring factory resolvers and dynamic dependency lookup during construction. |
| What disappears internally | Incremental registration freeze state, provided configuration is immutable throughout scope ancestry. | Factory resolver escape handling; some graph edges become available before construction. |
| What callers must learn | An explicit configuration/build boundary and scope-input rules. | Recipe identities, tuple ordering and collection/replacement semantics. |
| What remains | Scope ownership, lifetime safety, dynamic callback discipline and deliberate cardinality. | Scope ownership, lifetime safety, collection semantics and tuple type machinery. |
| Fit | Closest to current API; useful when registrations are assembled at startup. | Better when static dependency graph analysis becomes a demonstrated requirement. |
| Main cost | Breaking semantics for post-resolution registration and auto-registration. | Larger API/type-system change and less natural conditional dependencies. |

Illustrative A call site:

```ts
const definitions = new Registry()
  .register(database, { useValue: db })
  .add(handler, {
    lifetime: "scoped",
    useFactory: r => makeHandler(r.resolve(database), r.resolve(job)),
  });
const services = definitions.build();
const scope = services.createScope(config => config.register(job, { useValue: deliveredJob }));
await Promise.all(scope.resolveAll(handler).map(h => h.run()));
```

Illustrative B call site:

```ts
const job = input<Job>("job");
const database = service("database", [], () => openDatabase());
const handler = service(
  "handler", [database, job], (db, current) => makeHandler(db, current),
  { lifetime: "scoped" },
);
const scope = createScope([provide(job, deliveredJob)]);
await scope.resolve(handler).run();
```

These are alternative designs, not APIs currently implemented. Favor a targeted
repair now. If a deliberate API revision is warranted, A addresses the largest
hidden state machine with less change to service authors. Choose B only for a
demonstrated need for explicit dependency graph tooling. Neither requires
splitting the implementation into many files.

Accept A only if it actually removes the existing per-key/per-ancestor mutation
state machine and yields a simpler complete contract. Builder reuse, snapshots,
and escaping configuration callbacks also need defined semantics. Layering a new
`build()` API over the same hidden state would add interface cost without benefit.

**Recommended sequence.**

1. Fix emitted identity typing and invariance; add positive consumer checks and
   compile fixtures rejecting unrelated assignment, widening and explicit generic
   widening. Keep this first patch limited to the type boundary and its tests.
2. Make provider strategies exclusive, with a runtime boundary check if retaining
   compatibility with current object shapes; exercise undefined-valued providers.
3. Define one class-construction and collection-resolution policy. Test both lookup
   orders, inherited constructors and failed-construction recovery. Prefer factories.
4. Repair or shrink the database example; test row type, initialization and aliasing
   through its public operations. Keep the job example centered on DI.
5. Decide whether mutable registration is worth its hidden state. If changing it,
   migrate through an explicit configuration boundary and update affected contracts.
6. Tighten interface comments, names and diagnostics after the final semantics are
   settled. Preserve the useful scope/cache tests and add a controlled-overlap job
   scenario. Keep one package-consumption test even if most DI tests move to core.

Do not add disposal orchestration, async graph resolution, decorators, routing
policy, or caching optimizations merely to make this review produce more changes.
Path copying/scanning is quadratic on a long chain, but no workload measurement
establishes a performance problem. Dynamic missing-token queries can retain token
identities through ancestor sealing, but fixed startup tokens do not by themselves
establish a memory leak.

**Validation performed.** `pnpm test` rebuilt both packages and passed 21 tests.
Strict compiler probes against emitted declarations reproduced F1/F3; source
comparison and independent review established F2. Runtime probes reproduced the
collection/class ordering conflict, inherited/default-parameter construction,
example state corruption and repeated-object aliasing. A separate
brand prototype verified that its generic member survives declaration emission.
An independent emitted-consumer check verified the prototype's valid inference
and rejection of unrelated assignment, widening and invalid registration. A
provider-remedy probe confirmed that optional `never` alone does not fix F3 under
the current compiler configuration.
No production source or existing tests were edited for this review.

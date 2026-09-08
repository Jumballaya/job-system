/** One instance per job execution: registered as scoped. */
export class RequestScope {
  public readonly id = crypto.randomUUID().slice(0, 8);
}

/** Depends on the scope; shares the same RequestScope instance inside one execution. */
export class Auditor {
  constructor(private readonly scope: RequestScope) {}

  public stamp(action: string): string {
    return `${this.scope.id}:${action}`;
  }
}

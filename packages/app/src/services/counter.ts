/** Shared across executions: registered as a singleton. */
export class Counter {
  private value = 0;

  public add(amount: number): number {
    this.value += amount;
    return this.value;
  }
}

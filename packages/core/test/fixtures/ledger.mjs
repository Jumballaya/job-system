import { DatabaseSync } from "node:sqlite";

// A real effect store: the operation receipt and balance change commit in the same transaction.
export class Ledger {
  constructor(filename) {
    this.db = new DatabaseSync(filename);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA busy_timeout = 5000;
      CREATE TABLE IF NOT EXISTS accounts (tenant TEXT PRIMARY KEY, balance INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS receipts (
        tenant TEXT NOT NULL, operation TEXT NOT NULL, amount INTEGER NOT NULL, balance INTEGER NOT NULL,
        PRIMARY KEY (tenant, operation)
      );
      CREATE TABLE IF NOT EXISTS deliveries (tenant TEXT NOT NULL, operation TEXT NOT NULL);
    `);
  }

  credit({ tenant, operation, amount }) {
    this.db.prepare("INSERT INTO deliveries VALUES (?, ?)").run(tenant, operation);
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const receipt = this.db.prepare("SELECT amount, balance FROM receipts WHERE tenant = ? AND operation = ?").get(tenant, operation);
      if (receipt && receipt.amount !== amount) throw new Error("Operation payload conflict");
      const balance = receipt?.balance ?? this.db.prepare(`
        INSERT INTO accounts VALUES (?, ?) ON CONFLICT(tenant) DO UPDATE SET balance = balance + excluded.balance
        RETURNING balance
      `).get(tenant, amount).balance;
      if (!receipt) this.db.prepare("INSERT INTO receipts VALUES (?, ?, ?, ?)").run(tenant, operation, amount, balance);
      this.db.exec("COMMIT");
      return { balance };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  inspect() {
    return {
      deliveries: this.db.prepare("SELECT count(*) AS count FROM deliveries").get().count,
      receipts: this.db.prepare("SELECT count(*) AS count FROM receipts").get().count,
      balance: this.db.prepare("SELECT sum(balance) AS balance FROM accounts").get().balance,
    };
  }

  close() { this.db.close(); }
}

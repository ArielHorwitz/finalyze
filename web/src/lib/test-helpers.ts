// Fixtures for unit tests.
import type { Transaction } from "./api";

export function makeTransaction(overrides: Partial<Transaction> = {}): Transaction {
  return {
    id: "t1",
    account: "alice",
    group: "spending",
    group_role: "spending",
    source: "card",
    date: "2025-06-01",
    amount: -100,
    raw_description: "SuperMart",
    status: "untagged",
    annotation: null,
    rule: null,
    guess: null,
    ...overrides,
  };
}

export const GUESS = {
  tag: "food",
  subtag: "groceries",
  basis: "same_raw_description" as const,
  from_transaction_id: "t0",
};

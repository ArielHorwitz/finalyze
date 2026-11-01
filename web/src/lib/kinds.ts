// Tag kinds and account group roles: what they mean, for labels and selectors.
import type { Schemas } from "./api";

export type TagKind = Schemas["TagOut"]["kind"];
export type GroupRole = Schemas["GroupOut"]["role"];

export const TAG_KINDS: { id: TagKind; label: string; description: string }[] = [
  { id: "regular", label: "regular", description: "Incomes and expenses" },
  {
    id: "transfer",
    label: "transfer",
    description: "Between own accounts, both sides recorded: never incomes or expenses, only in balances",
  },
  { id: "saving", label: "saving", description: "Money set aside: saved, not spent" },
  {
    id: "one-off",
    label: "one-off",
    description: "Real, but left out of averages and trends (unless one-offs are included)",
  },
  {
    id: "reconciliation",
    label: "reconciliation",
    description: "Re-anchors a balance, with no other side (e.g. an opening balance): only in balances",
  },
];

export const GROUP_ROLES: { id: GroupRole; label: string; description: string }[] = [
  {
    id: "spending",
    label: "spending",
    description: "Day-to-day money: in cash flows, breakdowns and the available balance",
  },
  { id: "holding", label: "holding", description: "Savings, investments or debt: only in the net worth" },
];

/** Kinds of the incomes and expenses of cash flows and breakdowns. */
export const FLOW_KINDS: TagKind[] = ["regular", "one-off"];

/** Kinds of averages and trends. */
export function averagedKinds(includeOneOffs: boolean): TagKind[] {
  return includeOneOffs ? FLOW_KINDS : ["regular"];
}

export function kindDescription(kind: TagKind): string {
  return TAG_KINDS.find((item) => item.id === kind)?.description ?? "";
}

/** The next item of a list, wrapping around (e.g. to cycle kinds by key). */
export function nextOf<T>(items: T[], current: T): T {
  return items[(items.indexOf(current) + 1) % items.length];
}

// Batch edits: the same fields set on many rows, each field optional. What
// each row gets is worked out on its draft, the same way the API's bulk
// annotation does on the stored annotation (so it also previews batch apply).
import type { Transaction } from "./api";
import { acceptGuess, draftLabels, type Draft, type PartDraft } from "./drafts";
import type { TagKind } from "./kinds";
import { oneLine } from "./descriptions";

export const INHERIT = "inherit";

/** The fields to set. Null keeps each row's. */
export interface BatchFields {
  /** Replaces the parts (and splits) with one full-amount part. */
  label: { tag: string; subtag: string } | null;
  /** Set on every part. "inherit" clears it: the tags' kinds apply. */
  kind: TagKind | typeof INHERIT | null;
  /** Empty clears it. */
  description: string | null;
  needsReview: boolean | null;
}

export function hasFields(fields: BatchFields): boolean {
  return fields.label !== null || fields.kind !== null || fields.description !== null || fields.needsReview !== null;
}

export type BatchOutcome =
  | { draft: Draft; changes: string[] }
  /** The row is left as it is, for this reason. */
  | { skipped: string };

/**
 * A row's draft with the fields set, and what changes, or why it's skipped:
 * a rule tags it, or a field needs tags it doesn't have (a kind without a
 * tag, or the review flag without tags or a description). As with `O`, a kind
 * accepts the row's guess.
 */
export function batchDraft(transaction: Transaction, draft: Draft, fields: BatchFields): BatchOutcome {
  if (transaction.status === "rule") return { skipped: "tagged by a rule, which a batch can't change" };
  let next: Draft = { ...draft, parts: draft.parts.map((part) => ({ ...part })) };
  if (fields.label) {
    // The new part keeps a kind set on all the old parts (unless one is given)
    const kinds = new Set(tagged(draft) ? draft.parts.map((part) => part.kind) : []);
    const keptKind = kinds.size === 1 ? draft.parts[0].kind : null;
    next = { ...next, fromGuess: false, parts: [{ ...fields.label, amount: "", description: "", kind: keptKind }] };
  } else if (fields.kind !== null) {
    if (next.fromGuess) next = acceptGuess(next);
    if (!tagged(next)) return { skipped: "untagged: a kind needs a tag (include one)" };
  }
  if (fields.kind !== null) {
    const kind = fields.kind === INHERIT ? null : fields.kind;
    next = { ...next, parts: next.parts.map((part) => ({ ...part, kind })) };
  }
  if (fields.description !== null) next = { ...next, description: fields.description };
  if (fields.needsReview !== null) {
    if (!tagged(next) && !next.description.trim()) return { skipped: "nothing to flag: no tags or description" };
    next = { ...next, needsReview: fields.needsReview };
  }
  return { draft: next, changes: changesOf(draft, next) };
}

function tagged(draft: Draft): boolean {
  return draftLabels(draft).length > 0;
}

/** E.g. "food / groceries + home / supplies", or "untagged". */
export function labelsText(draft: Draft): string {
  const labels = draftLabels(draft);
  if (labels.length === 0) return "untagged";
  return labels.map(([tag, subtag]) => `${tag} / ${subtag || "(none)"}`).join(" + ");
}

function kindsText(parts: PartDraft[]): string {
  const kinds = [...new Set(parts.map((part) => part.kind ?? "the tag's"))];
  return kinds.join(", ");
}

/** What differs between two drafts of a row, in words. */
function changesOf(before: Draft, after: Draft): string[] {
  const changes: string[] = [];
  if (before.fromGuess && !after.fromGuess && labelsText(after) === labelsText({ ...before, fromGuess: false })) {
    changes.push(`accepts the guess ${labelsText(after)}`);
  } else if (labelsText(before) !== labelsText(after)) {
    const replaced = before.parts.length > 1 ? " (the split is replaced)" : "";
    changes.push(`tags: ${labelsText(before)} → ${labelsText(after)}${replaced}`);
  }
  const beforeKinds = tagged(before) ? kindsText(before.parts) : "the tag's";
  const afterKinds = kindsText(after.parts);
  if (beforeKinds !== afterKinds) {
    const parts = after.parts.length > 1 ? ` on all ${after.parts.length} parts` : "";
    changes.push(`kind: ${beforeKinds} → ${afterKinds}${parts}`);
  }
  const partDescriptions = (draft: Draft) => draft.parts.map((part) => part.description.trim()).filter(Boolean).join(", ");
  if (partDescriptions(before) && partDescriptions(before) !== partDescriptions(after)) changes.push("part descriptions cleared");
  if (before.description.trim() !== after.description.trim()) {
    changes.push(after.description.trim() ? `description: “${oneLine(after.description.trim())}”` : "description cleared");
  }
  if (before.needsReview !== after.needsReview) {
    changes.push(after.needsReview ? "flagged for review" : "marked reviewed");
  }
  return changes;
}

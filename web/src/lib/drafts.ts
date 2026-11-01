// Drafts: the unsaved edits of a transaction's annotation in a table row.
//
// A draft starts from the stored annotation (the baseline), or from the guess
// for an untagged transaction. Nothing is written until the drafts are
// submitted, and only drafts that differ from their baseline ("dirty") are.
import type { PartIn, PatchBatchItem, Transaction } from "./api";
import type { TagKind } from "./kinds";
import { DECIMALS, formatMinor, fromMinor, parseAmount, signOf, toMinor } from "./money";

export interface PartDraft {
  tag: string;
  subtag: string;
  /** As typed. Empty for a single part, which covers the full amount. */
  amount: string;
  description: string;
  /** The kind set on this transaction, over its tag's. Null inherits the tag's. */
  kind: TagKind | null;
}

export interface Draft {
  description: string;
  needsReview: boolean;
  /** At least one, possibly with an empty tag (untagged). */
  parts: PartDraft[];
  /** The first part holds the guess, which isn't applied unless accepted. */
  fromGuess: boolean;
}

export function emptyPart(): PartDraft {
  return { tag: "", subtag: "", amount: "", description: "", kind: null };
}

/** A split part's amount as typed: without a sign if it has the transaction's. */
export function amountText(minor: number, transactionMinor: number): string {
  const plain = fromMinor(Math.abs(minor)).toFixed(DECIMALS);
  if (signOf(minor) === signOf(transactionMinor)) return plain;
  return `${minor < 0 ? "-" : "+"}${plain}`;
}

/** The draft of the stored annotation, as is. */
export function baselineDraft(transaction: Transaction): Draft {
  const annotation = transaction.annotation;
  const total = toMinor(transaction.amount);
  const parts = annotation?.parts ?? [];
  return {
    description: annotation?.description ?? "",
    needsReview: annotation?.needs_review ?? false,
    parts:
      parts.length === 0
        ? [emptyPart()]
        : parts.map((part) => ({
            tag: part.tag,
            subtag: part.subtag,
            amount: parts.length > 1 ? amountText(toMinor(part.amount), total) : "",
            description: part.description ?? "",
            kind: part.kind_set ? part.kind : null,
          })),
    fromGuess: false,
  };
}

/** The draft a row starts with: the baseline, with the guess if untagged. */
export function initialDraft(transaction: Transaction): Draft {
  const draft = baselineDraft(transaction);
  const guess = transaction.guess;
  const hasParts = (transaction.annotation?.parts.length ?? 0) > 0;
  if (guess && !hasParts && transaction.status === "untagged") {
    draft.parts = [{ ...emptyPart(), tag: guess.tag, subtag: guess.subtag }];
    draft.fromGuess = true;
  }
  return draft;
}

/** Accept the guess: the draft then holds it as an edit. */
export function acceptGuess(draft: Draft): Draft {
  return { ...draft, fromGuess: false };
}

/** The parts that count: none while the guess isn't accepted, or if all empty. */
function effectiveParts(draft: Draft): PartDraft[] {
  if (draft.fromGuess) return [];
  const parts = draft.parts.map((part) => ({
    tag: part.tag.trim(),
    subtag: part.subtag.trim(),
    amount: part.amount.trim(),
    description: part.description.trim(),
    kind: part.kind,
  }));
  if (parts.length === 1 && !parts[0].tag && !parts[0].subtag && !parts[0].description && !parts[0].kind) {
    return [];
  }
  return parts;
}

export interface SplitState {
  isSplit: boolean;
  total: number;
  /** Per part: minor units, or an error message. */
  amounts: (number | string)[];
  /** The first part's amount is left empty, so it takes the rest. */
  firstTakesRest: boolean;
  /** The rest is zero or has the opposite sign: the other parts are too much. */
  restOverflows: boolean;
  /** Total minus the parts (minor units), zero once every amount is known. */
  remainder: number;
}

/**
 * The amounts of a draft's parts. In a split, the first part takes what the
 * others leave over while its amount is empty, so splitting off a part only
 * needs that part's amount. Other empty amounts count as zero (and are errors).
 */
export function splitState(transaction: Transaction, draft: Draft): SplitState {
  const total = toMinor(transaction.amount);
  const isSplit = draft.parts.length > 1;
  if (!isSplit) {
    return { isSplit, total, amounts: [total], firstTakesRest: false, restOverflows: false, remainder: 0 };
  }
  const amounts: (number | string)[] = draft.parts.map((part) => {
    if (!part.amount.trim()) return 0;
    const parsed = parseAmount(part.amount, signOf(total));
    return parsed.ok ? parsed.minor : parsed.error;
  });
  const sum = () =>
    amounts.reduce<number>((acc, amount) => acc + (typeof amount === "number" ? amount : 0), 0);
  const firstTakesRest = !draft.parts[0].amount.trim();
  if (firstTakesRest) amounts[0] = total - sum();
  const rest = amounts[0] as number;
  const restOverflows = firstTakesRest && (rest === 0 || signOf(rest) !== signOf(total));
  return { isSplit, total, amounts, firstTakesRest, restOverflows, remainder: total - sum() };
}

/** Problems that block submitting the draft, empty if valid. */
export function draftErrors(transaction: Transaction, draft: Draft): string[] {
  const parts = effectiveParts(draft);
  const errors: string[] = [];
  if (parts.length === 0) return errors;
  parts.forEach((part, index) => {
    const label = parts.length > 1 ? `part ${index + 1}: ` : "";
    if (!part.tag) errors.push(`${label}tag is missing`);
  });
  if (parts.length > 1) {
    const split = splitState(transaction, draft);
    split.amounts.forEach((amount, index) => {
      if (typeof amount === "string") errors.push(`part ${index + 1}: ${amount}`);
      else if (index > 0 && !draft.parts[index].amount.trim()) {
        errors.push(`part ${index + 1}: amount is missing`);
      }
    });
    if (split.restOverflows) {
      const rest = formatMinor(split.amounts[0] as number, { signed: true });
      errors.push(`the other parts add up to more than the amount (part 1 would be ${rest})`);
    }
    if (split.remainder !== 0) {
      errors.push(`${formatMinor(split.remainder, { signed: true })} left to split`);
    }
  }
  return errors;
}

/** A comparable form of a draft: what would be stored. */
function normalized(transaction: Transaction, draft: Draft) {
  const parts = effectiveParts(draft);
  const split = splitState(transaction, draft);
  return JSON.stringify({
    description: draft.description.trim() || null,
    needsReview: draft.needsReview,
    parts: parts.map((part, index) => ({
      tag: part.tag,
      subtag: part.subtag,
      amount: parts.length > 1 ? split.amounts[index] : null,
      description: part.description || null,
      kind: part.kind,
    })),
  });
}

export function isDirty(transaction: Transaction, draft: Draft): boolean {
  return normalized(transaction, draft) !== normalized(transaction, baselineDraft(transaction));
}

/** Whether the tags, splits or descriptions differ from the baseline (not only review). */
export function contentChanged(transaction: Transaction, draft: Draft): boolean {
  const baseline = baselineDraft(transaction);
  return isDirty(transaction, { ...draft, needsReview: baseline.needsReview });
}

/**
 * The write that saves a draft: a patch of only the fields that differ from the
 * stored annotation, so that what changed meanwhile (e.g. an agent's new
 * description) is kept, or deleting the annotation.
 */
export type DraftAction = { kind: "patch"; item: PatchBatchItem } | { kind: "clear" } | { kind: "none" };

export function draftAction(transaction: Transaction, draft: Draft): DraftAction {
  if (!isDirty(transaction, draft)) return { kind: "none" };
  const hasAnnotation = transaction.annotation != null;
  const parts = partsIn(transaction, draft);
  const description = draft.description.trim() || null;
  if (parts.length === 0 && !description) {
    return hasAnnotation ? { kind: "clear" } : { kind: "none" };
  }
  const baseline = baselineDraft(transaction);
  const item: PatchBatchItem = { id: transaction.id };
  if (description !== (baseline.description.trim() || null)) item.description = description;
  if (draft.needsReview !== baseline.needsReview) item.needs_review = draft.needsReview;
  const baselineParts = partsIn(transaction, baseline);
  if (JSON.stringify(parts) !== JSON.stringify(baselineParts)) {
    // Only one kind set on every part (e.g. toggling one-off) keeps the parts
    const withoutKinds = (list: PartIn[]) => JSON.stringify(list.map((part) => ({ ...part, kind: null })));
    const kinds = new Set(parts.map((part) => part.kind ?? null));
    const onlyKind =
      transaction.status === "annotated" && kinds.size === 1 && withoutKinds(parts) === withoutKinds(baselineParts);
    if (onlyKind) item.kind = [...kinds][0];
    else item.parts = parts;
  }
  return { kind: "patch", item };
}

/** The parts that a draft would write. */
function partsIn(transaction: Transaction, draft: Draft): PartIn[] {
  const parts = effectiveParts(draft);
  const split = splitState(transaction, draft);
  return parts.map((part, index) => ({
    tag: part.tag,
    subtag: part.subtag,
    amount: parts.length > 1 ? fromMinor(split.amounts[index] as number) : null,
    description: part.description || null,
    kind: part.kind,
  }));
}

/** Labels (tag, subtag) a draft would write, to check for new tags. */
export function draftLabels(draft: Draft): [string, string][] {
  return effectiveParts(draft)
    .filter((part) => part.tag)
    .map((part) => [part.tag, part.subtag]);
}

/** Set a kind on every part (or clear it, with null), which accepts a guess. */
export function setKind(draft: Draft, kind: TagKind | null): Draft {
  return { ...draft, fromGuess: false, parts: draft.parts.map((part) => ({ ...part, kind })) };
}

/** Whether every part of a draft is set as a one-off. */
export function isOneOff(draft: Draft): boolean {
  return draft.parts.every((part) => part.kind === "one-off");
}

/** Split off a new part. A single part becomes the first of the split. */
export function addPart(draft: Draft): Draft {
  const parts = [...draft.parts.map((part) => ({ ...part })), emptyPart()];
  return { ...draft, parts, fromGuess: false };
}

export function removePart(draft: Draft, index: number): Draft {
  const parts = draft.parts.filter((_, partIndex) => partIndex !== index);
  if (parts.length === 0) parts.push(emptyPart());
  if (parts.length === 1) parts[0] = { ...parts[0], amount: "" };
  return { ...draft, parts };
}

/** Fill a split part with what the other parts leave over. */
export function fillRemainder(transaction: Transaction, draft: Draft, index: number): Draft {
  const split = splitState(transaction, draft);
  const current = split.amounts[index];
  let value = split.remainder + (typeof current === "number" ? current : 0);
  if (split.firstTakesRest && index > 0) value += split.amounts[0] as number;
  const parts = draft.parts.map((part, partIndex) =>
    partIndex === index ? { ...part, amount: amountText(value, split.total) } : part,
  );
  return { ...draft, parts };
}

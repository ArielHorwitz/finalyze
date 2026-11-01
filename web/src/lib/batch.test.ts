import { describe, expect, it } from "vitest";
import type { Transaction } from "./api";
import { batchDraft, hasFields, type BatchFields } from "./batch";
import { baselineDraft, initialDraft, isDirty } from "./drafts";
import { GUESS, makeTransaction } from "./test-helpers";

const NONE: BatchFields = { label: null, kind: null, description: null, needsReview: null };

const tagged = makeTransaction({
  status: "annotated",
  annotation: {
    description: "keep me",
    authors: ["owner"],
    needs_review: true,
    parts: [{ tag: "car", subtag: "fuel", kind: "regular", kind_set: false, amount: -100 }],
  },
});
const split = makeTransaction({
  id: "t2",
  status: "annotated",
  annotation: {
    description: null,
    authors: ["owner"],
    needs_review: false,
    parts: [
      { tag: "food", subtag: "groceries", kind: "regular", kind_set: false, amount: -60 },
      { tag: "shopping", subtag: "gifts", kind: "one-off", kind_set: true, amount: -40 },
    ],
  },
});
const untagged = makeTransaction({ id: "t3" });
const ruled = makeTransaction({ id: "t4", status: "rule" });

function outcome(transaction: Transaction, fields: Partial<BatchFields>) {
  return batchDraft(transaction, initialDraft(transaction), { ...NONE, ...fields });
}

describe("batch edits", () => {
  it("need a field", () => {
    expect(hasFields(NONE)).toBe(false);
    expect(hasFields({ ...NONE, description: "" })).toBe(true);
  });

  it("set only the kind, keeping the tags, description and flag", () => {
    const result = outcome(tagged, { kind: "one-off" });
    if (!("draft" in result)) throw new Error("skipped");
    expect(result.draft).toMatchObject({ description: "keep me", needsReview: true });
    expect(result.draft.parts).toMatchObject([{ tag: "car", subtag: "fuel", kind: "one-off" }]);
    expect(result.changes).toEqual(["kind: the tag's → one-off"]);
  });

  it("set a kind on every part of a split, and say so", () => {
    const result = outcome(split, { kind: "one-off" });
    if (!("draft" in result)) throw new Error("skipped");
    expect(result.draft.parts.map((part) => part.kind)).toEqual(["one-off", "one-off"]);
    expect(result.changes).toEqual(["kind: the tag's, one-off → one-off on all 2 parts"]);
    const cleared = outcome(split, { kind: "inherit" });
    if (!("draft" in cleared)) throw new Error("skipped");
    expect(cleared.draft.parts.map((part) => part.kind)).toEqual([null, null]);
  });

  it("skip a kind on untagged rows, but accept a guess as O does", () => {
    expect(outcome(untagged, { kind: "one-off" })).toEqual({ skipped: "untagged: a kind needs a tag (include one)" });
    const guessed = outcome(makeTransaction({ guess: GUESS }), { kind: "one-off" });
    if (!("draft" in guessed)) throw new Error("skipped");
    expect(guessed.draft).toMatchObject({ fromGuess: false, parts: [{ tag: "food", kind: "one-off" }] });
    expect(guessed.changes).toEqual(["accepts the guess food / groceries", "kind: the tag's → one-off"]);
  });

  it("skip rules' rows for every field", () => {
    expect(outcome(ruled, { description: "x" })).toHaveProperty("skipped");
  });

  it("retag, keeping a kind set on all the parts", () => {
    const oneOff = batchDraft(tagged, { ...baselineDraft(tagged), parts: [{ tag: "car", subtag: "fuel", amount: "", description: "", kind: "one-off" }] }, {
      ...NONE,
      label: { tag: "car", subtag: "purchase" },
    });
    if (!("draft" in oneOff)) throw new Error("skipped");
    expect(oneOff.draft.parts).toEqual([{ tag: "car", subtag: "purchase", amount: "", description: "", kind: "one-off" }]);
    expect(oneOff.changes).toEqual(["tags: car / fuel → car / purchase"]);
    const retagged = outcome(split, { label: { tag: "car", subtag: "purchase" } });
    if (!("draft" in retagged)) throw new Error("skipped");
    expect(retagged.draft.parts).toMatchObject([{ kind: null }]);
    expect(retagged.changes[0]).toContain("(the split is replaced)");
    // Even to the same tags, a part's description is dropped
    const partDescribed = { ...baselineDraft(tagged), parts: [{ tag: "car", subtag: "fuel", amount: "", description: "pump 4", kind: null }] };
    const sameTags = batchDraft(tagged, partDescribed, { ...NONE, label: { tag: "car", subtag: "fuel" } });
    if (!("draft" in sameTags)) throw new Error("skipped");
    expect(sameTags.changes).toEqual(["part descriptions cleared"]);
  });

  it("set descriptions and the flag, skipping a flag on nothing", () => {
    const described = outcome(untagged, { description: "ask", needsReview: true });
    if (!("draft" in described)) throw new Error("skipped");
    expect(isDirty(untagged, described.draft)).toBe(true);
    expect(described.changes).toEqual(["description: “ask”", "flagged for review"]);
    expect(outcome(untagged, { needsReview: true })).toEqual({ skipped: "nothing to flag: no tags or description" });
    const unchanged = outcome(tagged, { needsReview: true });
    if (!("draft" in unchanged)) throw new Error("skipped");
    expect(unchanged.changes).toEqual([]);
  });
});

import { describe, expect, it } from "vitest";
import {
  acceptGuess,
  addPart,
  draftAction,
  draftErrors,
  fillRemainder,
  initialDraft,
  isDirty,
  isOneOff,
  removePart,
  setKind,
  splitState,
  type Draft,
} from "./drafts";
import { GUESS, makeTransaction } from "./test-helpers";

const withParts = (draft: Draft, parts: [string, string, string][]): Draft => ({
  ...draft,
  fromGuess: false,
  parts: parts.map(([tag, subtag, amount]) => ({ tag, subtag, amount, description: "", kind: null })),
});

describe("guesses", () => {
  const transaction = makeTransaction({ guess: GUESS });

  it("pre-fill the draft without making it dirty", () => {
    const draft = initialDraft(transaction);
    expect(draft.fromGuess).toBe(true);
    expect(draft.parts[0]).toMatchObject({ tag: "food", subtag: "groceries" });
    expect(isDirty(transaction, draft)).toBe(false);
    expect(draftAction(transaction, draft)).toEqual({ kind: "none" });
  });

  it("are written once accepted", () => {
    const draft = acceptGuess(initialDraft(transaction));
    expect(isDirty(transaction, draft)).toBe(true);
    expect(draftAction(transaction, draft)).toEqual({
      kind: "patch",
      item: {
        id: "t1",
        parts: [{ tag: "food", subtag: "groceries", amount: null, description: null, kind: null }],
      },
    });
  });

  it("aren't used for annotated transactions", () => {
    const annotated = makeTransaction({
      status: "annotated",
      guess: GUESS,
      annotation: {
        description: "n",
        parts: [{ tag: "car", subtag: "fuel", kind: "regular", kind_set: false, amount: -100 }],
        authors: ["owner"],
        needs_review: false,
      },
    });
    expect(initialDraft(annotated)).toMatchObject({ fromGuess: false, description: "n" });
  });
});

describe("splits", () => {
  const transaction = makeTransaction({ amount: -100.5 });

  it("let the first part take the rest while its amount is empty", () => {
    const draft = withParts(initialDraft(transaction), [
      ["food", "groceries", ""],
      ["shopping", "gifts", "40"],
    ]);
    const split = splitState(transaction, draft);
    expect(split.amounts).toEqual([-6050, -4000]);
    expect(split.firstTakesRest).toBe(true);
    expect(split.remainder).toBe(0);
    expect(draftErrors(transaction, draft)).toEqual([]);
    const action = draftAction(transaction, draft);
    expect(action.kind === "patch" && action.item.parts?.map((part) => part.amount)).toEqual([-60.5, -40]);
  });

  it("sum exactly in minor units", () => {
    // 0.1 + 0.2 as floats is 0.30000000000000004
    const small = makeTransaction({ amount: -0.3 });
    const draft = withParts(initialDraft(small), [
      ["food", "groceries", "0.1"],
      ["food", "groceries", "0.2"],
    ]);
    expect(splitState(small, draft).remainder).toBe(0);
    expect(draftErrors(small, draft)).toEqual([]);
  });

  it("block submitting until they sum exactly", () => {
    const draft = withParts(initialDraft(transaction), [
      ["food", "groceries", "60"],
      ["shopping", "gifts", "40"],
    ]);
    expect(splitState(transaction, draft).remainder).toBe(-50);
    expect(draftErrors(transaction, draft)).toEqual(["−0.50 left to split"]);
  });

  it("report bad and missing amounts and tags", () => {
    const draft = withParts(initialDraft(transaction), [
      ["food", "groceries", "1.234"],
      ["", "gifts", ""],
    ]);
    expect(draftErrors(transaction, draft)).toEqual([
      "part 2: tag is missing",
      "part 1: at most 2 decimals",
      "part 2: amount is missing",
      "−100.50 left to split",
    ]);
  });

  it("refuse a rest that flips the sign", () => {
    const draft = withParts(initialDraft(transaction), [
      ["food", "groceries", ""],
      ["shopping", "gifts", "1000"],
    ]);
    expect(draftErrors(transaction, draft)).toEqual([
      "the other parts add up to more than the amount (part 1 would be +899.50)",
    ]);
  });

  it("keep explicit opposite signs", () => {
    const draft = withParts(initialDraft(transaction), [
      ["food", "groceries", ""],
      ["refund", "store", "+20"],
    ]);
    expect(splitState(transaction, draft).amounts).toEqual([-12050, 2000]);
  });

  it("add, fill and remove parts", () => {
    let draft = addPart(withParts(initialDraft(transaction), [["food", "groceries", ""]]));
    expect(draft.parts).toHaveLength(2);
    draft = { ...draft, parts: [draft.parts[0], { ...draft.parts[1], tag: "x", subtag: "y" }] };
    draft = addPart(draft);
    draft = { ...draft, parts: [draft.parts[0], { ...draft.parts[1], amount: "30" }, draft.parts[2]] };
    // The third part takes what the first part took implicitly
    draft = fillRemainder(transaction, draft, 2);
    expect(draft.parts[2].amount).toBe("70.50");
    expect(splitState(transaction, draft).amounts).toEqual([0, -3000, -7050]);
    draft = removePart(removePart(draft, 2), 1);
    expect(draft.parts).toEqual([{ tag: "food", subtag: "groceries", amount: "", description: "", kind: null }]);
  });

  it("load from a stored split", () => {
    const stored = makeTransaction({
      amount: -100,
      status: "annotated",
      annotation: {
        description: null,
        authors: ["owner"],
        needs_review: false,
        parts: [
          { tag: "food", subtag: "groceries", kind: "regular", kind_set: false, amount: -70 },
          { tag: "refund", subtag: "store", kind: "regular", kind_set: false, amount: 10 },
          { tag: "shopping", subtag: "gifts", kind: "regular", kind_set: false, amount: -40, description: "gift" },
        ],
      },
    });
    const draft = initialDraft(stored);
    expect(draft.parts.map((part) => part.amount)).toEqual(["70.00", "+10.00", "40.00"]);
    expect(isDirty(stored, draft)).toBe(false);
  });
});

describe("draft actions", () => {
  const annotated = makeTransaction({
    status: "annotated",
    annotation: {
      description: "weekly",
      parts: [{ tag: "food", subtag: "groceries", kind: "regular", kind_set: false, amount: -100 }],
      authors: ["claude"],
      needs_review: true,
    },
  });

  it("patch only the review flag when only it changed", () => {
    const draft = { ...initialDraft(annotated), needsReview: false };
    expect(draftAction(annotated, draft)).toEqual({ kind: "patch", item: { id: "t1", needs_review: false } });
  });

  it("patch only the description when only it changed", () => {
    const draft = { ...initialDraft(annotated), description: " weekly shop " };
    expect(draftAction(annotated, draft)).toEqual({ kind: "patch", item: { id: "t1", description: "weekly shop" } });
    const cleared = { ...initialDraft(annotated), description: "" };
    expect(draftAction(annotated, cleared)).toEqual({ kind: "patch", item: { id: "t1", description: null } });
  });

  it("patch the parts when the tags changed, not the rest", () => {
    const draft = withParts(initialDraft(annotated), [["car", "fuel", ""]]);
    expect(draftAction(annotated, draft)).toEqual({
      kind: "patch",
      item: { id: "t1", parts: [{ tag: "car", subtag: "fuel", amount: null, description: null, kind: null }] },
    });
  });

  it("clear an annotation whose tags and description are emptied", () => {
    const draft = withParts({ ...initialDraft(annotated), description: "" }, [["", "", ""]]);
    expect(draftAction(annotated, draft)).toEqual({ kind: "clear" });
  });

  it("write a description only", () => {
    const transaction = makeTransaction();
    const draft = { ...initialDraft(transaction), description: "what was this?", needsReview: true };
    expect(draftAction(transaction, draft)).toEqual({
      kind: "patch",
      item: { id: "t1", description: "what was this?", needs_review: true },
    });
  });

  it("set a kind on the transaction, and clear it", () => {
    const oneOff = setKind(initialDraft(annotated), "one-off");
    expect(isOneOff(oneOff)).toBe(true);
    // Only the kind, keeping the parts
    expect(draftAction(annotated, oneOff)).toEqual({ kind: "patch", item: { id: "t1", kind: "one-off" } });
    // A stored kind loads into the draft, and clearing it is an edit
    const stored = makeTransaction({
      status: "annotated",
      annotation: { ...annotated.annotation!, parts: [{ ...annotated.annotation!.parts[0], kind: "one-off", kind_set: true }] },
    });
    const loaded = initialDraft(stored);
    expect(loaded.parts[0].kind).toBe("one-off");
    expect(isDirty(stored, loaded)).toBe(false);
    expect(draftAction(stored, setKind(loaded, null))).toEqual({ kind: "patch", item: { id: "t1", kind: null } });
    // A kind with other changes to the parts goes with the parts
    const retagged = setKind(withParts(loaded, [["car", "fuel", ""]]), "one-off");
    expect(draftAction(stored, retagged)).toMatchObject({ item: { parts: [{ tag: "car", kind: "one-off" }] } });
    // The tag's kind isn't stored as set
    expect(initialDraft(annotated).parts[0].kind).toBeNull();
  });

  it("need a tag for a kind", () => {
    const transaction = makeTransaction();
    const draft = setKind(initialDraft(transaction), "one-off");
    expect(draftErrors(transaction, draft)).toEqual(["tag is missing"]);
  });

  it("ignore whitespace-only edits", () => {
    const draft = { ...initialDraft(annotated), description: "weekly  " };
    expect(isDirty(annotated, draft)).toBe(false);
  });
});

import { describe, expect, it } from "vitest";
import { acceptGuess } from "./drafts";
import { Rows } from "./rows.svelte";
import { GUESS, makeTransaction } from "./test-helpers";

const annotated = (subtag: string) =>
  makeTransaction({
    status: "annotated",
    annotation: { description: null, authors: ["owner"], needs_review: false, parts: [{ tag: "food", subtag, kind: "regular", kind_set: false, amount: -100 }] },
  });

describe("Rows.load", () => {
  it("keeps unsaved edits across reloads", () => {
    const rows = new Rows();
    rows.load([makeTransaction({ guess: GUESS })]);
    rows.setDraft(rows.rows[0], acceptGuess(rows.rows[0].draft));
    rows.load([makeTransaction({ guess: GUESS })]);
    expect(rows.dirtyRows).toHaveLength(1);
  });

  it("drops edits that were written", () => {
    const rows = new Rows();
    rows.load([makeTransaction({ guess: GUESS })]);
    rows.setDraft(rows.rows[0], acceptGuess(rows.rows[0].draft));
    rows.load([annotated("groceries")]);
    expect(rows.dirtyRows).toHaveLength(0);
  });

  it("refreshes rows without edits from changed data", () => {
    // E.g. after a batch apply: the old draft must not become a reverse edit
    const rows = new Rows();
    rows.load([annotated("groceries")]);
    rows.load([annotated("dining out")]);
    expect(rows.dirtyRows).toHaveLength(0);
    expect(rows.rows[0].draft.parts[0].subtag).toBe("dining out");
  });
});

describe("Rows selection", () => {
  const first = makeTransaction({ id: "first" });
  const second = makeTransaction({ id: "second" });

  it("is kept per view, for when the view is shown again", () => {
    const rows = new Rows("test:untagged");
    rows.load([first, second]);
    rows.toggleSelected(rows.rows[0]);
    rows.setView("test:review");
    expect(rows.selectedRows).toHaveLength(0);
    rows.setView("test:untagged");
    expect(rows.selectedRows.map((row) => row.transaction.id)).toEqual(["first"]);
    // As when the page is opened again
    const again = new Rows("test:untagged");
    again.load([first, second]);
    expect(again.selectedRows.map((row) => row.transaction.id)).toEqual(["first"]);
  });

  it("drops the selected rows that leave the view, and acts only on those shown", () => {
    const rows = new Rows("test:filter");
    rows.load([first, second]);
    rows.selectAll();
    rows.load([second]);
    expect([...rows.selectedIds]).toEqual(["second"]);
    expect(new Rows("test:filter").selectedIds).toEqual(new Set(["second"]));
    // Until a load prunes it, only the rows shown count: none here, so the cursor's
    rows.selectedIds = new Set(["first"]);
    expect(rows.selectedRows).toHaveLength(0);
    expect(rows.targets().map((row) => row.transaction.id)).toEqual(["second"]);
  });
});

describe("Rows.toggleOneOff", () => {
  it("sets the rows as one-offs, then back to their tags' kinds, skipping rules' rows", () => {
    const rows = new Rows();
    const ruled = makeTransaction({ id: "ruled", status: "rule" });
    rows.load([{ ...annotated("groceries"), id: "a" }, { ...annotated("dining out"), id: "b" }, ruled]);
    expect(rows.toggleOneOff(rows.rows)).toBe(2);
    expect(rows.rows.map((row) => row.draft.parts[0].kind)).toEqual(["one-off", "one-off", null]);
    expect(rows.dirtyRows).toHaveLength(2);
    rows.toggleOneOff(rows.rows);
    expect(rows.rows.map((row) => row.draft.parts[0].kind)).toEqual([null, null, null]);
    expect(rows.dirtyRows).toHaveLength(0);
  });
});

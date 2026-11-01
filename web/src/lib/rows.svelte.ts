// The state of a table of transactions being annotated: each row's draft,
// the cursor, the selection, and submitting the drafts.
import { api, ApiError, type PatchBatchItem, type Transaction } from "./api";
import {
  acceptGuess,
  baselineDraft,
  contentChanged,
  draftAction,
  draftErrors,
  draftLabels,
  initialDraft,
  isDirty,
  isOneOff,
  setKind,
  type Draft,
} from "./drafts";
import { savedSelection, saveSelection } from "./selections";
import {
  confirm,
  labelExists,
  refreshStatus,
  refreshTags,
  toast,
  toastError,
} from "./state.svelte";

export interface Row {
  transaction: Transaction;
  draft: Draft;
  /** From the last submit, shown in the row until it changes. */
  error: string | null;
}

export interface SubmitResult {
  written: number;
  failed: number;
}

export class Rows {
  rows = $state<Row[]>([]);
  cursor = $state(0);
  submitting = $state(false);
  #selectedIds = $state<Set<string>>(new Set());
  /** The view whose selection this is, remembered for when it is shown again. */
  #view: string | null = null;

  dirtyRows = $derived(this.rows.filter((row) => isDirty(row.transaction, row.draft)));
  /**
   * The selected rows that are shown: the only ones any action touches, and
   * the ones counted. (Until a load prunes it, `selectedIds` can hold more.)
   */
  selectedRows = $derived(this.rows.filter((row) => this.#selectedIds.has(row.transaction.id)));
  guessRows = $derived(this.rows.filter((row) => row.draft.fromGuess));

  constructor(view: string | null = null) {
    if (view) this.setView(view);
  }

  get selectedIds(): Set<string> {
    return this.#selectedIds;
  }

  set selectedIds(ids: Set<string>) {
    this.#selectedIds = ids;
    if (this.#view) saveSelection(this.#view, ids);
  }

  /** Show another view's selection (e.g. another queue), as it was left. */
  setView(view: string) {
    this.#view = view;
    this.#selectedIds = savedSelection(view);
  }

  /**
   * Show these transactions. Unsaved edits of transactions that are still
   * listed are kept (unless `keepDrafts` is false), if they still differ from
   * what is stored: after a submit, the written drafts don't. Rows without
   * edits start over from the new data (e.g. after a batch apply).
   */
  load(transactions: Transaction[], { keepDrafts = true } = {}) {
    const previous = new Map(this.rows.map((row) => [row.transaction.id, row]));
    const cursorId = this.rows[this.cursor]?.transaction.id;
    this.rows = transactions.map((transaction) => {
      const old = previous.get(transaction.id);
      const edited = old && isDirty(old.transaction, old.draft);
      if (keepDrafts && old && edited && isDirty(transaction, old.draft)) {
        return { transaction, draft: old.draft, error: old.error };
      }
      return { transaction, draft: initialDraft(transaction), error: null };
    });
    // The selected rows that left the view (filtered out, or changed) are dropped
    const ids = new Set(transactions.map((transaction) => transaction.id));
    this.selectedIds = new Set([...this.selectedIds].filter((id) => ids.has(id)));
    const cursorIndex = this.rows.findIndex((row) => row.transaction.id === cursorId);
    this.moveTo(cursorIndex >= 0 ? cursorIndex : this.cursor);
  }

  get current(): Row | undefined {
    return this.rows[this.cursor];
  }

  moveTo(index: number) {
    this.cursor = Math.max(0, Math.min(index, this.rows.length - 1));
  }

  move(step: number) {
    this.moveTo(this.cursor + step);
  }

  /** The selected rows, or the row under the cursor if none is selected. */
  targets(): Row[] {
    if (this.selectedRows.length > 0) return this.selectedRows;
    return this.current ? [this.current] : [];
  }

  setDraft(row: Row, draft: Draft) {
    // Editing a flagged annotation's content counts as reviewing it. The flag
    // can be set again before submitting
    const wasChanged = contentChanged(row.transaction, row.draft);
    row.draft = draft;
    row.error = null;
    if (
      !wasChanged &&
      row.transaction.annotation?.needs_review &&
      draft.needsReview &&
      contentChanged(row.transaction, draft)
    ) {
      row.draft = { ...draft, needsReview: false };
    }
  }

  toggleSelected(row: Row) {
    const ids = new Set(this.selectedIds);
    if (ids.has(row.transaction.id)) ids.delete(row.transaction.id);
    else ids.add(row.transaction.id);
    this.selectedIds = ids;
  }

  selectAll() {
    this.selectedIds = new Set(this.rows.map((row) => row.transaction.id));
  }

  clearSelection() {
    this.selectedIds = new Set();
  }

  acceptGuesses(rows: Row[]) {
    for (const row of rows) if (row.draft.fromGuess) this.setDraft(row, acceptGuess(row.draft));
  }

  /** Toggle the review flag: all on, unless all targets are on already. */
  toggleReview(rows: Row[]) {
    const value = !rows.every((row) => row.draft.needsReview);
    for (const row of rows) {
      row.draft = { ...row.draft, needsReview: value };
      row.error = null;
    }
  }

  /** Set the rows as one-offs (every part), unless all of them are already:
   * then they inherit their tags' kinds again. Rules' rows can't have a kind. */
  toggleOneOff(rows: Row[]): number {
    const editable = rows.filter((row) => row.transaction.status !== "rule");
    const kind = editable.every((row) => isOneOff(row.draft)) ? null : "one-off";
    for (const row of editable) this.setDraft(row, setKind(row.draft, kind));
    return editable.length;
  }

  revert(rows: Row[]) {
    for (const row of rows) {
      row.draft = initialDraft(row.transaction);
      row.error = null;
    }
  }

  /** Empty the tags and descriptions, which deletes the annotation on submit. */
  clear(rows: Row[]) {
    for (const row of rows) {
      const draft = baselineDraft(row.transaction);
      this.setDraft(row, {
        ...draft,
        description: "",
        needsReview: false,
        parts: [{ tag: "", subtag: "", amount: "", description: "", kind: null }],
      });
    }
  }

  /**
   * Write the dirty drafts (of the selected rows, with `onlySelected`).
   *
   * Changes go in one atomic batch of patches, which only write the fields
   * that were edited: if any is invalid, nothing is written and each row shows
   * its error. Then come clears.
   */
  async submit({ onlySelected = false } = {}): Promise<SubmitResult | null> {
    const candidates = onlySelected ? this.selectedRows.filter((row) => isDirty(row.transaction, row.draft)) : this.dirtyRows;
    if (candidates.length === 0 || this.submitting) return null;
    // Validate locally first
    let failed = 0;
    const patches: [Row, PatchBatchItem][] = [];
    const clears: Row[] = [];
    for (const row of candidates) {
      const errors = draftErrors(row.transaction, row.draft);
      const action = draftAction(row.transaction, row.draft);
      if (errors.length === 0 && action.kind === "none") {
        errors.push("add tags or a description to flag it for review");
      }
      row.error = errors.length ? errors.join(", ") : null;
      if (errors.length) failed++;
      else if (action.kind === "patch") patches.push([row, action.item]);
      else if (action.kind === "clear") clears.push(row);
    }
    if (failed) return { written: 0, failed };
    const newLabels = [
      ...new Set(
        candidates
          .flatMap((row) => draftLabels(row.draft))
          .filter(([tag, subtag]) => !labelExists(tag, subtag))
          .map(([tag, subtag]) => (subtag ? `${tag} / ${subtag}` : `${tag} (no subtag)`)),
      ),
    ];
    if (newLabels.length > 0) {
      const confirmed = await confirm(`Create new tags?\n\n${newLabels.join("\n")}`, {
        title: "New tags",
        confirmLabel: "Create and submit",
      });
      if (!confirmed) return null;
    }
    this.submitting = true;
    try {
      if (patches.length > 0) {
        try {
          await api.patchAnnotations({
            items: patches.map(([, item]) => item),
            author: "owner",
            allow_new_tags: newLabels.length > 0,
            dry_run: false,
          });
        } catch (error) {
          if (error instanceof ApiError && error.errors.length > 0) {
            for (const itemError of error.errors) {
              const row = patches[itemError.index]?.[0];
              if (row) row.error = itemError.message;
            }
            return { written: 0, failed: error.errors.length };
          }
          throw error;
        }
      }
      if (clears.length > 0) {
        await api.clearAnnotations(clears.map((row) => row.transaction.id));
      }
      return { written: patches.length + clears.length, failed: 0 };
    } finally {
      this.submitting = false;
    }
  }
}

/** Submit a table's drafts, report the outcome, and reload after a write. */
export async function submitRows(
  rows: Rows,
  reload: () => Promise<void>,
  { onlySelected = false } = {},
) {
  try {
    const result = await rows.submit({ onlySelected });
    if (!result) {
      if (rows.dirtyRows.length === 0) toast("Nothing to submit: no edits");
      return;
    }
    if (result.failed > 0) {
      const rowsText = result.failed === 1 ? "1 row has errors" : `${result.failed} rows have errors`;
      toast(`Nothing saved: ${rowsText}`, "error");
      return;
    }
    toast(`Saved ${result.written} annotation${result.written === 1 ? "" : "s"}`, "success");
    rows.clearSelection();
    await Promise.all([reload(), refreshStatus(), refreshTags()]);
  } catch (error) {
    toastError(error, "Submit failed");
  }
}

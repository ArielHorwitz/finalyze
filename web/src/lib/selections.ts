// Each view's selection (a queue of Annotate, Transactions, Orphans), kept
// while the app is open: looking at another view, or another page, doesn't
// lose it. Only for the session, a reload starts over.

const selections = new Map<string, Set<string>>();

export function savedSelection(view: string): Set<string> {
  return selections.get(view) ?? new Set();
}

export function saveSelection(view: string, ids: Set<string>) {
  selections.set(view, ids);
}

// Descriptions may have several lines (Shift+Enter in the editor). Tables show
// the first line and how many more there are, with the whole description on
// hover.

/** A description's lines. */
export function descriptionLines(description: string): string[] {
  return description.split(/\r\n|\r|\n/);
}

/** The first line, and the number of lines after it (empty ones too). */
export function descriptionSummary(description: string): { first: string; more: number } {
  const lines = descriptionLines(description);
  return { first: lines[0], more: lines.length - 1 };
}

/** "+1 line", "+3 lines". */
export function moreLinesText(more: number): string {
  return `+${more} ${more === 1 ? "line" : "lines"}`;
}

/** A description on one line, for text: its first line, and how many more, e.g. "lunch (+2 lines)". */
export function oneLine(description: string): string {
  const { first, more } = descriptionSummary(description);
  return more > 0 ? `${first} (${moreLinesText(more)})` : first;
}

// A transaction shows one description wherever it shows: its written one, with
// the raw description on hover; without one, the raw description stands in, in
// italics (see TransactionDescription).

/** The hover text of a written description: the whole of it, then the raw description. */
export function describedTitle(description: string, raw: string): string {
  return `${description}\nRaw description: ${raw}`;
}

type Described = { description?: string | null; parts: { description?: string | null }[] };

/**
 * What a transaction is, as written: its description, else its parts' (joined
 * by " · "), though not those of an annotation a rule overrides (its parts
 * don't apply). Null without one: the raw description stands in.
 */
export function writtenDescription(annotation: Described | null | undefined, overridden = false): string | null {
  if (!annotation) return null;
  if (annotation.description) return annotation.description;
  if (overridden) return null;
  const parts = annotation.parts.map((part) => part.description).filter((description) => !!description);
  return parts.length ? parts.join(" · ") : null;
}

/** An analysis row's (one per split part) written description: its part's, else its transaction's. */
export function rowDescription(row: { description?: string | null; transaction_description?: string | null }): string | null {
  return row.description || row.transaction_description || null;
}

/**
 * "rtl" when a text's first letter is of a right-to-left script (e.g. Hebrew),
 * else "ltr": what dir="auto" does, for where it can't, e.g. a placeholder.
 */
export function textDirection(text: string): "rtl" | "ltr" {
  const letter = text.match(/\p{L}/u)?.[0];
  return letter && /[\u0590-\u08ff\ufb1d-\ufdff\ufe70-\ufeff]/.test(letter) ? "rtl" : "ltr";
}

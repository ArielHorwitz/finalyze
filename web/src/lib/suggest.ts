// Suggestions for tag inputs: ranked matches of tags and subtags.
import type { Tag } from "./api";

export interface LabelSuggestion {
  tag: string;
  /** Null when suggesting a tag alone. */
  subtag: string | null;
  /** Current transactions tagged with it, to rank common labels first. */
  count: number;
}

/**
 * Rank how well a label matches a query, lower is better, null for no match.
 *
 * "gro" matches subtag "groceries", "food" matches the tag "food", and
 * "f/g" or "f g" matches "food / groceries" by prefixes of both.
 */
export function labelScore(query: string, tag: string, subtag: string | null): number | null {
  const needle = query.trim().toLowerCase();
  if (!needle) return 3;
  const tagName = tag.toLowerCase();
  const subtagName = (subtag ?? "").toLowerCase();
  const pair = /^(.+?)\s*[/ ]\s*(.*)$/.exec(needle);
  if (pair && subtag !== null) {
    const [, tagPart, subtagPart] = pair;
    if (tagName.startsWith(tagPart) && subtagName.startsWith(subtagPart)) return 0;
  }
  if (tagName.startsWith(needle)) return subtag === null ? 0 : 1;
  if (subtagName.startsWith(needle)) return 1;
  if (`${tagName}/${subtagName}`.includes(needle)) return 2;
  return null;
}

/** Tag and subtag pairs (and tags alone with `withTags`) matching a query. */
export function suggestLabels(
  tags: Tag[],
  query: string,
  { withTags = false, limit = 50 } = {},
): LabelSuggestion[] {
  const scored: [number, LabelSuggestion][] = [];
  for (const tag of tags) {
    if (withTags) {
      const score = labelScore(query, tag.name, null);
      if (score !== null) scored.push([score, { tag: tag.name, subtag: null, count: tag.transactions }]);
    }
    for (const subtag of tag.subtags) {
      const score = labelScore(query, tag.name, subtag.name);
      if (score !== null) {
        scored.push([score, { tag: tag.name, subtag: subtag.name, count: subtag.transactions }]);
      }
    }
  }
  scored.sort(([scoreA, a], [scoreB, b]) => scoreA - scoreB || b.count - a.count);
  return scored.slice(0, limit).map(([, suggestion]) => suggestion);
}

/** Subtags of a tag matching a query, most used first. */
export function suggestSubtags(tags: Tag[], tag: string, query: string): LabelSuggestion[] {
  const known = tags.find((item) => item.name === tag);
  if (!known) return [];
  const needle = query.trim().toLowerCase();
  return known.subtags
    .filter((subtag) => subtag.name.toLowerCase().includes(needle))
    .sort((a, b) => {
      const aPrefix = a.name.toLowerCase().startsWith(needle) ? 0 : 1;
      const bPrefix = b.name.toLowerCase().startsWith(needle) ? 0 : 1;
      return aPrefix - bPrefix || b.transactions - a.transactions;
    })
    .map((subtag) => ({ tag, subtag: subtag.name, count: subtag.transactions }));
}

/** Plain strings matching a query, prefix matches first. */
export function suggestNames(names: string[], query: string): string[] {
  const needle = query.trim().toLowerCase();
  const matches = names.filter((name) => name.toLowerCase().includes(needle));
  return matches.sort(
    (a, b) =>
      Number(!a.toLowerCase().startsWith(needle)) - Number(!b.toLowerCase().startsWith(needle)),
  );
}

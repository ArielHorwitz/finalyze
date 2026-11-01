// The pages of the app (their keys are in the keymap, see keymap.ts).

/** The nav's groups of pages, shown apart. */
export type RouteGroup = "see" | "routine" | "setup" | "reference";

export interface Route {
  path: string;
  title: string;
  group: RouteGroup;
}

/** In the nav's order. The first is the landing page. */
export const ROUTES: Route[] = [
  { path: "dashboard", title: "Dashboard", group: "see" },
  { path: "transactions", title: "Transactions", group: "see" },
  { path: "ingest", title: "Ingest", group: "routine" },
  { path: "annotate", title: "Annotate", group: "routine" },
  { path: "orphans", title: "Orphans", group: "routine" },
  { path: "tags", title: "Tags", group: "setup" },
  { path: "rules", title: "Rules", group: "setup" },
  { path: "accounts", title: "Accounts", group: "setup" },
  { path: "settings", title: "Settings", group: "setup" },
  { path: "glossary", title: "Glossary", group: "reference" },
];

/**
 * A route's title around its letter (after the go key), e.g. ["Ta", "g",
 * "s"], to underline it. A letter that isn't in the title gives the whole
 * title first.
 */
export function splitTitle(route: Route, letter: string | undefined): [string, string, string] {
  // Only a lower-case letter is the title's (a capital one takes Shift)
  const index = letter && /^[a-z]$/.test(letter) ? route.title.toLowerCase().indexOf(letter) : -1;
  if (index < 0) return [route.title, "", ""];
  return [route.title.slice(0, index), route.title[index], route.title.slice(index + 1)];
}

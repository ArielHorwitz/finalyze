// A tiny hash router: the page is the part of the URL after "#/", e.g.
// http://127.0.0.1:8765/#/tags. Hash URLs need no server-side routing.

import { ROUTES } from "./routes";

/** The page and its options, e.g. "#/annotate?review". */
function parseHash(): { path: string; query: string } {
  const [path, query = ""] = window.location.hash.replace(/^#\/?/, "").split("?");
  const known = ROUTES.some((route) => route.path === path);
  return { path: known ? path : ROUTES[0].path, query };
}

export const router = $state(parseHash());

/** Asks before leaving the page, e.g. when it has unsaved edits. */
type LeaveGuard = () => Promise<boolean>;
let leaveGuard: LeaveGuard | null = null;

/** Set while the current page has something to lose. Returns a remover. */
export function guardLeaving(guard: LeaveGuard): () => void {
  leaveGuard = guard;
  return () => {
    if (leaveGuard === guard) leaveGuard = null;
  };
}

const hashOf = ({ path, query }: { path: string; query: string }) =>
  query ? `#/${path}?${query}` : `#/${path}`;

window.addEventListener("hashchange", async () => {
  const next = parseHash();
  if (next.path !== router.path && leaveGuard) {
    // Stay on the page (without a new history entry) until confirmed
    history.replaceState(null, "", hashOf(router));
    if (!(await leaveGuard())) return;
    history.replaceState(null, "", hashOf(next));
  }
  Object.assign(router, next);
});

/**
 * Go to a page. The route changes at once (unlike setting the hash, whose
 * `hashchange` comes later), so keys typed right after go to the new page.
 */
export async function navigate(path: string, query = "") {
  const next = { path, query };
  if (hashOf(next) === window.location.hash) return;
  if (next.path !== router.path && leaveGuard && !(await leaveGuard())) return;
  history.pushState(null, "", hashOf(next));
  Object.assign(router, next);
}

/** Replace the page's options in the URL, without a history entry or a reload. */
export function setQuery(query: string) {
  router.query = query;
  history.replaceState(null, "", hashOf(router));
}

// The glossary (GET /api/glossary): what the app's figures, labels and
// concepts mean. It is loaded once, for the tooltips of labels (see Term) and
// the Glossary page.
//
// Tooltips are for the mouse only: labels with an entry carry a `data-term`
// attribute (and `data-term-context` for what the label's figure is of, e.g.
// the months of an average), and one listener on the document shows the
// tooltip of the label under the mouse. Nothing is focusable, so they change
// nothing in the keyboard's flow.
import { api, type GlossaryEntry } from "./api";

export const glossary = $state<{ entries: GlossaryEntry[]; byKey: Record<string, GlossaryEntry>; failed: boolean }>({
  entries: [],
  byKey: {},
  failed: false,
});

let loading: Promise<void> | null = null;

/** Loads the glossary, once (again after a failure). */
export function loadGlossary(): Promise<void> {
  loading ??= api
    .glossary()
    .then((entries) => {
      glossary.entries = entries;
      glossary.byKey = Object.fromEntries(entries.map((entry) => [entry.key, entry]));
      glossary.failed = false;
    })
    .catch(() => {
      glossary.failed = true;
      loading = null;
    });
  return loading;
}

/** The Glossary page's URL of an entry. */
export function glossaryHref(key: string): string {
  return `#/glossary?${encodeURIComponent(key)}`;
}

// The tooltip shown: the label under the mouse

export const termTooltip = $state<{ key: string | null; context: string; anchor: DOMRect | null }>({
  key: null,
  context: "",
  anchor: null,
});

const SHOW_DELAY = 350;
const HIDE_DELAY = 200;
let showTimer: ReturnType<typeof setTimeout> | undefined;
let hideTimer: ReturnType<typeof setTimeout> | undefined;

function showSoon(element: HTMLElement) {
  clearTimeout(hideTimer);
  clearTimeout(showTimer);
  const show = () => {
    termTooltip.key = element.dataset.term ?? null;
    termTooltip.context = element.dataset.termContext ?? "";
    termTooltip.anchor = element.getBoundingClientRect();
    loadGlossary();
  };
  // Moving from one label to another while a tooltip is open: at once
  if (termTooltip.key) show();
  else showTimer = setTimeout(show, SHOW_DELAY);
}

/** Hides the tooltip, after a moment (so the mouse can move into it). */
export function hideTermSoon() {
  clearTimeout(showTimer);
  clearTimeout(hideTimer);
  hideTimer = setTimeout(hideTerm, HIDE_DELAY);
}

/** Keeps the tooltip open, e.g. while the mouse is over it. */
export function keepTerm() {
  clearTimeout(hideTimer);
}

export function hideTerm() {
  clearTimeout(showTimer);
  clearTimeout(hideTimer);
  termTooltip.key = null;
}

/** Shows the tooltips of labels under the mouse (not touch or pen) in a
 * document. Returns a remover. */
export function followTerms(target: Document): () => void {
  let current: HTMLElement | null = null;
  const over = (event: PointerEvent) => {
    if (event.pointerType !== "mouse") return;
    const element = (event.target as Element | null)?.closest?.<HTMLElement>("[data-term]") ?? null;
    if (element === current) return;
    current = element;
    if (element) showSoon(element);
    else if (!(event.target as Element | null)?.closest?.(".term-tooltip")) hideTermSoon();
  };
  const leave = () => {
    current = null;
    hideTermSoon();
  };
  // Clicking (e.g. a card) or scrolling moves things: the tooltip goes
  const away = (event: Event) => {
    if ((event.target as Element | null)?.closest?.(".term-tooltip")) return;
    current = null;
    hideTerm();
  };
  target.addEventListener("pointerover", over);
  target.documentElement.addEventListener("pointerleave", leave);
  target.addEventListener("pointerdown", away, true);
  target.addEventListener("scroll", away, true);
  target.addEventListener("keydown", hideTerm, true);
  return () => {
    target.removeEventListener("pointerover", over);
    target.documentElement.removeEventListener("pointerleave", leave);
    target.removeEventListener("pointerdown", away, true);
    target.removeEventListener("scroll", away, true);
    target.removeEventListener("keydown", hideTerm, true);
  };
}

// The entries' Markdown: paragraphs, "- " lists, **bold**, *italics*, `code`
// and [text](#key) links to other entries. Nothing else is needed.

function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]!);
}

function inline(text: string): string {
  return escapeHtml(text)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|[^*])\*([^*]+)\*/g, "$1<em>$2</em>")
    .replace(/\[([^\]]+)\]\(#([a-z0-9_]+)\)/g, (_, label, key) => `<a href="${glossaryHref(key)}">${label}</a>`);
}

/** An entry's Markdown as HTML (escaped). */
export function renderMarkdown(markdown: string): string {
  const blocks = markdown.trim().split(/\n\s*\n/);
  return blocks
    .map((block) => {
      const lines = block.split("\n").map((line) => line.trim());
      if (lines.every((line) => line.startsWith("- "))) {
        return `<ul>${lines.map((line) => `<li>${inline(line.slice(2))}</li>`).join("")}</ul>`;
      }
      return `<p>${inline(lines.join(" "))}</p>`;
    })
    .join("");
}

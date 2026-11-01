// A cursor over a list (e.g. the rows of the tags or rules page), moved with
// the down and up keys (j and k by default) and kept in view.
import type { Binding } from "./keys";
import { bind } from "./use-keys.svelte";

export class Cursor {
  index = $state(0);

  constructor(private length: () => number) {}

  move(step: number) {
    this.moveTo(this.index + step);
  }

  moveTo(index: number) {
    this.index = Math.max(0, Math.min(index, this.length() - 1));
  }

  bindings(): Binding[] {
    return [
      bind("list.down", () => this.move(1)),
      bind("list.up", () => this.move(-1)),
      bind("list.first", () => this.moveTo(0)),
      bind("list.last", () => this.moveTo(this.length() - 1)),
    ];
  }
}

/** Scroll the element with `data-index` of the cursor into view. */
export function scrollToIndex(container: HTMLElement | undefined, index: number) {
  container?.querySelector(`[data-index="${index}"]`)?.scrollIntoView({ block: "nearest" });
}

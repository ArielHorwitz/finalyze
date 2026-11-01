import { describe, expect, it } from "vitest";
import type { Tag } from "./api";
import { labelScore, suggestLabels, suggestSubtags } from "./suggest";

const subtag = (name: string, transactions: number) => ({
  name,
  color: null,
  kind: null,
  effective_color: null,
  effective_kind: "regular" as const,
  annotations: 0,
  rules: 0,
  transactions,
});

const TAGS: Tag[] = [
  { name: "food", color: null, kind: "regular", transactions: 30, subtags: [subtag("groceries", 20), subtag("dining out", 10)] },
  { name: "shopping", color: null, kind: "regular", transactions: 5, subtags: [subtag("gifts", 5), subtag("groceries-like", 0)] },
];

describe("labelScore", () => {
  it("ranks pair prefixes and tag prefixes first", () => {
    expect(labelScore("f/g", "food", "groceries")).toBe(0);
    expect(labelScore("f g", "food", "groceries")).toBe(0);
    expect(labelScore("foo", "food", "groceries")).toBe(1);
    expect(labelScore("gro", "food", "groceries")).toBe(1);
    expect(labelScore("ceri", "food", "groceries")).toBe(2);
    expect(labelScore("car", "food", "groceries")).toBeNull();
  });
});

describe("suggestLabels", () => {
  it("lists matching pairs, most used first among equals", () => {
    const labels = suggestLabels(TAGS, "gro").map((item) => `${item.tag}/${item.subtag}`);
    expect(labels).toEqual(["food/groceries", "shopping/groceries-like"]);
  });

  it("lists everything for an empty query", () => {
    expect(suggestLabels(TAGS, "")).toHaveLength(4);
  });
});

describe("suggestSubtags", () => {
  it("lists the subtags of a tag", () => {
    expect(suggestSubtags(TAGS, "food", "").map((item) => item.subtag)).toEqual(["groceries", "dining out"]);
    expect(suggestSubtags(TAGS, "food", "out").map((item) => item.subtag)).toEqual(["dining out"]);
    expect(suggestSubtags(TAGS, "unknown", "")).toEqual([]);
  });
});

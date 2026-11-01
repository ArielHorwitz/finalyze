import { describe, expect, it } from "vitest";
import { describedTitle, descriptionSummary, moreLinesText, oneLine, rowDescription, textDirection, writtenDescription } from "./descriptions";

describe("descriptions", () => {
  it("show their first line, and how many more", () => {
    expect(descriptionSummary("lunch")).toEqual({ first: "lunch", more: 0 });
    expect(descriptionSummary("lunch\nwith Bob\n\nand Kim")).toEqual({ first: "lunch", more: 3 });
    expect(descriptionSummary("a\r\nb")).toEqual({ first: "a", more: 1 });
    expect(moreLinesText(1)).toBe("+1 line");
    expect(moreLinesText(2)).toBe("+2 lines");
    expect(oneLine("lunch\nwith Bob")).toBe("lunch (+1 line)");
    expect(oneLine("lunch")).toBe("lunch");
  });

  it("are the transaction's, else its parts', unless a rule overrides them", () => {
    const parts = [{ description: "for me" }, { description: null }, { description: "a gift" }];
    expect(writtenDescription({ description: "lunch", parts })).toBe("lunch");
    expect(writtenDescription({ description: null, parts })).toBe("for me · a gift");
    expect(writtenDescription({ description: "", parts }, true)).toBeNull();
    expect(writtenDescription({ description: "lunch", parts }, true)).toBe("lunch");
    expect(writtenDescription({ description: null, parts: [{ description: "" }] })).toBeNull();
    expect(writtenDescription(null)).toBeNull();
  });

  it("written, show the raw description on hover", () => {
    expect(describedTitle("lunch\nwith Bob", "SuperMart")).toBe("lunch\nwith Bob\nRaw description: SuperMart");
  });

  it("of a part's row are the part's, else the transaction's", () => {
    expect(rowDescription({ description: "part", transaction_description: "whole" })).toBe("part");
    expect(rowDescription({ description: null, transaction_description: "whole" })).toBe("whole");
    expect(rowDescription({ description: null, transaction_description: null })).toBeNull();
  });

  it("go right to left by their first letter", () => {
    expect(textDirection("סופר 123")).toBe("rtl");
    expect(textDirection("123 סופר")).toBe("rtl");
    expect(textDirection("SuperMart סופר")).toBe("ltr");
    expect(textDirection("")).toBe("ltr");
  });
});

import { describe, expect, it } from "vitest";
import { glossaryHref, renderMarkdown } from "./glossary.svelte";

describe("glossary", () => {
  it("links to entries of the Glossary page", () => {
    expect(glossaryHref("kept")).toBe("#/glossary?kept");
  });

  it("renders the entries' Markdown", () => {
    const html = renderMarkdown(`
The **sum** of \`regular\` rows,
over [kept](#kept) *months*.

- one
- two
`);
    expect(html).toBe(
      '<p>The <strong>sum</strong> of <code>regular</code> rows, over <a href="#/glossary?kept">kept</a> <em>months</em>.</p>' +
        "<ul><li>one</li><li>two</li></ul>",
    );
  });

  it("escapes HTML", () => {
    expect(renderMarkdown('a <b> & "c"')).toBe("<p>a &lt;b&gt; &amp; &quot;c&quot;</p>");
  });
});

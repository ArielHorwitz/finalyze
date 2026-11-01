import { describe, expect, it } from "vitest";
import { ago } from "./time";

describe("ago", () => {
  const now = Date.parse("2025-06-14T12:00:00Z");
  const before = (minutes: number) => new Date(now - minutes * 60_000).toISOString();

  it("rounds to minutes, hours, then days", () => {
    expect(ago(before(0.4), now)).toBe("just now");
    expect(ago(before(5), now)).toBe("5 min ago");
    expect(ago(before(180), now)).toBe("3 h ago");
    expect(ago(before(3 * 24 * 60), now)).toBe("3 days ago");
  });
});

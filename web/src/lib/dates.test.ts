import { describe, expect, it } from "vitest";
import { formatDate, formatTime, parseDate } from "./dates";

describe("formatDate", () => {
  it("shows an ISO date in each format", () => {
    expect(formatDate("2025-06-04", "dd/mm/yyyy")).toBe("04/06/2025");
    expect(formatDate("2025-06-04", "yyyy-mm-dd")).toBe("2025-06-04");
    expect(formatDate("2025-06-04", "mm/dd/yyyy")).toBe("06/04/2025");
  });

  it("takes the date of a longer ISO string, and leaves other text as it is", () => {
    expect(formatDate("2025-06-04T23:59:00Z", "dd/mm/yyyy")).toBe("04/06/2025");
    expect(formatDate("", "dd/mm/yyyy")).toBe("");
    expect(formatDate("soon", "dd/mm/yyyy")).toBe("soon");
  });

  it("shows timestamps in local time, to the minute", () => {
    const local = new Date(2025, 5, 4, 9, 5).toISOString();
    expect(formatTime(local, "dd/mm/yyyy")).toBe("04/06/2025 09:05");
    expect(formatTime(local, "yyyy-mm-dd")).toBe("2025-06-04 09:05");
    expect(formatTime(local, "mm/dd/yyyy")).toBe("06/04/2025 09:05");
  });
});

describe("parseDate", () => {
  it("reads each format", () => {
    expect(parseDate("04/06/2025", "dd/mm/yyyy")).toEqual({ date: "2025-06-04" });
    expect(parseDate("2025-06-04", "yyyy-mm-dd")).toEqual({ date: "2025-06-04" });
    expect(parseDate("06/04/2025", "mm/dd/yyyy")).toEqual({ date: "2025-06-04" });
  });

  it("accepts other separators, single digits and two-digit years", () => {
    for (const text of ["4/6/2025", "4.6.2025", "4-6-2025", "4 6 2025", " 04 / 6 / 25 "]) {
      expect(parseDate(text, "dd/mm/yyyy")).toEqual({ date: "2025-06-04" });
    }
    expect(parseDate("6.4.25", "mm/dd/yyyy")).toEqual({ date: "2025-06-04" });
    expect(parseDate("2025/6/4", "yyyy-mm-dd")).toEqual({ date: "2025-06-04" });
  });

  it("reads a year first whatever the format", () => {
    expect(parseDate("2025-06-04", "dd/mm/yyyy")).toEqual({ date: "2025-06-04" });
    expect(parseDate("2025-06-04", "mm/dd/yyyy")).toEqual({ date: "2025-06-04" });
  });

  it("says why text isn't a date", () => {
    expect(parseDate("", "dd/mm/yyyy").error).toBe("Expected a date like 14/06/2025");
    expect(parseDate("4/6", "mm/dd/yyyy").error).toBe("Expected a date like 06/14/2025");
    expect(parseDate("4/6/2025x", "dd/mm/yyyy").error).toBeDefined();
    expect(parseDate("4/6/202", "dd/mm/yyyy").error).toBeDefined();
    expect(parseDate("13/13/2025", "dd/mm/yyyy").error).toBe("No month 13");
    expect(parseDate("31/06/2025", "dd/mm/yyyy").error).toBe("June 2025 has 30 days");
    expect(parseDate("29/02/2025", "dd/mm/yyyy").error).toBe("February 2025 has 28 days");
    expect(parseDate("29/02/2024", "dd/mm/yyyy")).toEqual({ date: "2024-02-29" });
    expect(parseDate("0/1/2025", "dd/mm/yyyy").error).toBe("January 2025 has 31 days");
  });
});

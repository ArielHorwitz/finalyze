import { describe, expect, it } from "vitest";
import type { IngestFile, RawUpload } from "./api";
import { accepted, accountNameProblem, coverage, filesProblem, replaceConfirmation } from "./upload";

function ingestFile(fields: Partial<IngestFile> = {}): IngestFile {
  return {
    path: "/raw/alice/card-2025.xls",
    name: "card-2025.xls",
    size: 1000,
    modified_at: "2025-06-15T10:00:00+03:00",
    newer_than_ingest: true,
    parser: "leumi card",
    parser_errors: {},
    error: null,
    rows: 240,
    kept_rows: 240,
    transfer_rows: 0,
    start_date: "2025-01-01",
    end_date: "2025-12-31",
    total: -1000,
    sources: [{ source: "card", rows: 240, start_date: "2025-01-01", end_date: "2025-12-31", total: -1000 }],
    ...fields,
  };
}

const iso = (date: string) => date;

describe("raw uploads", () => {
  it("checks a new account's name", () => {
    expect(accountNameProblem("alice")).toBeNull();
    expect(accountNameProblem("joint account")).toBeNull();
    expect(accountNameProblem("")).toBe("Name the account");
    expect(accountNameProblem(" alice")).toBe("No spaces around the name");
    expect(accountNameProblem("..")).toBe("Not a folder name");
    expect(accountNameProblem(".hidden")).toContain("dot");
    expect(accountNameProblem("a/b")).toBe("No slashes");
    expect(accountNameProblem("a\\b")).toBe("No slashes");
  });

  it("refuses two files of the same name", () => {
    const files = [new File(["a"], "a.csv"), new File(["b"], "b.csv"), new File(["c"], "a.csv")];
    expect(filesProblem(files)).toBe("Files of the same name: a.csv");
    expect(filesProblem(files.slice(0, 2))).toBeNull();
  });

  it("tells a file's coverage", () => {
    expect(coverage(ingestFile(), iso)).toBe("2025-01-01 to 2025-12-31, 240 rows (card)");
    expect(coverage(ingestFile({ parser: null, error: "No parser could read the file" }), iso)).toBe("No parser could read the file");
    expect(coverage(ingestFile({ rows: 0, start_date: null, end_date: null, sources: [] }), iso)).toBe("no rows");
  });

  it("asks before replacing, comparing both files", () => {
    const old = ingestFile({ rows: 120, end_date: "2025-06-30" });
    const result: RawUpload = {
      dry_run: true,
      account: "alice",
      folder: "/raw/alice",
      new_account: false,
      files: [
        { name: "card-2025.xls", path: "/raw/alice/card-2025.xls", accepted: true, check: ingestFile(), replaces: old },
        { name: "bit.csv", path: "/raw/alice/bit.csv", accepted: true, check: ingestFile({ name: "bit.csv" }), replaces: null },
        { name: "notes.txt", path: "/raw/alice/notes.txt", accepted: false, check: ingestFile({ parser: null }), replaces: null },
      ],
    };
    expect(accepted(result).map((file) => file.name)).toEqual(["card-2025.xls", "bit.csv"]);
    const question = replaceConfirmation(accepted(result), "alice", iso);
    expect(question).toContain("Replace 1 file of alice?");
    expect(question).toContain("card-2025.xls\nNow: 2025-01-01 to 2025-06-30, 120 rows (card)\nNew: 2025-01-01 to 2025-12-31, 240 rows (card)");
    expect(question).not.toContain("bit.csv");
  });
});

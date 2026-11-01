import { describe, expect, it } from "vitest";
import type { Backup, Backups } from "./api";
import { backupNameProblem, backupsWarning, counted, firstOf, formatSize } from "./archive";

function backup(name: string): Backup {
  return { file: `${name}.zip`, name, created_at: "2025-06-15T10:00:00+03:00", size: 1000, accounts: ["alice"] };
}

function listing(count: number, limit = 10): Backups {
  return {
    folder: "/data/backups",
    limit,
    backups: Array.from({ length: count }, (_, index) => backup(`backup ${index}`)),
    default_names: { import: "before import on 15-06-2025", restore: "before restore on 15-06-2025" },
  };
}

describe("export, import and backups", () => {
  it("formats sizes", () => {
    expect(formatSize(512)).toBe("512 bytes");
    expect(formatSize(1234)).toBe("1.2 KB");
    expect(formatSize(5_600_000)).toBe("5.6 MB");
  });

  it("counts", () => {
    expect(counted(1, "raw file")).toBe("1 raw file");
    expect(counted(0, "raw file")).toBe("0 raw files");
  });

  it("warns ahead of the backups' limit", () => {
    expect(backupsWarning(listing(8))).toBeNull();
    expect(backupsWarning(listing(9))).toEqual({ full: false, message: expect.stringContaining("9 of 10 backups: room for one more") });
    expect(backupsWarning(listing(10))).toEqual({ full: true, message: expect.stringContaining("10 of 10 backups, the most kept") });
    expect(backupsWarning(listing(12))?.full).toBe(true);
    expect(backupsWarning(listing(0, 1))?.full).toBe(false);
  });

  it("checks new backups' names", () => {
    const existing = [backup("Monthly")];
    expect(backupNameProblem("  ", existing)).toBe("A backup needs a name.");
    expect(backupNameProblem(" monthly ", existing)).toBe("A backup named “monthly” already exists.");
    expect(backupNameProblem("x".repeat(101), existing)).toContain("At most 100 characters");
    expect(backupNameProblem("x".repeat(100), existing)).toBeNull();
    expect(backupNameProblem("before import on 15/06/2025", existing)).toBeNull();
  });

  it("shortens long lists", () => {
    expect(firstOf([1, 2, 3], 2)).toEqual({ shown: [1, 2], more: 1 });
    expect(firstOf([1], 2)).toEqual({ shown: [1], more: 0 });
  });
});

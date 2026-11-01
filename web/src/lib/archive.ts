// Export, import and backups of the whole instance (see GET /api/export, POST
// /api/import and /api/backups): the words of the Settings page.
import type { Backup, Backups } from "./api";

/** A size in bytes, e.g. "1.2 MB". */
export function formatSize(bytes: number): string {
  const units = ["bytes", "KB", "MB", "GB"];
  let size = bytes;
  let unit = 0;
  while (size >= 1000 && unit < units.length - 1) {
    size /= 1000;
    unit++;
  }
  return unit === 0 ? `${size} ${units[0]}` : `${size.toFixed(1)} ${units[unit]}`;
}

export function counted(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

/** Have the browser download a URL (an attachment), without leaving the page. */
export function download(url: string) {
  const link = document.createElement("a");
  link.href = url;
  link.download = "";
  link.click();
}

/** The longest backup name (as the server's). */
export const MAX_BACKUP_NAME = 100;

/** A warning ahead of the backups' limit: at it (new backups are refused, even
 * before an import or a restore), or one place before. */
export function backupsWarning(listing: Backups): { full: boolean; message: string } | null {
  const count = listing.backups.length;
  const limit = listing.limit;
  if (count >= limit) {
    return {
      full: true,
      message: `${count} of ${limit} backups, the most kept: new ones, including the one before an import or a restore, are refused until you delete one.`,
    };
  }
  if (count === limit - 1) {
    return { full: false, message: `${count} of ${limit} backups: room for one more, then delete one to make another.` };
  }
  return null;
}

/** Why a new backup can't have this name, if it can't: required, at most
 * `MAX_BACKUP_NAME` characters, and unique whatever the case. */
export function backupNameProblem(name: string, backups: Backup[]): string | null {
  const trimmed = name.trim();
  if (!trimmed) return "A backup needs a name.";
  if (trimmed.length > MAX_BACKUP_NAME) return `At most ${MAX_BACKUP_NAME} characters (this one has ${trimmed.length}).`;
  const key = trimmed.toLocaleLowerCase();
  if (backups.some((backup) => backup.name.trim().toLocaleLowerCase() === key)) return `A backup named “${trimmed}” already exists.`;
  return null;
}

/** The first of a list, and how many more there are, to show a long list short. */
export function firstOf<T>(items: T[], shown = 10): { shown: T[]; more: number } {
  return { shown: items.slice(0, shown), more: Math.max(0, items.length - shown) };
}

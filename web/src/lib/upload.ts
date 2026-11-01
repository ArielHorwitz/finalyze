// Uploading raw files into an account's folder (see POST /api/raw/{account}):
// what the dialog checks before sending, and the words of its results.
import type { IngestFile, RawUpload, RawUploadFile } from "./api";
import { counted } from "./archive";

/** Why a new account's name can't be a folder of the raw folder, if it can't
 * (the server checks it again). */
export function accountNameProblem(name: string): string | null {
  if (!name.trim()) return "Name the account";
  if (name !== name.trim()) return "No spaces around the name";
  if (name === "." || name === "..") return "Not a folder name";
  if (name.startsWith(".")) return "It can't start with a dot (hidden folders aren't accounts)";
  if (/[/\\]/.test(name)) return "No slashes";
  return null;
}

/** Why the files can't go together, if they can't: two of the same name. */
export function filesProblem(files: File[]): string | null {
  const names = files.map((file) => file.name);
  const twice = [...new Set(names.filter((name, index) => names.indexOf(name) !== index))];
  return twice.length ? `Files of the same name: ${twice.join(", ")}` : null;
}

/** A file's dates and rows, e.g. "1/1/2025 to 31/12/2025, 240 rows (card, checking)". */
export function coverage(file: IngestFile, showDate: (date: string) => string): string {
  if (!file.parser) return file.error ?? "no parser reads it";
  if (!file.rows || !file.start_date || !file.end_date) return "no rows";
  const sources = file.sources.map((source) => source.source).join(", ");
  return `${showDate(file.start_date)} to ${showDate(file.end_date)}, ${counted(file.rows, "row")}${sources ? ` (${sources})` : ""}`;
}

/** The files that can be uploaded: those a parser reads. */
export function accepted(result: RawUpload): RawUploadFile[] {
  return result.files.filter((file) => file.accepted);
}

/** The question before replacing files: each one's coverage, then and after. */
export function replaceConfirmation(files: RawUploadFile[], account: string, showDate: (date: string) => string): string {
  const replacing = files.filter((file) => file.replaces);
  return [
    `Replace ${counted(replacing.length, "file")} of ${account}? The files there now are lost (unless exported).`,
    ...replacing.map((file) => `${file.name}\nNow: ${coverage(file.replaces!, showDate)}\nNew: ${coverage(file.check, showDate)}`),
  ].join("\n\n");
}

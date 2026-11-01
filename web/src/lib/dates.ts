// Full dates as the web app shows and reads them, in the date format of the
// display settings (see GET /api/settings/display). The API and URLs keep ISO
// dates ("2025-06-14"): only what is shown and typed follows the format.
import type { DateFormat } from "./api";

export const DATE_FORMATS: { id: DateFormat; title: string }[] = [
  { id: "dd/mm/yyyy", title: "Day first: 14/06/2025" },
  { id: "yyyy-mm-dd", title: "ISO: 2025-06-14" },
  { id: "mm/dd/yyyy", title: "Month first: 06/14/2025" },
];

export const DEFAULT_DATE_FORMAT: DateFormat = "dd/mm/yyyy";

const MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

const pad = (value: number, width = 2) => String(value).padStart(width, "0");

/** An ISO date ("2025-06-14", or the date of a longer ISO string) in a format, e.g. "14/06/2025". */
export function formatDate(iso: string, format: DateFormat): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  if (!match) return iso;
  const [, year, month, day] = match;
  if (format === "yyyy-mm-dd") return `${year}-${month}-${day}`;
  if (format === "mm/dd/yyyy") return `${month}/${day}/${year}`;
  return `${day}/${month}/${year}`;
}

/** The ISO date of a local Date, e.g. "2025-06-14". */
export function isoOfLocal(date: Date): string {
  return `${pad(date.getFullYear(), 4)}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** A timestamp in local time, to the minute, e.g. "14/06/2025 09:05". */
export function formatTime(timestamp: string, format: DateFormat): string {
  const date = new Date(timestamp);
  return `${formatDate(isoOfLocal(date), format)} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export type ParsedDate = { date: string; error?: undefined } | { date?: undefined; error: string };

/**
 * A typed date, in a format, as an ISO date (or why it isn't a date). Lenient:
 * separators "/", "." "-" or spaces, single-digit days and months, two-digit
 * years (20yy), and a year first ("2025-6-14") whatever the format.
 */
export function parseDate(text: string, format: DateFormat): ParsedDate {
  const parts = text.trim().split(/\s*[/.\-\s]\s*/);
  if (parts.length !== 3 || parts.some((part) => !/^\d+$/.test(part))) {
    return { error: `Expected a date like ${formatDate("2025-06-14", format)}` };
  }
  let year: string, month: string, day: string;
  if (format === "yyyy-mm-dd" || parts[0].length === 4) [year, month, day] = parts;
  else if (format === "mm/dd/yyyy") [month, day, year] = parts;
  else [day, month, year] = parts;
  if (year.length === 2) year = `20${year}`;
  if (year.length !== 4 || month.length > 2 || day.length > 2) {
    return { error: `Expected a date like ${formatDate("2025-06-14", format)}` };
  }
  const [yearNumber, monthNumber, dayNumber] = [year, month, day].map(Number);
  if (monthNumber < 1 || monthNumber > 12) return { error: `No month ${monthNumber}` };
  const daysInMonth = new Date(Date.UTC(yearNumber, monthNumber, 0)).getUTCDate();
  if (dayNumber < 1 || dayNumber > daysInMonth) {
    return { error: `${MONTH_NAMES[monthNumber - 1]} ${year} has ${daysInMonth} days` };
  }
  return { date: `${year}-${pad(monthNumber)}-${pad(dayNumber)}` };
}

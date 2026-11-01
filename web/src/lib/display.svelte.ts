// The display settings in use (see GET /api/settings/display), and full dates
// shown in their format. Reactive: what shows a date updates when it changes.
import type { DisplaySettings } from "./api";
import { DEFAULT_DATE_FORMAT, formatDate, formatTime } from "./dates";

export const display = $state({ settings: { date_format: DEFAULT_DATE_FORMAT } as Required<DisplaySettings>, loaded: false });

/** An ISO date in the date format, e.g. "14/06/2025". */
export function showDate(iso: string): string {
  return formatDate(iso, display.settings.date_format);
}

/** A timestamp in local time and the date format, e.g. "14/06/2025 09:05". */
export function showTime(timestamp: string): string {
  return formatTime(timestamp, display.settings.date_format);
}

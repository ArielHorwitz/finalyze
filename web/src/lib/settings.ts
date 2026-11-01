// The Settings page's form of the config file: its settings as editable text,
// and back. The server validates the result again (see PUT /api/settings/config).
// The config holds no paths: those are the host file's (GET /api/settings/host).
import type { Config, ConfigInput, Filters, IngestionRule } from "./api";

/** The config file's settings, as the form edits them. */
export interface ConfigForm {
  startDate: string;
  endDate: string;
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;

export function formOf(config: Config): ConfigForm {
  const filters = config.ingestion?.filters ?? {};
  return {
    startDate: filters.start_date ?? "",
    endDate: filters.end_date ?? "",
  };
}

/** Ingestion filters other than the dates, which the form keeps as they are. */
export function otherFilters(config: Config): string[] {
  const filters: Filters = config.ingestion?.filters ?? {};
  return Object.entries(filters)
    .filter(([name, value]) => !["start_date", "end_date", "invert"].includes(name) && value !== null && value !== undefined)
    .map(([name]) => name)
    .concat(filters.invert ? ["invert"] : []);
}

/** A line about each ingestion rule, which the form keeps as they are. */
export function ruleSummaries(config: Config): string[] {
  return (config.ingestion?.rules ?? []).map((rule) => {
    const action =
      rule.action === "transfer"
        ? `Transfer to ${rule.to_account ? `${rule.to_account} ` : ""}${rule.to_source}`
        : rule.action === "drop"
          ? "Drop"
          : "Rewrite";
    const changes = [
      ...(rule.raw_description != null ? [`raw description "${rule.raw_description}"`] : []),
      ...(rule.shift_days ? [`date ${rule.shift_days > 0 ? "+" : ""}${rule.shift_days} days`] : []),
    ];
    return `${action}: ${filtersSummary(rule.match)}${changes.length ? ` → ${changes.join(", ")}` : ""}`;
  });
}

function filtersSummary(filters: IngestionRule["match"]): string {
  const parts = Object.entries(filters)
    .filter(([name, value]) => value !== null && value !== undefined && name !== "invert")
    .map(([name, value]) => (Array.isArray(value) ? `${name} (${value.length})` : `${name} ${value}`));
  const summary = parts.join(", ") || "every row";
  return filters.invert ? `not (${summary})` : summary;
}

/**
 * The config of a form, over the original config (for the settings the form
 * doesn't show), or the form's errors.
 */
export function configOf(form: ConfigForm, original: Config): { config: ConfigInput; errors: string[] } {
  const errors: string[] = [];
  for (const [label, date] of [["Start date", form.startDate], ["End date", form.endDate]]) {
    if (date.trim() && !DATE.test(date.trim())) errors.push(`${label} must be YYYY-MM-DD`);
  }

  const filters: Filters = {
    ...(original.ingestion?.filters ?? {}),
    start_date: form.startDate.trim() || null,
    end_date: form.endDate.trim() || null,
  };
  const config: ConfigInput = { ingestion: { rules: original.ingestion?.rules ?? [], filters } };
  return { config, errors };
}

/** Whether two forms differ. */
export function formChanged(form: ConfigForm, saved: ConfigForm): boolean {
  return JSON.stringify(form) !== JSON.stringify(saved);
}

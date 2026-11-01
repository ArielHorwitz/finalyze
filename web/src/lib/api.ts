// A small typed client for the finalyze HTTP API (see /docs when serving).
//
// The types come from `api-schema.d.ts`, generated from the API's OpenAPI
// schema by `npm run gen:api`.
import type { components } from "./api-schema";

export type Schemas = components["schemas"];
export type Transaction = Schemas["Transaction"];
export type TransactionPage = Schemas["TransactionPage"];
export type TransactionQuery = Schemas["TransactionQuery"];
export type TransactionListRequest = Schemas["TransactionListRequest"];
export type Annotation = Schemas["AnnotationOut"];
export type PartIn = Schemas["PartIn"];
export type BatchItem = Schemas["BatchItem"];
export type BatchRequest = Schemas["BatchRequest"];
export type BatchResult = Schemas["BatchResult"];
export type PatchBatchItem = Schemas["PatchBatchItem"];
export type PatchBatchRequest = Schemas["PatchBatchRequest"];
export type BulkRequest = Schemas["BulkAnnotateRequest"];
export type BulkResult = Schemas["BulkAnnotateResult"];
export type Tag = Schemas["TagOut"];
export type Subtag = Schemas["SubtagOut"];
export type TagRef = Schemas["TagRef"];
export type Account = Schemas["AccountOut"];
export type Group = Schemas["GroupOut"];
export type Rule = Schemas["RuleOut"];
/** A rule's filters (the same shape is read and written). */
export type Filters = Schemas["Filters-Input"];
export type IngestionRule = Schemas["IngestionRule-Output"];
export type RulePreview = Schemas["RulePreview"];
export type StoredAnnotation = Schemas["StoredAnnotation"];
export type Status = Schemas["Status"];
export type IngestSummary = Schemas["IngestSummary"];
export type IngestDryRun = Schemas["IngestDryRun"];
export type IngestFile = Schemas["IngestFile"];
export type IngestProblem = Schemas["IngestProblem"];
export type IngestPreview = Schemas["IngestPreview"];
export type IngestRow = Schemas["IngestRow"];
export type Dashboard = Schemas["Dashboard"];
export type DashboardOptions = Schemas["DashboardOptions"];
export type DashboardSelection = Schemas["DashboardSelection"];
export type AnalysisRow = Schemas["AnalysisRow"];
export type AnalysisRows = Schemas["AnalysisRows"];
export type ServerInfo = Schemas["ServerInfo"];
export type GlossaryEntry = Schemas["GlossaryEntry"];
export type DashboardDefaults = Schemas["DashboardDefaults"];
export type LabelColors = Schemas["LabelColors"];
/** Of a balance on a date: the net worth without an account or a group. */
export type BalanceQuery = { date?: string | null; account?: string; source?: string; group?: string };
export type Keymap = Schemas["Keymap"];
export type DisplaySettings = Schemas["DisplaySettings"];
export type DateFormat = NonNullable<DisplaySettings["date_format"]>;
export type ConfigSettings = Schemas["ConfigSettings"];
/** The config file's settings, as read. */
export type Config = Schemas["Config-Output"];
/** The config file's settings, as written. */
export type ConfigInput = Schemas["Config-Input"];
export type HostSettings = Schemas["HostSettings"];
export type Backups = Schemas["Backups"];
export type Backup = Schemas["Backup"];
export type ImportResult = Schemas["ImportResult"];
export type RawUpload = Schemas["RawUpload"];
export type RawUploadFile = Schemas["RawUploadFile"];

/** An invalid item of a batch, in a 422 response (not in the OpenAPI schema). */
export interface ItemError {
  index: number;
  id: string | null;
  message: string;
}

/** An error response: `detail` is a message, `errors` lists invalid batch items. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly detail: string,
    readonly errors: ItemError[] = [],
  ) {
    super(detail);
  }
}

/** FastAPI's own validation errors have a list as `detail`, ours a string. */
function describeDetail(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((item) => `${(item.loc ?? []).slice(1).join(".")}: ${item.msg}`)
      .join("; ");
  }
  return JSON.stringify(detail);
}

/** Send a request to the API, returning the parsed JSON (or null for 204).
 * A `FormData` body is sent as a multipart form (an upload), anything else as JSON. */
export async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  fetcher: typeof fetch = fetch,
): Promise<T> {
  const form = body instanceof FormData;
  const response = await fetcher(`/api${path}`, {
    method,
    headers: body === undefined || form ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : form ? body : JSON.stringify(body),
  });
  if (response.status === 204) return null as T;
  const text = await response.text();
  const data = text ? parsed(text) : null;
  if (!response.ok) {
    throw new ApiError(
      response.status,
      describeDetail(data?.detail ?? response.statusText),
      data?.errors ?? [],
    );
  }
  return data as T;
}

/** JSON (as `JSON.parse` returns it), or (e.g. a proxy's error page) the text as the detail. */
function parsed(text: string): any {
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

/** A multipart form of files (each under `name`) and fields. */
export function multipartOf(name: string, files: File[], fields: Record<string, boolean | string | undefined>): FormData {
  const form = new FormData();
  for (const file of files) form.append(name, file, file.name);
  for (const [field, value] of Object.entries(fields)) if (value !== undefined) form.append(field, String(value));
  return form;
}

/** Of an import or a restore: a dry run, or replacing the data, backed up first unless `backup` is false. */
export type ReplaceOptions = {
  dry_run: boolean;
  backup: boolean;
  backup_name?: string;
};

const get = <T>(path: string) => request<T>("GET", path);
const post = <T>(path: string, body: unknown) => request<T>("POST", path, body);
const patch = <T>(path: string, body: unknown) => request<T>("PATCH", path, body);
const put = <T>(path: string, body: unknown) => request<T>("PUT", path, body);

export const api = {
  status: () => get<Status>("/status"),
  server: () => get<ServerInfo>("/server"),
  glossary: () => get<GlossaryEntry[]>("/glossary"),
  ingest: () => post<IngestSummary>("/ingest", {}),
  ingestDryRun: () => get<IngestDryRun>("/ingest/dry-run"),
  previewIngest: () => get<IngestPreview>("/ingest/preview"),

  queryTransactions: (query: TransactionListRequest) =>
    post<TransactionPage>("/transactions/query", query),
  setAnnotations: (body: BatchRequest) => post<BatchResult>("/annotations/batch", body),
  /** Change only each item's given fields. */
  patchAnnotations: (body: PatchBatchRequest) => patch<BatchResult>("/annotations/batch", body),
  clearAnnotations: (ids: string[]) =>
    post<Schemas["ClearResult"]>("/annotations/clear", { ids }),
  bulk: (body: BulkRequest) => post<BulkResult>("/annotations/bulk", body),

  tags: () => get<Tag[]>("/tags"),
  createTag: (body: Schemas["TagCreate"]) => post<Tag>("/tags", body),
  updateTag: (body: Schemas["TagPropertiesUpdate"]) => patch<Tag>("/tags", body),
  renameTag: (body: Schemas["TagRename"]) => post<Tag>("/tags/rename", body),
  mergeTag: (body: Schemas["TagMerge"]) => post<Tag>("/tags/merge", body),
  deleteTag: (body: TagRef) => post<null>("/tags/delete", body),

  accounts: () => get<Account[]>("/accounts"),
  /** A balance at the end of a date (default: the latest data's). */
  balance: (query: BalanceQuery) => {
    const params = new URLSearchParams(Object.entries(query).filter((entry): entry is [string, string] => !!entry[1]));
    return get<Schemas["BalanceOnDate"]>(`/balance?${params}`);
  },
  updateAccount: (name: string, body: Schemas["AccountUpdate"]) =>
    patch<Account>(`/accounts/${encodeURIComponent(name)}`, body),
  groups: () => get<Group[]>("/groups"),
  createGroup: (body: Schemas["GroupCreate"]) => post<Group>("/groups", body),
  updateGroup: (name: string, body: Schemas["GroupUpdate"]) =>
    patch<Group>(`/groups/${encodeURIComponent(name)}`, body),
  deleteGroup: (name: string) => request<null>("DELETE", `/groups/${encodeURIComponent(name)}`),

  rules: () => get<Rule[]>("/rules"),
  createRule: (body: Schemas["RuleCreate"]) => post<Rule>("/rules", body),
  updateRule: (id: number, body: Schemas["RuleUpdate"]) => patch<Rule>(`/rules/${id}`, body),
  deleteRule: (id: number) => request<null>("DELETE", `/rules/${id}`),
  reorderRules: (ruleIds: number[]) => post<Rule[]>("/rules/reorder", { rule_ids: ruleIds }),
  previewRule: (body: Schemas["RulePreviewRequest"]) =>
    post<RulePreview>("/rules/preview", body),

  dashboard: (options: DashboardOptions) => post<Dashboard>("/dashboard", options),
  dashboardRows: (options: DashboardOptions, selection: DashboardSelection) =>
    post<AnalysisRows>("/dashboard/rows", { options, selection }),

  dashboardDefaults: () => get<DashboardDefaults>("/settings/dashboard"),
  setDashboardDefaults: (defaults: DashboardDefaults) =>
    put<DashboardDefaults>("/settings/dashboard", defaults),
  labelColors: () => get<LabelColors>("/settings/colors"),
  keymap: () => get<Keymap>("/settings/keymap"),
  setKeymap: (keymap: Keymap) => put<Keymap>("/settings/keymap", keymap),
  displaySettings: () => get<DisplaySettings>("/settings/display"),
  setDisplaySettings: (settings: DisplaySettings) => put<DisplaySettings>("/settings/display", settings),
  setLabelColor: (label: string, color: string | null) =>
    put<LabelColors>("/settings/colors", { label, color }),
  configSettings: () => get<ConfigSettings>("/settings/config"),
  setConfig: (config: ConfigInput) => put<ConfigSettings>("/settings/config", config),

  hostSettings: () => get<HostSettings>("/settings/host"),

  /** Check raw files for an account (by default), or add them to its folder. */
  uploadRaw: (account: string, files: File[], options: { dry_run: boolean; replace: boolean }) =>
    post<RawUpload>(`/raw/${encodeURIComponent(account)}`, multipartOf("files", files, options)),

  /** Where the browser downloads an export of the whole instance. */
  exportUrl: "/api/export",
  /** Check an uploaded export (by default), or replace the whole instance with it. */
  importInstance: (file: File, options: ReplaceOptions) => post<ImportResult>("/import", multipartOf("file", [file], options)),
  backups: () => get<Backups>("/backups"),
  createBackup: (name: string) => post<Backup>("/backups", { name }),
  backupUrl: (file: string) => `/api/backups/${encodeURIComponent(file)}`,
  deleteBackup: (file: string) => request<null>("DELETE", `/backups/${encodeURIComponent(file)}`),
  /** Check restoring a backup (by default), or replace the whole instance with it. */
  restoreBackup: (file: string, options: ReplaceOptions) => post<ImportResult>(`/backups/${encodeURIComponent(file)}/restore`, options),

  orphans: () => get<StoredAnnotation[]>("/orphans"),
  overridden: () => get<StoredAnnotation[]>("/overridden"),
  deleteOrphans: (ids: string[]) =>
    post<Schemas["OrphanDeleteResult"]>("/orphans/delete", { ids }),
};

/** Fetch transactions by ID, in any number (they go in the body). */
export async function transactionsByIds(ids: string[]): Promise<Transaction[]> {
  if (ids.length === 0) return [];
  const page = await api.queryTransactions({ ids, limit: null, sort: "date", descending: true, offset: 0 });
  return page.items;
}

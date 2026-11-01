// The backups (see /api/backups), shared by the Settings page's backups list and
// the dialog of an import or a restore, which backs up first.
import { api, type Backups } from "./api";
import { app, loadDisplay, loadKeymap, refreshAccounts, refreshStatus, refreshTags, toastError } from "./state.svelte";

export const backups = $state({ listing: null as Backups | null });

export async function loadBackups() {
  try {
    backups.listing = await api.backups();
  } catch (error) {
    toastError(error, "Backups");
  }
}

/** After an import or a restore: everything the app shows may have changed. */
export async function refreshAfterReplacing() {
  await Promise.all([refreshStatus(), refreshAccounts(), refreshTags(), loadKeymap(), loadDisplay(), loadBackups()]);
  app.dataVersion++;
}

/** The Settings page's backups list, which the dialogs link to. */
export const BACKUPS_SECTION_ID = "backups";

export function showBackupsList() {
  document.getElementById(BACKUPS_SECTION_ID)?.scrollIntoView({ block: "start", behavior: "smooth" });
}

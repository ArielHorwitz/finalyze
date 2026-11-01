// End-to-end tests: a real browser against `finalyze` over the demo data.
// Run with `npm run e2e` (which builds the app first).
import { defineConfig } from "@playwright/test";

// Set FINALYZE_E2E_PORT (and FINALYZE_E2E_DIR) to run beside another run
export const PORT = Number(process.env.FINALYZE_E2E_PORT ?? 8766);

export default defineConfig({
  testDir: "e2e",
  // The tests share one server and its data, so they run one at a time
  workers: 1,
  fullyParallel: false,
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    viewport: { width: 1500, height: 900 },
    // FINALYZE_E2E_BROWSER=firefox to run in Firefox (after `npx playwright install firefox`)
    browserName: (process.env.FINALYZE_E2E_BROWSER as "chromium" | "firefox" | undefined) ?? "chromium",
  },
  webServer: {
    command: "./e2e/serve-demo.sh",
    url: `http://127.0.0.1:${PORT}/api/status`,
    reuseExistingServer: false,
    env: { FINALYZE_E2E_PORT: String(PORT) },
  },
});

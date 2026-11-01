import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vitest/config";

// The address of `finalyze serve`, which the dev server forwards /api to
const API_TARGET = "http://127.0.0.1:8765";

export default defineConfig({
  plugins: [svelte()],
  build: {
    // Inside the Python package, where `finalyze serve` finds it (gitignored)
    outDir: "../finalyze/static",
    emptyOutDir: true,
    // The dashboard's chunk holds the charts library (ECharts, about 230 kB
    // gzipped), loaded only when the dashboard opens
    chunkSizeWarningLimit: 800,
  },
  server: {
    // Same-origin requests to /api in development, so the API needs no CORS
    proxy: { "/api": API_TARGET, "/agent.md": API_TARGET },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
});

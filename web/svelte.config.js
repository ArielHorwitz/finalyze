import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

export default {
  // Lets <script lang="ts"> work in .svelte files
  preprocess: vitePreprocess(),
};

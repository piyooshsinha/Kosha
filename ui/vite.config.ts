import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Served by the Kosha binary under /_kosha/ui/. In development, API calls are
// proxied to a local server; changeOrigin stays false so the Host header the
// browser signed is the one the server verifies.
export default defineConfig({
  base: "/_kosha/ui/",
  plugins: [react()],
  server: {
    proxy: {
      "^/(?!_kosha/ui|@|src|node_modules).*": { target: "http://127.0.0.1:9000", changeOrigin: false },
    },
  },
  build: { outDir: "dist", emptyOutDir: true, chunkSizeWarningLimit: 600 },
});

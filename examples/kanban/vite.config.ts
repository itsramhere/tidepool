import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Override the sync endpoint with VITE_SERVER_URL (ws://host:port/v1/sync).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
});

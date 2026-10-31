import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The e2e suite spawns the Go server; give it room.
    testTimeout: 60_000,
    hookTimeout: 60_000,
  },
});

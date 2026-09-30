import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";
import { BASE_PATH } from "./src/basePath";

// The dashboard is embedded into taskforge-api and served under /dashboard/
// (internal/api.DashboardPath). The build writes straight into the Go embed
// directory; `make dash-build` runs it inside a pinned Node container and
// copies only this output back to the host. See docs/adr/0017.
export default defineConfig({
  plugins: [react()],
  base: BASE_PATH,
  build: {
    outDir: "../internal/dashboard/dist",
    // Required explicitly because outDir is outside this project root. It also
    // deletes the committed dist/.gitkeep, which is why the supported path is
    // `make dash-build`: that builds in a container and copies only the output
    // back. Running `npm run build` on a host checkout leaves a dirty tree.
    emptyOutDir: true,
    // internal/api treats everything under assets/ as content-hashed:
    // cached forever, and a miss there is a 404 rather than a client route.
    assetsDir: "assets",
    // Anything inlined as a data: URI would be subject to the CSP's img-src;
    // keeping every asset a real file keeps the policy the whole story.
    assetsInlineLimit: 0,
    sourcemap: false,
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.{ts,tsx}"],
    restoreMocks: true,
  },
});

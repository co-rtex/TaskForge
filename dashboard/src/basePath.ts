/**
 * Where taskforge-api serves the dashboard: internal/api.DashboardPath. The
 * one definition vite.config.ts (as Vite's `base`) and the router both import,
 * so the built asset URLs and the routes cannot disagree. The Go test
 * TestAssets_BuiltOutputIsSelfConsistent checks the build against the server.
 */
export const BASE_PATH = "/dashboard/";

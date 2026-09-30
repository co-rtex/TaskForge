# The dashboard's entire Node toolchain. Node never runs on the host:
# `make dash-lint`, `make dash-test`, and `make dash-build` each build one stage
# of this file, and only `dash-build` copies anything back -- the static output,
# into internal/dashboard/dist, which go:embed compiles into taskforge-api.
# Docker is already a required prerequisite, so PROJECT_SPEC.md section 5's list
# is unchanged. See docs/adr/0017-dashboard-toolchain-and-serving.md.
#
# The build context is the repository root, because the type-drift test reads
# api/openapi.yaml. Dockerfile.dockerignore beside this file is an allowlist:
# the dashboard's source and that one document are all that enter the context.
#
# There is deliberately no "# syntax=" line. It would fetch a Dockerfile
# frontend image by floating tag on every build, an unpinned dependency beside
# the pinned one below; nothing here needs more than the builtin frontend.

# Pinned by version AND by multi-platform index digest, like every image
# compose.yaml runs and every action ci.yml uses. The digest is what makes the
# pin real: a tag can be re-pointed, a digest cannot. Bumping Node is an edit
# to this line alone.
FROM node:24.21.0-alpine@sha256:ebfe2f90462722a7a4de65e91990e97fe0d401c70e0e762c5b53302f905ec1c1 AS deps
# Plain output: these logs are read in a terminal and in CI, not a TTY.
ENV NO_COLOR=1
WORKDIR /repo/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
# npm ci installs exactly the lockfile or fails. --ignore-scripts refuses every
# dependency's install hook: nothing this build needs runs one, and a hook is
# the usual way a compromised package executes code at install time.
RUN npm ci --ignore-scripts --no-audit --no-fund

FROM deps AS source
COPY api/openapi.yaml /repo/api/openapi.yaml
COPY dashboard/ ./

# Biome's format and lint check, then the TypeScript type check.
FROM source AS lint
RUN npm run lint

FROM source AS test
RUN npm test

# vite.config.ts writes to ../internal/dashboard/dist, i.e. this path.
FROM source AS build
RUN npm run build

# Only the built files, exported with `docker build --output`.
FROM scratch AS dist
COPY --from=build /repo/internal/dashboard/dist/ /

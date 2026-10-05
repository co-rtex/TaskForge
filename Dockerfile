# The service images: one builder stage, one runtime stage, and one final target
# per service. `make images` builds all six as taskforge-<service>:dev; a single
# one is `docker build --target api .`. See
# docs/adr/0021-container-images-and-supply-chain-scanning.md for why it is
# shaped this way and what each rule below guards.
#
# Each image holds exactly one binary on a distroless static base, running as the
# base's non-root user. There is no CLI image: taskforge-cli is a client a person
# runs, not a service.
#
# The api embeds internal/dashboard/dist (go:embed). That directory is gitignored
# apart from .gitkeep, so an api image is only the real dashboard if
# `make dash-build` ran first. `make images` depends on it for that reason. Node
# is pinned in dashboard/Dockerfile and nowhere else; this file never runs it.
#
# There is deliberately no "# syntax=" line, for the reason dashboard/Dockerfile
# gives: it would fetch a Dockerfile frontend by floating tag on every build, an
# unpinned dependency beside the pinned ones below.
#
# Both base images are pinned by version AND by multi-platform index digest, each
# in exactly one place. A tag can be re-pointed and a digest cannot. The Go
# version here must equal go.mod's `go` directive (tests/verification checks it),
# so the binaries that ship are compiled by the toolchain CI tests with. Bumping
# either base is an edit to its FROM line alone.

FROM golang:1.25.14@sha256:699337d620559a59b4a2bb298ad59611e535d2ee755a34cf2d2a98f37578dc80 AS builder
WORKDIR /src
# Modules first, so a source-only change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
# Static (CGO_ENABLED=0: the final image has no libc to link against) and trimmed
# (-trimpath: build paths do not leak into the binary). The six commands are named
# rather than ./cmd/..., which would also build the CLI. -s -w drops the symbol
# table and DWARF; the module and VCS-free build info that `go version -m` and
# govulncheck read are kept.
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/ \
    ./cmd/taskforge-api ./cmd/taskforge-outbox ./cmd/taskforge-scheduler \
    ./cmd/taskforge-reconciler ./cmd/taskforge-worker ./cmd/taskforge-migrate

# The base every target shares, so it is pinned once. Its default user is
# `nonroot` (uid 65532); no target sets another.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/co-rtex/TaskForge" \
      org.opencontainers.image.revision="${REVISION}"

FROM runtime AS api
COPY --from=builder /out/taskforge-api /taskforge-api
ENTRYPOINT ["/taskforge-api"]

FROM runtime AS outbox
COPY --from=builder /out/taskforge-outbox /taskforge-outbox
ENTRYPOINT ["/taskforge-outbox"]

FROM runtime AS scheduler
COPY --from=builder /out/taskforge-scheduler /taskforge-scheduler
ENTRYPOINT ["/taskforge-scheduler"]

FROM runtime AS reconciler
COPY --from=builder /out/taskforge-reconciler /taskforge-reconciler
ENTRYPOINT ["/taskforge-reconciler"]

FROM runtime AS worker
COPY --from=builder /out/taskforge-worker /taskforge-worker
ENTRYPOINT ["/taskforge-worker"]

FROM runtime AS migrate
COPY --from=builder /out/taskforge-migrate /taskforge-migrate
ENTRYPOINT ["/taskforge-migrate"]

# The images: one builder stage, one runtime stage, and one final target per
# image. `make images` builds all seven as taskforge-<service>:dev; a single
# one is `docker build --target api .`. See
# docs/adr/0021-container-images-and-supply-chain-scanning.md for why it is
# shaped this way and what each rule below guards.
#
# Each image holds exactly one binary on a distroless static base, running as the
# base's non-root user. The seventh, cli, is taskforge-cli run as a sidecar in the
# api's ECS task: `taskforge-cli idle` keeps it running, and an operator mints the
# first keys in it through ECS Exec against the admin listener on 127.0.0.1, which
# it shares with the api (docs/adr/0027-aws-native-configuration-and-the-cli-sidecar.md).
# The api image itself still holds only taskforge-api.
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

FROM golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS builder
WORKDIR /src
# Modules first, so a source-only change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
# Static (CGO_ENABLED=0: the final image has no libc to link against) and trimmed
# (-trimpath: build paths do not leak into the binary). The seven commands are
# named rather than ./cmd/..., so a new command never enters an image by accident.
# -s -w drops the symbol
# table and DWARF; the module and VCS-free build info that `go version -m` and
# govulncheck read are kept.
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/ \
    ./cmd/taskforge-api ./cmd/taskforge-outbox ./cmd/taskforge-scheduler \
    ./cmd/taskforge-reconciler ./cmd/taskforge-worker ./cmd/taskforge-migrate \
    ./cmd/taskforge-cli

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

# The CLI sidecar. Its default command idles until SIGTERM or SIGINT, so the
# container stays up for ECS Exec; any other arguments run that CLI command.
FROM runtime AS cli
COPY --from=builder /out/taskforge-cli /taskforge-cli
ENTRYPOINT ["/taskforge-cli"]
CMD ["idle"]

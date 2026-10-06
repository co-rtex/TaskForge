# TaskForge developer commands.
#
# Every target fails loudly: no target swallows an error or prints success after
# a failed command. See AGENTS.md section 4.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GO             ?= go
COMPOSE        ?= docker compose
BIN_DIR        := bin
INTEGRATION_PKG := ./tests/integration/...

# Python SDK (sdk/python). Its gates are deliberately separate targets
# rather than folded into fmt/lint/test: those stay Go-only so a Go
# contributor -- and the fast CI job that runs them -- never needs a
# provisioned virtualenv. See AGENTS.md section 4.
SDK_DIR        := sdk/python
SDK_VENV       := $(SDK_DIR)/.venv
SDK_PY         := $(SDK_VENV)/bin/python
PYTHON         ?= python3

# Operator dashboard (dashboard/). Every target runs Node inside the pinned
# image dashboard/Dockerfile names, so the host needs Docker -- already a
# prerequisite -- and never Node. Like the SDK's, these are deliberately
# separate from fmt/lint/test, which stay Go-only. See docs/adr/0017.
DOCKER         ?= docker
DASH_DIR       := dashboard
DASH_BUILD     := $(DOCKER) build --file $(DASH_DIR)/Dockerfile
DASH_DIST      := internal/dashboard/dist
DASH_TOOLS     := taskforge-dashboard-tools

.PHONY: help bootstrap up down logs migrate fmt lint build \
        test test-unit test-integration test-race clean \
        demo demo-failure bench bench-smoke images images-smoke scan \
        sdk-venv sdk-fmt sdk-lint sdk-test \
        dash-fmt dash-lint dash-test dash-build

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Create .env from the example and download dependencies
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env from .env.example"; \
	else echo ".env already exists; leaving it untouched"; fi
	$(GO) mod download

up: ## Start local infrastructure (PostgreSQL, ElasticMQ) and wait for it
	$(COMPOSE) up -d
	./scripts/wait-for-infra.sh

down: ## Stop local infrastructure and delete its data
	$(COMPOSE) down -v

logs: ## Tail infrastructure logs
	$(COMPOSE) logs -f

migrate: ## Apply database migrations
	$(GO) run ./cmd/taskforge-migrate

fmt: ## Format all Go code
	gofmt -w .

lint: ## Check formatting and run go vet
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needs to run on:"; echo "$$unformatted"; exit 1; \
	fi
	$(GO) vet ./...

build: ## Compile all binaries into ./bin
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/ ./cmd/...

test: test-unit test-integration ## Run unit and integration tests

test-unit: ## Run tests that need no external dependencies
	$(GO) test ./...

test-integration: ## Run tests against real PostgreSQL and a real broker (needs `make up`)
	$(GO) test -tags=integration -count=1 $(INTEGRATION_PKG)

test-race: ## Run unit and integration tests under the race detector
	$(GO) test -race ./...
	$(GO) test -race -tags=integration -count=1 $(INTEGRATION_PKG)

# The demonstrations are a Go program under scripts/, not under cmd/, so `make
# build` neither compiles nor ships them. They start their own services on free
# loopback ports and stop every one of them on every way out, and they leave the
# infrastructure running: `make down` is separate and deletes its data.
demo: build up migrate ## Run the success demo: a job that succeeds, one that retries and dead-letters, one that dead-letters at once
	$(GO) run ./scripts/demo success

demo-failure: build up migrate ## Run the failure demo: a worker killed mid-job, and a frozen worker that wakes after its job moved on
	$(GO) run ./scripts/demo failure

# The benchmark harness (scripts/bench), a Go program under scripts/ like the
# demonstrations. `make bench` is the one recorded run: it refuses to start on a
# dirty tree, runs the throughput measurement and then the fault-injection one
# with the shipped default timings, and writes docs/benchmarks/<date>-<sha>.md and
# .json. It took about sixteen minutes in the two committed records (16.1 and
# 15.7, from the first warm-up to the record being written; see
# docs/CURRENT_STATE.md). `make bench-smoke` runs both in miniature, asserts the
# harness measured validly, records nothing, and is what CI runs; CI never
# records numbers. See docs/adr/0020-benchmark-methodology.md.
#
# BENCH_ARGS passes extra flags through, e.g. `make bench BENCH_ARGS="--profile
# tuned"` for the one labelled tuned run.
#
# On macOS the run is wrapped in caffeinate: a laptop that sleeps mid-run makes
# the Docker VM's clock step, and the harness aborts a run it sees that happen to.
# caffeinate cannot stop a closed lid. Keep the lid open and the machine on power.
BENCH_ARGS ?=
KEEP_AWAKE := $(shell command -v caffeinate >/dev/null 2>&1 && echo "caffeinate -dimsu")

bench: build up migrate ## Run the full benchmark and record it in docs/benchmarks (clean tree; about 16 minutes; lid open, on power)
	$(KEEP_AWAKE) $(GO) run ./scripts/bench throughput faults --record $(BENCH_ARGS)

bench-smoke: build up migrate ## Smoke-test the benchmark harness: about a minute, records nothing, fails if it measured wrongly
	$(KEEP_AWAKE) $(GO) run ./scripts/bench smoke

# The six service images (the root Dockerfile): api, outbox, scheduler,
# reconciler, worker and migrate, tagged taskforge-<service>:dev with the commit
# in the org.opencontainers.image.revision label. It depends on dash-build because
# the api embeds internal/dashboard/dist: a clean clone has only .gitkeep there,
# and an api image built without the dashboard would serve a placeholder page.
# See docs/adr/0021-container-images-and-supply-chain-scanning.md.
IMAGE_SERVICES := api outbox scheduler reconciler worker migrate
IMAGE_REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)

images: dash-build ## Build the six service images as taskforge-<service>:dev (builds the dashboard first)
	@set -e; for svc in $(IMAGE_SERVICES); do \
		echo "==> taskforge-$$svc:dev"; \
		$(DOCKER) build --target $$svc --build-arg REVISION=$(IMAGE_REVISION) --tag taskforge-$$svc:dev .; \
	done
	@$(DOCKER) image ls --format 'table {{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.Size}}' | \
		awk 'NR==1 || /^taskforge-(api|outbox|scheduler|reconciler|worker|migrate):dev/'

# Inspects and runs the six images: non-root user, labels, only the service's own
# binary, rejection of an invalid configuration, the migrate image against
# PostgreSQL, and the api serving the real dashboard. It builds the images first and
# needs the infrastructure up. See scripts/imagesmoke and ADR-0021.
images-smoke: images up ## Check the six images: non-root, labels, config rejection, migrate against PostgreSQL, the api's real dashboard
	$(GO) run ./scripts/imagesmoke

# The four supply-chain scanners, through one driver: govulncheck (reachable
# vulnerabilities in the Go code), gitleaks (secrets in the full git history),
# pip-audit (the SDK's runtime dependency tree) and npm audit (the dashboard's
# production dependencies). A govulncheck, pip-audit or npm audit finding is accepted
# only by a valid, unexpired entry in security/scan-exceptions.yaml; a gitleaks
# finding only in .gitleaks.toml (a fake fixture, or a revoked secret pinned to its
# commit). Anything else fails it. It needs Docker (gitleaks and npm audit run in
# pinned containers), Python 3 and the network.
# See docs/adr/0021-container-images-and-supply-chain-scanning.md.
scan: ## Scan for reachable Go vulnerabilities, secrets in git history, and vulnerable SDK and dashboard dependencies
	$(GO) run ./scripts/scan

sdk-venv: ## Create the Python SDK virtualenv and install it with dev extras
	$(PYTHON) -m venv $(SDK_VENV)
	$(SDK_PY) -m pip install --quiet --upgrade pip
	$(SDK_PY) -m pip install --quiet -e '$(SDK_DIR)[dev]'

sdk-fmt: ## Format the Python SDK
	$(SDK_PY) -m ruff format $(SDK_DIR)

sdk-lint: ## Check Python SDK formatting, lint, and types
	$(SDK_PY) -m ruff format --check $(SDK_DIR)
	$(SDK_PY) -m ruff check $(SDK_DIR)
	cd $(SDK_DIR) && .venv/bin/python -m mypy

sdk-test: ## Run the Python SDK test suite
	cd $(SDK_DIR) && .venv/bin/python -m pytest -q

# --no-cache-filter re-runs the named stage every time. Without it, an
# unchanged tree would print a cached layer and the checks would not actually
# have run.
dash-lint: ## Check dashboard formatting, lint, and types (in a pinned Node container)
	$(DASH_BUILD) --target lint --no-cache-filter lint --progress plain .

dash-test: ## Run the dashboard test suite (in a pinned Node container)
	$(DASH_BUILD) --target test --no-cache-filter test --progress plain .

dash-build: ## Build the dashboard into internal/dashboard/dist for go:embed
	find $(DASH_DIST) -mindepth 1 ! -name .gitkeep -delete
	$(DASH_BUILD) --target dist --output type=local,dest=$(DASH_DIST) .
	@test -f $(DASH_DIST)/index.html || { echo "dash-build produced no index.html"; exit 1; }
	@echo "dashboard built into $(DASH_DIST); rebuild taskforge-api to embed it"

dash-fmt: ## Format the dashboard source in place (in a pinned Node container)
	$(DASH_BUILD) --target source --tag $(DASH_TOOLS) --quiet .
	$(DOCKER) run --rm --user "$$(id -u):$$(id -g)" \
		--volume "$(CURDIR)/$(DASH_DIR):/work" --workdir /work \
		$(DASH_TOOLS) /repo/dashboard/node_modules/.bin/biome format --write .

clean: ## Remove build output
	rm -rf $(BIN_DIR)

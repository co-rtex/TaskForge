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

.PHONY: help bootstrap up down logs migrate fmt lint build \
        test test-unit test-integration test-race clean \
        sdk-venv sdk-fmt sdk-lint sdk-test

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

clean: ## Remove build output
	rm -rf $(BIN_DIR)

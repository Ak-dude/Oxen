##############################################################################
# OxenDB — Top-level Makefile
#
# Targets:
#   build-core      Build the Rust storage engine (debug + release dylib)
#   build-server    Build the Go HTTP server (requires build-core first)
#   build-all       Build everything
#   test-core       Run Rust unit tests
#   test-server     Run Go tests
#   test-client     Run Python SDK tests
#   test            Run all tests
#   run             Build and run the server locally
#   proto           Regenerate Go protobuf stubs (requires protoc)
#   clean           Remove build artifacts
#   help            Show this help message
##############################################################################

SHELL := /bin/bash
.PHONY: build-core build-server build-all test-core test-server test-client \
        test run proto clean help

# ---- paths ------------------------------------------------------------------
REPO_ROOT   := $(shell pwd)
CORE_DIR    := $(REPO_ROOT)/core
SERVER_DIR  := $(REPO_ROOT)/server
CLIENT_DIR  := $(REPO_ROOT)/client

RUST_LIB    := $(CORE_DIR)/target/release/liboxendb_core.dylib
RUST_LIB_SO := $(CORE_DIR)/target/release/liboxendb_core.so
SERVER_BIN  := $(SERVER_DIR)/bin/oxendb

# ---- colours ----------------------------------------------------------------
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RESET  := \033[0m

# ---- default target ---------------------------------------------------------
.DEFAULT_GOAL := help

help:
	@printf "$(GREEN)OxenDB Build System$(RESET)\n"
	@printf "Usage: make <target>\n\n"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}'

# ---- Phase 1: Rust core -----------------------------------------------------
build-core: ## Build the Rust LSM-tree storage engine (release + debug)
	@printf "$(GREEN)Building Rust core...$(RESET)\n"
	cd $(CORE_DIR) && cargo build --release
	cd $(CORE_DIR) && cargo build
	@printf "$(GREEN)Rust core built.$(RESET)\n"

build-core-debug: ## Build Rust core in debug mode only
	cd $(CORE_DIR) && cargo build

# ---- Phase 2: Go server -----------------------------------------------------
build-server: ## Build the Go HTTP server (links against Rust release dylib)
	@printf "$(GREEN)Building Go server...$(RESET)\n"
	mkdir -p $(SERVER_DIR)/bin
	cd $(SERVER_DIR) && \
		CGO_ENABLED=1 \
		go build -o $(SERVER_BIN) ./cmd/oxendb/...
	@printf "$(GREEN)Server binary: $(SERVER_BIN)$(RESET)\n"

build-server-nocgo: ## Build Go server without cgo (stub bridge; no Rust linkage)
	mkdir -p $(SERVER_DIR)/bin
	cd $(SERVER_DIR) && \
		CGO_ENABLED=0 \
		go build -o $(SERVER_BIN)-nocgo ./cmd/oxendb/...

# ---- Combined ---------------------------------------------------------------
build-all: build-core build-server ## Build Rust core then Go server

# ---- Testing ----------------------------------------------------------------
test-core: ## Run Rust unit tests
	@printf "$(GREEN)Running Rust tests...$(RESET)\n"
	cd $(CORE_DIR) && cargo test 2>&1

test-server: ## Run Go tests (no cgo required)
	@printf "$(GREEN)Running Go tests...$(RESET)\n"
	cd $(SERVER_DIR) && CGO_ENABLED=0 go test ./... -v 2>&1

test-client: ## Run Python SDK tests
	@printf "$(GREEN)Running Python tests...$(RESET)\n"
	cd $(CLIENT_DIR) && \
		pip install -e ".[dev]" --quiet && \
		python -m pytest -v 2>&1

test: test-core test-server test-client ## Run all tests

# ---- Run locally ------------------------------------------------------------
run: build-core build-server ## Build and start the OxenDB server
	@printf "$(GREEN)Starting OxenDB server...$(RESET)\n"
	OXEN_DATA_DIR=$(REPO_ROOT)/data \
	DYLD_LIBRARY_PATH=$(CORE_DIR)/target/release:$(DYLD_LIBRARY_PATH) \
	LD_LIBRARY_PATH=$(CORE_DIR)/target/release:$(LD_LIBRARY_PATH) \
	$(SERVER_BIN)

# ---- Protobuf ---------------------------------------------------------------
proto: ## Regenerate Go protobuf stubs (requires protoc + protoc-gen-go)
	@command -v protoc >/dev/null 2>&1 || { echo "protoc not found; install protobuf compiler"; exit 1; }
	mkdir -p $(SERVER_DIR)/proto/oxendbpb
	protoc \
		--proto_path=$(SERVER_DIR)/proto \
		--go_out=$(SERVER_DIR)/proto/oxendbpb \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(SERVER_DIR)/proto/oxendbpb \
		--go-grpc_opt=paths=source_relative \
		$(SERVER_DIR)/proto/oxendb.proto
	@printf "$(GREEN)Protobuf stubs regenerated.$(RESET)\n"

# ---- Clean ------------------------------------------------------------------
clean: ## Remove build artifacts
	@printf "$(YELLOW)Cleaning build artifacts...$(RESET)\n"
	cd $(CORE_DIR) && cargo clean
	rm -rf $(SERVER_DIR)/bin
	rm -rf $(CLIENT_DIR)/dist $(CLIENT_DIR)/*.egg-info
	find $(CLIENT_DIR) -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find $(CLIENT_DIR) -name "*.pyc" -delete 2>/dev/null || true
	@printf "$(GREEN)Clean complete.$(RESET)\n"

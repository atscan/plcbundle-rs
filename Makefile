.PHONY: build install release clean test run help

# Default target
help:
	@echo "bundleq Makefile"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build in debug mode
	cargo build

release: ## Build optimized release binary
	cargo build --release
	@echo ""
	@echo "Release binary: target/release/bundleq"

install: ## Install to ~/.cargo/bin
	cargo install --path .

clean: ## Clean build artifacts
	cargo clean

test: ## Run tests
	cargo test

run: ## Run with example query
	cargo run -- "did" -b "1" -j 1

fmt: ## Format code
	cargo fmt

lint: ## Run clippy linter
	cargo clippy -- -D warnings

check: ## Check without building
	cargo check

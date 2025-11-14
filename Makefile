.PHONY: build install release clean test run help version patch minor major

# Default target
help:
	@echo "bundleq Makefile"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build in debug mode
	cargo build

build-go: build
	@echo "Building Go bindings..."
	cd bindings/go && go build ./...

build-go-example: build-go
	cd bindings/go && go build -o ../../plcbundle-go cmd/example/main.go

release: ## Build optimized release binary
	cargo build --release
	@echo ""
	@echo "Release binary: target/release/bundleq"

install: ## Install to ~/.cargo/bin (with all features)
	cargo install --all-features --path .

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

# Version management with cargo-release
# Requires: cargo install cargo-release

version: ## Show current version
	@cargo metadata --format-version 1 | jq -r '.packages[] | select(.name=="plcbundle") | .version'

patch: ## Bump patch version (0.1.0 -> 0.1.1)
	cargo release patch --execute

minor: ## Bump minor version (0.1.0 -> 0.2.0)
	cargo release minor --execute

major: ## Bump major version (0.1.0 -> 1.0.0)
	cargo release major --execute

release-dry-run: ## Dry run of release (no changes)
	cargo release patch --dry-run

release-preview: ## Preview what would change in a release
	cargo release patch --no-publish --no-push --no-tag --allow-dirty

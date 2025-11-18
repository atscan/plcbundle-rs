.PHONY: build install release clean test run help version patch minor major alpha beta rc docker-build docker-run docker-compose-up docker-compose-down docker-compose-logs docker-push docker-tag

# Default target
help:
	@echo "plcbundle Makefile"
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
	@echo "Release binary: target/release/plcbundle"

install: ## Install to ~/.cargo/bin (with all features)
	cargo install --all-features --path .

clean: ## Clean build artifacts
	cargo clean

test: ## Run tests
	cargo test --all-features

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

# Pre-release version bumping with cargo-release
alpha: ## Bump alpha version (0.9.0-alpha.0 -> 0.9.0-alpha.1)
	cargo release alpha --execute

beta: ## Bump beta version (0.9.0-beta.0 -> 0.9.0-beta.1)
	cargo release beta --execute

rc: ## Bump release candidate version (0.9.0-rc.0 -> 0.9.0-rc.1)
	cargo release rc --execute

# -----------------------------------------------------------------------------
# Docker targets
# -----------------------------------------------------------------------------

IMAGE ?= atscan/plcbundle:latest
HTTP_PORT ?= 8080
DATA_DIR ?= ./data
TZ ?= UTC

docker-build: ## Build Docker image (Alpine musl) for current platform
	docker build -t $(IMAGE) .

docker-build-amd64: ## Build Docker image for linux/amd64
	docker buildx build --platform linux/amd64 -t $(IMAGE) --load .

docker-build-arm64: ## Build Docker image for linux/arm64
	docker buildx build --platform linux/arm64 -t $(IMAGE) --load .

docker-build-multi: ## Build multi-arch Docker image (amd64 + arm64)
	@echo "Building multi-architecture image for linux/amd64,linux/arm64..."
	docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE) --push .

docker-build-multi-local: ## Build multi-arch image and load both architectures locally
	@echo "Building multi-architecture image for linux/amd64,linux/arm64 (local)..."
	docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE) .

docker-run: ## Run container locally with sync and websocket
	mkdir -p $(DATA_DIR)
	docker run --rm \
		--name plcbundle \
		-p $(HTTP_PORT):8080 \
		-v $(PWD)/$(DATA_DIR):/data \
		-e TZ=$(TZ) \
		$(IMAGE) server --host 0.0.0.0 --port 8080 --sync --websocket -C /data

docker-compose-up: ## Start via docker compose (detached)
	docker compose up -d

docker-compose-down: ## Stop compose services
	docker compose down

docker-compose-logs: ## Tail compose logs
	docker compose logs -f --tail=100

docker-tag: ## Tag local image (set TAG=...)
	@if [ -z "$(TAG)" ]; then echo "TAG is required (e.g., make docker-tag TAG=v0.9.0)"; exit 1; fi
	docker tag $(IMAGE) $(IMAGE:latest=$(TAG))

docker-push: ## Push image to registry
	docker push $(IMAGE)

PROTO_DIR     := proto
AUTH_SVC       := services/auth
ASSET_SVC      := services/asset
PLAYBACK_SVC   := services/playback
JOB_SVC        := services/job
PROBE_SVC      := services/probe
TRANSCODE_SVC  := services/transcode
THUMBNAIL_SVC  := services/thumbnail
STORYBOARD_SVC := services/storyboard
SUBTITLE_SVC   := services/subtitle
WEBHOOK_SVC    := services/webhook

AUTH_PB_OUT    := $(AUTH_SVC)/gen/pb
ASSET_PB_OUT   := $(ASSET_SVC)/gen/pb
PLAYBACK_PB_OUT := $(PLAYBACK_SVC)/gen/pb
PROTO_MODULES  := auth assets playback pipeline

.PHONY: help proto build build-auth build-asset build-playback build-job build-probe build-transcode build-thumbnail build-storyboard build-subtitle build-webhook run-auth run-asset run-playback run-job run-probe run-transcode run-thumbnail run-storyboard run-subtitle run-webhook tidy scylla-up rustfs-up rustfs-shell rustfs-logs dev-start docker-up docker-down docker-logs

help: ## Show the available project commands
	@echo "Available targets:"
	@echo "  proto            - Generate Go/Connect stubs from the workspace proto modules"
	@echo "  build            - Build all workspace service binaries"
	@echo "  build-auth       - Build the auth service binary"
	@echo "  build-asset      - Build the asset service binary"
	@echo "  build-playback   - Build the playback service binary"
	@echo "  build-job        - Build the job-service binary"
	@echo "  build-probe      - Build the probe worker binary"
	@echo "  build-transcode  - Build the transcode worker binary"
	@echo "  build-thumbnail  - Build the thumbnail worker binary"
	@echo "  build-storyboard - Build the storyboard worker binary"
	@echo "  build-subtitle   - Build the subtitle service binary"
	@echo "  build-webhook    - Build the webhook service binary"
	@echo "  run-auth         - Start the auth service"
	@echo "  run-asset        - Start the asset service when cmd/main.go exists"
	@echo "  run-playback     - Start the playback service"
	@echo "  run-job          - Start the pipeline job service"
	@echo "  run-probe        - Start the probe worker"
	@echo "  run-transcode    - Start the transcode worker"
	@echo "  run-thumbnail    - Start the thumbnail worker"
	@echo "  run-storyboard   - Start the storyboard worker"
	@echo "  run-subtitle     - Start the subtitle service"
	@echo "  run-webhook      - Start the webhook service"
	@echo "  tidy             - Tidy Go modules for the workspace"
	@echo "  scylla-up        - Start the local ScyllaDB container"
	@echo "  rustfs-up        - Start the local RustFS S3 container and init the uploads bucket"
	@echo "  rustfs-shell     - Open a shell inside the RustFS container"
	@echo "  rustfs-logs      - Tail RustFS logs"
	@echo "  docker-up        - Start all local containers"
	@echo "  docker-down      - Stop all local containers"
	@echo "  docker-logs      - Tail logs for all local containers"

# ─── Code generation ───────────────────────────────────────────────────────────

proto: ## Generate Go/Connect stubs from the workspace proto modules
	buf generate --template $(PROTO_DIR)/auth/buf.gen.yaml --path $(PROTO_DIR)/auth
	buf generate --template $(PROTO_DIR)/assets/buf.gen.yaml --path $(PROTO_DIR)/assets
	buf generate --template $(PROTO_DIR)/playback/buf.gen.yaml --path $(PROTO_DIR)/playback
	buf generate --template $(PROTO_DIR)/pipeline/buf.gen.yaml --path $(PROTO_DIR)/pipeline
	@echo "✓ Proto generation complete"

# ─── Build & run ───────────────────────────────────────────────────────────────

build-auth: ## Build the auth service binary
	cd $(AUTH_SVC) && go build -o ../../bin/auth ./cmd/

build-asset: ## Build the asset service binary when its command is ready
	cd $(ASSET_SVC) && go build -o ../../bin/asset ./cmd/

build-playback: ## Build the playback service binary
	cd $(PLAYBACK_SVC) && go build -o ../../bin/playback ./cmd/

build-job: ## Build the pipeline job-service binary
	cd $(JOB_SVC) && go build -o ../../bin/job ./cmd/

build-probe: ## Build the probe worker binary
	cd $(PROBE_SVC) && go build -o ../../bin/probe ./cmd/

build-transcode: ## Build the transcode worker binary
	cd $(TRANSCODE_SVC) && go build -o ../../bin/transcode ./cmd/

build-thumbnail: ## Build the thumbnail worker binary
	cd $(THUMBNAIL_SVC) && go build -o ../../bin/thumbnail ./cmd/

build-storyboard: ## Build the storyboard worker binary
	cd $(STORYBOARD_SVC) && go build -o ../../bin/storyboard ./cmd/

build-subtitle: ## Build the subtitle service binary
	cd $(SUBTITLE_SVC) && go build -o ../../bin/subtitle ./cmd/

build-webhook: ## Build the webhook service binary
	cd $(WEBHOOK_SVC) && go build -o ../../bin/webhook ./cmd/

build: build-auth build-asset build-playback build-job build-probe build-transcode build-thumbnail build-storyboard build-subtitle build-webhook ## Build the workspace service binaries

run-auth: ## Start the auth service
	cd $(AUTH_SVC) && go run ./cmd/

run-asset: ## Start the asset service when its cmd entrypoint exists
	cd $(ASSET_SVC) && go run ./cmd/

run-playback: ## Start the playback service
	cd $(PLAYBACK_SVC) && go run ./cmd/

run-job: ## Start the job service
	cd $(JOB_SVC) && go run ./cmd/

run-probe: ## Start the probe worker
	cd $(PROBE_SVC) && go run ./cmd/

run-transcode: ## Start the transcode worker
	cd $(TRANSCODE_SVC) && go run ./cmd/

run-thumbnail: ## Start the thumbnail worker
	cd $(THUMBNAIL_SVC) && go run ./cmd/

run-storyboard: ## Start the storyboard worker
	cd $(STORYBOARD_SVC) && go run ./cmd/

run-subtitle: ## Start the subtitle service
	cd $(SUBTITLE_SVC) && go run ./cmd/

run-webhook: ## Start the webhook service
	cd $(WEBHOOK_SVC) && go run ./cmd/

tidy: ## Tidy Go modules in the workspace
	cd $(AUTH_SVC) && go mod tidy
	cd $(ASSET_SVC) && go mod tidy
	cd $(PLAYBACK_SVC) && go mod tidy
	cd $(JOB_SVC) && go mod tidy
	cd $(PROBE_SVC) && go mod tidy
	cd $(TRANSCODE_SVC) && go mod tidy
	cd $(THUMBNAIL_SVC) && go mod tidy
	cd $(STORYBOARD_SVC) && go mod tidy
	cd $(SUBTITLE_SVC) && go mod tidy
	cd $(WEBHOOK_SVC) && go mod tidy
	go work sync

# ─── Local infrastructure ───────────────────────────────────────────────────────

scylla-up: ## Start the local ScyllaDB container
	docker compose up -d scylladb
	@echo "✓ ScyllaDB started"

rustfs-up: ## Start the local RustFS S3 container and initialize the uploads bucket
	docker compose up -d rustfs rustfs-init
	@echo "✓ RustFS started"
	@echo "  S3 API: http://localhost:9000"
	@echo "  Console: http://localhost:9001"
	@echo "  Credentials: rustfsadmin / rustfsadmin"

rustfs-shell: ## Open a shell inside the RustFS container
	docker exec -it rustfs-dev /bin/sh

rustfs-logs: ## Tail RustFS logs
	docker compose logs -f rustfs rustfs-init

dev-start: scylla-up ## Start the auth service with ScyllaDB locally
	@echo "Waiting for ScyllaDB readiness..."
	@sleep 5
	$(MAKE) run-auth

# ─── Docker helpers ─────────────────────────────────────────────────────────────

docker-up: ## Start all local containers defined in docker-compose.yml
	docker compose up -d

docker-down: ## Stop and remove all local containers
	docker compose down

docker-logs: ## Tail logs for all local containers
	docker compose logs -f

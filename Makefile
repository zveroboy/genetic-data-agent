# Every command needed to run, exercise and inspect the vertical slice.
#
# The demo, from nothing:
#
#   make up            # temporal, minio, qdrant, ts-api, ts-control-worker, rust-ingestion-worker
#   make seed          # the two allowlisted source objects into s3://genomic-data/samples/
#   make demo          # ingest demo-small, wait for the manifest, ask a question
#
# `cargo` is installed through rustup and is not on a login shell's PATH on every machine; if a
# Rust target fails with "command not found", export it first:
#
#   export PATH="$$(rustup which cargo | xargs dirname):$$PATH"

.PHONY: all up down ps logs seed demo reference-snapshot temporal-dev worker api trigger \
        build build-rust build-ts test test-ts test-rust test-integration test-rust-integration \
        test-e2e download-real-data cleanup-orphans clean

API ?= http://localhost:3000
DATASET_KEY ?= demo-small

all: build test

# ---------------------------------------------------------------------------------------------
# The containerized stack
# ---------------------------------------------------------------------------------------------

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# The two allowlisted ingestion sources. Idempotent: an object is re-uploaded only when its
# stored sha256 metadata does not match the local file.
seed:
	./scripts/seed_demo_s3.sh

# One ingestion, start to answered question, against the running stack.
demo:
	@set -eu; \
	response="$$(curl -sS -X POST $(API)/api/ingestions -H 'content-type: application/json' \
	  -d '{"datasetKey":"$(DATASET_KEY)"}')"; \
	echo "$$response"; \
	dataset_id="$$(printf '%s' "$$response" | node -e 'let s="";process.stdin.on("data",c=>s+=c).on("end",()=>process.stdout.write(JSON.parse(s).datasetId))')"; \
	workflow_id="$$(printf '%s' "$$response" | node -e 'let s="";process.stdin.on("data",c=>s+=c).on("end",()=>process.stdout.write(JSON.parse(s).workflowId))')"; \
	echo "Waiting for $$workflow_id (watch it at http://localhost:8233)…"; \
	while :; do \
	  state="$$(curl -sS $(API)/api/ingestions/$$workflow_id | node -e 'let s="";process.stdin.on("data",c=>s+=c).on("end",()=>{try{process.stdout.write(JSON.parse(s).state??"")}catch{process.stdout.write("")}})')"; \
	  echo "  state=$$state"; \
	  case "$$state" in COMPLETED) break ;; FAILED) echo "ingestion failed" >&2; exit 1 ;; esac; \
	  sleep 2; \
	done; \
	echo "Asking about CYP1A2 / caffeine metabolism:"; \
	curl -sS -X POST $(API)/ask -H 'content-type: application/json' \
	  -d "{\"datasetId\":\"$$dataset_id\",\"question\":\"Can I drink coffee?\"}"; \
	echo

# Reports attempt prefixes no manifest points at. Dry run; pass ARGS="--delete" to remove them.
cleanup-orphans:
	./scripts/cleanup_orphan_attempts.sh $(ARGS)

# ---------------------------------------------------------------------------------------------
# Running the pieces directly, without containers
# ---------------------------------------------------------------------------------------------

temporal-dev:
	temporal server start-dev --db-filename temporal.sqlite

# The versioned ClinVar coordinate snapshot, from the committed TSV. The API opens this and
# refuses to start without it; the image builds it at build time.
reference-snapshot:
	node ts-api-agent/scripts/build_reference_snapshot.ts

api:
	node ts-api-agent/src/index.ts

worker:
	node ts-api-agent/src/application/worker.ts

trigger:
	node ts-api-agent/src/application/trigger_workflow.ts

ingest-pubmed:
	node ts-api-agent/scripts/ingest_pubmed.ts

download-real-data:
	./scripts/download_na12878.sh

# ---------------------------------------------------------------------------------------------
# Build and test
# ---------------------------------------------------------------------------------------------

build-rust:
	cargo build --release --manifest-path rust-ingestion-worker/Cargo.toml

build-ts:
	npm run build --workspace=ts-api-agent

build: build-rust build-ts

# Unit tests, both languages, plus typechecking tests/integration/** (tsconfig.integration.json)
# so a contract rename there fails here rather than only at runtime under Docker+Temporal+MinIO.
# No Docker, no Temporal, no MinIO.
test:
	npm test

test-ts:
	npm run test:ts

test-rust:
	npm run test:rust

# End-to-end. Needs Docker (MinIO), the `temporal` CLI and a Rust toolchain; each suite starts
# its own Temporal dev server and creates its own buckets.
test-integration: test-rust-integration
	npm run test:integration

# The MinIO object-store adapter tests (rust-ingestion-worker/tests/minio_object_store_test.rs).
# They are `#[ignore]`d in the default `cargo test` run for hermeticity, so they only run here —
# against a real MinIO this target brings up itself. Each test owns and deletes its own uniquely
# named bucket; the shared dev MinIO is never dropped.
test-rust-integration:
	docker compose up -d minio
	cargo test --manifest-path rust-ingestion-worker/Cargo.toml \
	  --test minio_object_store_test -- --ignored

# A single question against an already published dataset.
test-e2e:
	DATASET_ID=$${DATASET_ID:?set DATASET_ID to a published dataset} node ts-api-agent/src/test_e2e.ts

# Removes only build output and the derived reference snapshot — never a bucket, a container or
# a volume, and never the downloaded source genomes under data/.
clean:
	rm -rf target
	rm -rf data/reference

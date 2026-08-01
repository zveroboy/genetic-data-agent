.PHONY: all run-all run-local temporal-dev worker run-workflow test-e2e init-db download-real-data build-rust build-ts build clean docker-up docker-down

all: build test-e2e

temporal-dev:
	temporal server start-dev --db-filename temporal.sqlite

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

build-rust:
	cargo build --release --manifest-path rust-ingestion-worker/Cargo.toml

build-ts:
	npm run build --workspace=ts-api-agent

build: build-rust build-ts

init-db:
	node scripts/init_duckdb.ts

download-real-data:
	./scripts/download_na12878.sh

download-to-s3:
	./scripts/download_to_s3.sh

test-e2e:
	node ts-api-agent/src/test_e2e.ts

ingest-pubmed:
	node ts-api-agent/scripts/ingest_pubmed.ts

run-ts-api:
	node ts-api-agent/src/index.ts

worker:
	node ts-api-agent/src/application/worker.ts

run-workflow:
	node ts-api-agent/src/application/trigger_workflow.ts

run-local: init-db test-e2e
	@echo "=========================================================="
	@echo "Genomic VCF Ingestion & AI Insight Engine MVP is ACTIVE!"
	@echo "=========================================================="
	@echo "Local Temporal CLI: /opt/homebrew/bin/temporal"
	@echo "1. Start local Temporal dev server: make temporal-dev"
	@echo "2. Start Temporal worker:           make worker"
	@echo "3. Trigger Ingestion workflow:      make run-workflow"
	@echo "4. Start API server (port 3000):    make run-ts-api"
	@echo "5. Download real NA12878/ClinVar:   make download-real-data"
	@echo "=========================================================="

run-all: docker-up init-db test-e2e
	@echo "=========================================================="
	@echo "Genomic VCF Ingestion & AI Insight Engine MVP is ACTIVE!"
	@echo "=========================================================="
	@echo "Temporal Server: http://localhost:8233"
	@echo "Qdrant Vector DB: http://localhost:6333"
	@echo "MinIO Storage: http://localhost:9001 (admin/password123)"
	@echo "To start API server: make run-ts-api (port 3000)"

clean:
	rm -rf rust-ingestion-worker/target
	rm -rf ts-api-agent/dist
	rm -f *.duckdb *.duckdb.wal temporal.sqlite
	rm -rf data

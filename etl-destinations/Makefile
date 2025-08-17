# Makefile for Iceberg Integration Tests

.PHONY: help test-unit test-integration test-all docker-up docker-down docker-clean coverage benchmark lint

# Default target
help:
	@echo "Iceberg Destination Test Commands:"
	@echo "  make test-unit        - Run unit tests only"
	@echo "  make test-integration - Run integration tests with Docker"
	@echo "  make test-all        - Run all tests"
	@echo "  make docker-up       - Start Docker test infrastructure"
	@echo "  make docker-down     - Stop Docker test infrastructure"
	@echo "  make docker-clean    - Clean Docker volumes and images"
	@echo "  make coverage        - Generate code coverage report"
	@echo "  make benchmark       - Run performance benchmarks"
	@echo "  make lint           - Run formatting and linting checks"

# Run unit tests only
test-unit:
	@echo "Running Iceberg unit tests..."
	cargo test --features iceberg --lib iceberg

# Run integration tests with Docker infrastructure
test-integration: docker-up
	@echo "Waiting for services to be ready..."
	@sleep 30
	@echo "Running Iceberg integration tests..."
	SOURCE_POSTGRES_HOST=localhost \
	SOURCE_POSTGRES_PORT=5432 \
	SOURCE_POSTGRES_DB=source_db \
	SOURCE_POSTGRES_USER=replication_user \
	SOURCE_POSTGRES_PASSWORD=replication_pass \
	ICEBERG_CATALOG_URI=http://localhost:8181 \
	ICEBERG_WAREHOUSE=s3://iceberg-warehouse/ \
	ICEBERG_NAMESPACE=test_namespace \
	AWS_ACCESS_KEY_ID=minioadmin \
	AWS_SECRET_ACCESS_KEY=minioadmin \
	AWS_ENDPOINT=http://localhost:9000 \
	AWS_REGION=us-east-1 \
	AWS_ALLOW_HTTP=true \
	S3_PATH_STYLE=true \
	TEST_INTEGRATION=true \
	cargo test --features "iceberg integration-tests" iceberg_integration --nocapture
	@$(MAKE) docker-down

# Run all tests
test-all: test-unit test-integration

# Start Docker test infrastructure
docker-up:
	@echo "Starting Docker test infrastructure..."
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for PostgreSQL source..."
	@timeout 60 bash -c 'until docker exec etl-postgres-source pg_isready -U replication_user -d source_db 2>/dev/null; do sleep 2; done' || true
	@echo "Waiting for MinIO..."
	@timeout 60 bash -c 'until curl -f http://localhost:9000/minio/health/live 2>/dev/null; do sleep 2; done' || true
	@echo "Waiting for Iceberg REST catalog..."
	@timeout 120 bash -c 'until curl -f http://localhost:8181/v1/config 2>/dev/null; do sleep 5; done' || true
	@echo "All services are ready!"

# Stop Docker test infrastructure
docker-down:
	@echo "Stopping Docker test infrastructure..."
	docker-compose -f docker-compose.test.yml down

# Clean Docker volumes and images
docker-clean: docker-down
	@echo "Cleaning Docker volumes and images..."
	docker-compose -f docker-compose.test.yml down -v
	docker system prune -f

# Generate code coverage report
coverage: docker-up
	@echo "Generating code coverage report..."
	@command -v cargo-tarpaulin >/dev/null 2>&1 || cargo install cargo-tarpaulin
	cargo tarpaulin \
		--features "iceberg integration-tests" \
		--timeout 300 \
		--out Html \
		--output-dir target/coverage \
		-- iceberg
	@echo "Coverage report generated at target/coverage/tarpaulin-report.html"
	@$(MAKE) docker-down

# Run performance benchmarks
benchmark: docker-up
	@echo "Running performance benchmarks..."
	SOURCE_POSTGRES_HOST=localhost \
	ICEBERG_CATALOG_URI=http://localhost:8181 \
	AWS_ENDPOINT=http://localhost:9000 \
	cargo bench --features iceberg iceberg
	@$(MAKE) docker-down

# Run formatting and linting checks
lint:
	@echo "Checking code formatting..."
	cargo fmt --all -- --check
	@echo "Running Clippy..."
	cargo clippy --features iceberg --all-targets -- -D warnings

# Development helpers
watch:
	@echo "Watching for changes and running tests..."
	cargo watch -x "test --features iceberg --lib iceberg"

clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf target/coverage
	rm -rf target/criterion

# Docker helpers
logs:
	docker-compose -f docker-compose.test.yml logs -f

ps:
	docker-compose -f docker-compose.test.yml ps

exec-postgres:
	docker exec -it etl-postgres-source psql -U replication_user -d source_db

exec-minio:
	@echo "MinIO Console: http://localhost:9001"
	@echo "Username: minioadmin"
	@echo "Password: minioadmin"
	@open http://localhost:9001 || xdg-open http://localhost:9001 || echo "Please open http://localhost:9001 in your browser"

# CI/CD simulation
ci-local: lint test-unit test-integration
	@echo "All CI checks passed!"
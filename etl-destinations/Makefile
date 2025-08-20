# Makefile for Iceberg Destination Tests

.PHONY: help test test-unit test-integration docker-up docker-down lint

# Default target
help:
	@echo "Iceberg Test Commands:"
	@echo "  make test            - Run all tests (unit + integration if Docker is up)"
	@echo "  make test-unit       - Run unit tests only"  
	@echo "  make test-integration- Run integration tests (requires Docker)"
	@echo "  make docker-up       - Start Docker test infrastructure"
	@echo "  make docker-down     - Stop Docker test infrastructure"
	@echo "  make lint           - Run formatting and linting checks"

# Run all tests
test:
	@echo "Running Iceberg tests..."
	cargo test --features iceberg iceberg

# Run unit tests only
test-unit:
	@echo "Running Iceberg unit tests..."
	cargo test --features iceberg --lib iceberg

# Run integration tests (requires Docker infrastructure)
test-integration:
	@echo "Running Iceberg integration tests..."
	@echo "Note: Start Docker first with 'make docker-up'"
	cargo test --features iceberg iceberg_integration

# Start Docker test infrastructure
docker-up:
	@echo "Starting Docker test infrastructure..."
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for services..."
	@sleep 20
	@echo "Services should be ready. Run 'make test-integration' to test."

# Stop Docker test infrastructure  
docker-down:
	@echo "Stopping Docker test infrastructure..."
	docker-compose -f docker-compose.test.yml down

# Run formatting and linting checks
lint:
	@echo "Checking code formatting..."
	cargo fmt --all -- --check
	@echo "Running Clippy..."
	cargo clippy --features iceberg --all-targets -- -D warnings
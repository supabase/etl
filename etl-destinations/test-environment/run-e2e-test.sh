#!/bin/bash

set -e

echo "🚀 Starting End-to-End Test Environment for ETL Iceberg Destination"
echo "======================================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to test environment directory
cd "$(dirname "$0")"

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    docker-compose down -v
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}
trap cleanup EXIT

# Start services
echo -e "\n${YELLOW}Starting Docker services...${NC}"
docker-compose up -d

# Wait for services to be healthy
echo -e "\n${YELLOW}Waiting for services to be ready...${NC}"
sleep 10

# Check service health
echo -e "\n${YELLOW}Checking service health...${NC}"
docker-compose ps

# Wait for Iceberg REST catalog to be fully ready
echo -e "\n${YELLOW}Waiting for Iceberg REST catalog...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:8181/v1/config > /dev/null; then
        echo -e "${GREEN}✅ Iceberg REST catalog is ready${NC}"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

# Test PostgreSQL connection
echo -e "\n${YELLOW}Testing PostgreSQL connection...${NC}"
docker exec etl-postgres psql -U etl_user -d etl_source -c "SELECT COUNT(*) FROM users;" || {
    echo -e "${RED}❌ PostgreSQL connection failed${NC}"
    exit 1
}
echo -e "${GREEN}✅ PostgreSQL is ready with test data${NC}"

# Test MinIO connection
echo -e "\n${YELLOW}Testing MinIO connection...${NC}"
docker exec etl-minio-setup mc ls myminio/iceberg-warehouse/ || {
    echo -e "${RED}❌ MinIO connection failed${NC}"
    exit 1
}
echo -e "${GREEN}✅ MinIO is ready with iceberg-warehouse bucket${NC}"

# Test Iceberg REST catalog
echo -e "\n${YELLOW}Testing Iceberg REST catalog...${NC}"
curl -s http://localhost:8181/v1/config | jq . || {
    echo -e "${RED}❌ Iceberg REST catalog not responding${NC}"
    exit 1
}
echo -e "${GREEN}✅ Iceberg REST catalog is operational${NC}"

# Create test configuration file
echo -e "\n${YELLOW}Creating test configuration...${NC}"
cat > test-config.toml << EOF
[postgres]
host = "localhost"
port = 5432
database = "etl_source"
user = "etl_user"
password = "etl_password"

[iceberg]
catalog_uri = "http://localhost:8181"
warehouse = "s3://iceberg-warehouse/"
namespace = "etl_test"

[etl]
publication_name = "etl_publication"
batch_size = 1000
EOF
echo -e "${GREEN}✅ Test configuration created${NC}"

# Display connection information
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Environment Ready for Testing!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nConnection Information:"
echo -e "  PostgreSQL: localhost:5432 (user: etl_user, password: etl_password)"
echo -e "  Iceberg REST: http://localhost:8181"
echo -e "  MinIO Console: http://localhost:9001 (user: minioadmin, password: minioadmin)"
echo -e "  MinIO S3: http://localhost:9000"
echo -e "  Trino UI: http://localhost:8080"
echo -e "\nTo query Iceberg tables with Trino:"
echo -e "  docker exec -it etl-trino trino"
echo -e "  > SHOW CATALOGS;"
echo -e "  > USE iceberg.etl_test;"
echo -e "  > SHOW TABLES;"
echo -e "\n${YELLOW}Services will remain running. Press Ctrl+C to stop and cleanup.${NC}"

# Keep script running
read -r -d '' _ </dev/tty
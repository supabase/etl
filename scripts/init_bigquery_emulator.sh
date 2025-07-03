#!/usr/bin/env bash
set -eo pipefail

# Check for Docker
if ! [ -x "$(command -v docker)" ]; then
  echo >&2 "❌ Error: Docker is not installed."
  echo >&2 "Please install it to continue."
  exit 1
fi

# Emulator Configuration
echo "🔧 Configuring BigQuery emulator settings..."
PROJECT_ID="${BIGQUERY_PROJECT_ID:=local-project}"
HTTP_PORT="${BIGQUERY_EMULATOR_PORT_HTTP:=9050}"
GRPC_PORT="${BIGQUERY_EMULATOR_PORT_GRPC:=9060}"
CONTAINER_NAME="bigquery-emulator"

# Docker container setup
if [[ -z "${SKIP_DOCKER}" ]]; then
  echo "🐳 Checking Docker container status for '${CONTAINER_NAME}'..."
  
  # Check if the container is already running
  RUNNING_BIGQUERY_CONTAINER_ID=$(docker ps --filter "name=${CONTAINER_NAME}" --format '{{.ID}}')
  if [[ -n "${RUNNING_BIGQUERY_CONTAINER_ID}" ]]; then
    echo "✅ BigQuery emulator container is already running."
  else
    # Check if a stopped container with the same name exists and remove it
    STOPPED_BIGQUERY_CONTAINER_ID=$(docker ps -a --filter "name=${CONTAINER_NAME}" --format '{{.ID}}')
    if [[ -n "${STOPPED_BIGQUERY_CONTAINER_ID}" ]]; then
      echo "🗑️ Removing stopped container '${CONTAINER_NAME}'..."
      docker rm "${CONTAINER_NAME}"
    fi
    
    echo "🚀 Starting new BigQuery emulator container..."

    DOCKER_CMD=("docker" "run" "-d")

    if [[ "$(uname -m)" == "arm64" || "$(uname -m)" == "aarch64" ]]; then
      echo "👾 Detected ARM architecture. Using x86_64 emulation for Docker."
      DOCKER_CMD+=("--platform" "linux/x86_64")
    fi

    DOCKER_CMD+=(
      -p "${HTTP_PORT}:9050" 
      -p "${GRPC_PORT}:9060" 
      --name "${CONTAINER_NAME}" 
      ghcr.io/goccy/bigquery-emulator:latest
      --project="${PROJECT_ID}"
      --port=9050 
      --grpc-port=9060)

    "${DOCKER_CMD[@]}"

    echo "✅ BigQuery emulator container started."
  fi
fi

# Wait for the emulator to be ready
echo "⏳ Waiting for BigQuery emulator to be ready..."
until curl -s "http://localhost:${HTTP_PORT}/" > /dev/null; do
  echo "⏳ Emulator is still starting up... waiting"
  sleep 1
done

echo "✅ BigQuery emulator is up and running!"
echo "   - Project ID: ${PROJECT_ID}"
echo "   - HTTP Port:  ${HTTP_PORT}"
echo "   - gRPC Port:  ${GRPC_PORT}"

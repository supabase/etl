#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONTEXT="${K8S_CONTEXT:-orbstack}"
NAMESPACE="${K8S_NAMESPACE:-etl-data-plane}"
NAME="${REPLICATOR_NAME:-local-replicator}"
IMAGE="${REPLICATOR_IMAGE:-etl-replicator:local}"
APP_ENVIRONMENT="${APP_ENVIRONMENT:-dev}"
RUST_LOG_VALUE="${RUST_LOG:-info}"
ENABLE_TRACING_VALUE="${ENABLE_TRACING:-1}"

# Allow both generic and "mac_*" env var names.
CPU_REQUEST="${CPU_REQUEST:-${MAC_CPU_REQUEST:-250m}}"
CPU_LIMIT="${CPU_LIMIT:-${MAC_CPU_LIMIT:-1000m}}"
MEMORY_REQUEST="${MEMORY_REQUEST:-${MAC_MEMORY_REQUEST:-512Mi}}"
MEMORY_LIMIT="${MEMORY_LIMIT:-${MAC_MEMORY_LIMIT:-1024Mi}}"

SKIP_BUILD=0
SKIP_WAIT=0

usage() {
  cat <<'EOF'
usage: scripts/deploy-local-replicator-orbstack.sh [options]

builds etl-replicator image and deploys a single-replica statefulset to local orbstack kubernetes.

options:
  --context <name>            kubernetes context (default: orbstack)
  --namespace <name>          namespace (default: etl-data-plane)
  --name <name>               resource prefix (default: local-replicator)
  --image <name:tag>          docker image tag (default: etl-replicator:local)
  --app-environment <env>     app environment to run (default: dev)
  --cpu-request <value>       cpu request (default: 250m)
  --cpu-limit <value>         cpu limit (default: 1000m)
  --memory-request <value>    memory request (default: 512Mi)
  --memory-limit <value>      memory limit (default: 1024Mi)
  --skip-build                skip docker build
  --skip-wait                 skip rollout wait
  -h, --help                  show this help

notes:
  - config files are sourced from etl-replicator/configuration:
    base.yaml + dev.yaml + staging.yaml.
  - if staging.yaml is missing, dev.yaml is copied as staging.yaml.
  - config is mounted as files in /etc/etl/replicator-config.
  - no secrets are mounted separately; config values are in configmaps for local testing.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --context)
      CONTEXT="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --name)
      NAME="$2"
      shift 2
      ;;
    --image)
      IMAGE="$2"
      shift 2
      ;;
    --app-environment)
      APP_ENVIRONMENT="$2"
      shift 2
      ;;
    --cpu-request)
      CPU_REQUEST="$2"
      shift 2
      ;;
    --cpu-limit)
      CPU_LIMIT="$2"
      shift 2
      ;;
    --memory-request)
      MEMORY_REQUEST="$2"
      shift 2
      ;;
    --memory-limit)
      MEMORY_LIMIT="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --skip-wait)
      SKIP_WAIT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required." >&2
  exit 1
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync is required." >&2
  exit 1
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required." >&2
  exit 1
fi

if ! kubectl config get-contexts -o name | grep -qx "${CONTEXT}"; then
  echo "kubernetes context '${CONTEXT}' not found." >&2
  exit 1
fi

BASE_SOURCE="${REPO_ROOT}/etl-replicator/configuration/base.yaml"
DEV_SOURCE="${REPO_ROOT}/etl-replicator/configuration/dev.yaml"
STAGING_SOURCE="${REPO_ROOT}/etl-replicator/configuration/staging.yaml"

if [[ ! -f "${BASE_SOURCE}" ]]; then
  echo "missing config file: ${BASE_SOURCE}" >&2
  exit 1
fi

if [[ ! -f "${DEV_SOURCE}" ]]; then
  echo "missing config file: ${DEV_SOURCE}" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
BUILD_CONTEXT_DIR="${TMP_DIR}/build-context"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

cp "${BASE_SOURCE}" "${TMP_DIR}/base.yaml"
cp "${DEV_SOURCE}" "${TMP_DIR}/dev.yaml"
if [[ -f "${STAGING_SOURCE}" ]]; then
  cp "${STAGING_SOURCE}" "${TMP_DIR}/staging.yaml"
else
  cp "${DEV_SOURCE}" "${TMP_DIR}/staging.yaml"
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  mkdir -p "${BUILD_CONTEXT_DIR}"

  # Build from a curated workspace snapshot to avoid uploading giant local artifacts.
  BUILD_CONTEXT_PATHS=(
    "Cargo.toml"
    "Cargo.lock"
    ".cargo"
    "etl"
    "etl-api"
    "etl-benchmarks"
    "etl-config"
    "etl-destinations"
    "etl-examples"
    "etl-postgres"
    "etl-replicator"
    "etl-telemetry"
  )

  for path in "${BUILD_CONTEXT_PATHS[@]}"; do
    if [[ -e "${REPO_ROOT}/${path}" ]]; then
      rsync -a \
        --exclude '.git' \
        --exclude 'target' \
        --exclude '.DS_Store' \
        "${REPO_ROOT}/${path}" "${BUILD_CONTEXT_DIR}/"
    fi
  done

  echo "building image ${IMAGE}..."
  docker build -f "${BUILD_CONTEXT_DIR}/etl-replicator/Dockerfile" -t "${IMAGE}" "${BUILD_CONTEXT_DIR}"
fi

echo "ensuring namespace prerequisites..."
kubectl --context "${CONTEXT}" apply -f "${SCRIPT_DIR}/etl-data-plane.yaml"
kubectl --context "${CONTEXT}" apply -f "${SCRIPT_DIR}/trusted-root-certs-config.yaml"

CONFIG_MAP_NAME="${NAME}-config"
ENV_CONFIG_MAP_NAME="${NAME}-env"
HEADLESS_SERVICE_NAME="${NAME}-headless"
STATEFUL_SET_NAME="${NAME}-statefulset"

echo "applying configmaps..."
kubectl --context "${CONTEXT}" -n "${NAMESPACE}" create configmap "${CONFIG_MAP_NAME}" \
  --from-file=base.yaml="${TMP_DIR}/base.yaml" \
  --from-file=dev.yaml="${TMP_DIR}/dev.yaml" \
  --from-file=staging.yaml="${TMP_DIR}/staging.yaml" \
  --dry-run=client -o yaml | kubectl --context "${CONTEXT}" apply -f -

kubectl --context "${CONTEXT}" -n "${NAMESPACE}" create configmap "${ENV_CONFIG_MAP_NAME}" \
  --from-literal=APP_CONFIG_DIR=/etc/etl/replicator-config \
  --from-literal=APP_ENVIRONMENT="${APP_ENVIRONMENT}" \
  --from-literal=RUST_LOG="${RUST_LOG_VALUE}" \
  --from-literal=ENABLE_TRACING="${ENABLE_TRACING_VALUE}" \
  --from-literal=APP_VERSION="${IMAGE}" \
  --dry-run=client -o yaml | kubectl --context "${CONTEXT}" apply -f -

echo "applying service + statefulset..."
kubectl --context "${CONTEXT}" -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ${HEADLESS_SERVICE_NAME}
  labels:
    app.kubernetes.io/name: ${NAME}
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: ${NAME}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${STATEFUL_SET_NAME}
  labels:
    app.kubernetes.io/name: ${NAME}
spec:
  serviceName: ${HEADLESS_SERVICE_NAME}
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${NAME}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${NAME}
    spec:
      terminationGracePeriodSeconds: 30
      containers:
      - name: replicator
        image: ${IMAGE}
        imagePullPolicy: IfNotPresent
        envFrom:
        - configMapRef:
            name: ${ENV_CONFIG_MAP_NAME}
        resources:
          requests:
            cpu: "${CPU_REQUEST}"
            memory: "${MEMORY_REQUEST}"
          limits:
            cpu: "${CPU_LIMIT}"
            memory: "${MEMORY_LIMIT}"
        volumeMounts:
        - name: replicator-config
          mountPath: /etc/etl/replicator-config
          readOnly: true
      volumes:
      - name: replicator-config
        configMap:
          name: ${CONFIG_MAP_NAME}
EOF

if [[ "${SKIP_WAIT}" -eq 0 ]]; then
  echo "waiting for rollout..."
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" rollout status "statefulset/${STATEFUL_SET_NAME}" --timeout=180s
fi

echo "done."
echo "statefulset: ${STATEFUL_SET_NAME}"
echo "configmap (files): ${CONFIG_MAP_NAME}"
echo "configmap (env): ${ENV_CONFIG_MAP_NAME}"
echo "logs:"
echo "  kubectl --context ${CONTEXT} -n ${NAMESPACE} logs -f statefulset/${STATEFUL_SET_NAME}"

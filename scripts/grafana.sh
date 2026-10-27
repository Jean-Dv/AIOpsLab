#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
GRAFANA_DIR="${PROJECT_DIR}/deploy/grafana"
COMPOSE_FILE="${GRAFANA_DIR}/docker-compose.yaml"

usage() {
  echo "Usage: $0 {up|down|status|logs|validate}"
}

discover_kind_host() {
  local node_name="${AIOPSLAB_KIND_NODE:-kind-control-plane}"
  docker inspect \
    --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
    "${node_name}" 2>/dev/null || true
}

connect_kind_network() {
  local node_name="${AIOPSLAB_KIND_NODE:-kind-control-plane}"
  local network_name

  network_name="$(docker inspect \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{println}}{{end}}' \
    "${node_name}" 2>/dev/null | head -n 1 || true)"

  if [[ -n "${network_name}" ]]; then
    docker network connect "${network_name}" aiopslab-grafana 2>/dev/null || true
  fi
}

read_env_value() {
  local key="$1"
  local env_file="${GRAFANA_DIR}/.env"

  if [[ -f "${env_file}" ]]; then
    sed -n "s/^${key}=//p" "${env_file}" | tail -n 1
  fi
}

configure_backend_urls() {
  local backend_host="${AIOPSLAB_K8S_HOST:-$(read_env_value AIOPSLAB_K8S_HOST)}"
  local configured_url
  local needs_backend_host=false

  for key in \
    AIOPSLAB_APP_PROMETHEUS_URL \
    AIOPSLAB_INFRA_PROMETHEUS_URL \
    AIOPSLAB_OPENSEARCH_URL \
    AIOPSLAB_JAEGER_URL; do
    if [[ -z "${!key:-}" ]]; then
      configured_url="$(read_env_value "${key}")"
      if [[ -n "${configured_url}" ]]; then
        export "${key}=${configured_url}"
      else
        needs_backend_host=true
      fi
    fi
  done

  if [[ "${needs_backend_host}" == false ]]; then
    return
  fi

  if [[ -z "${backend_host}" ]]; then
    backend_host="$(discover_kind_host)"
  fi

  if [[ -z "${backend_host}" ]]; then
    echo "Could not discover the kind control-plane address." >&2
    echo "Set AIOPSLAB_K8S_HOST or AIOPSLAB_KIND_NODE and retry." >&2
    exit 1
  fi

  export AIOPSLAB_APP_PROMETHEUS_URL="${AIOPSLAB_APP_PROMETHEUS_URL:-http://${backend_host}:32001}"
  export AIOPSLAB_INFRA_PROMETHEUS_URL="${AIOPSLAB_INFRA_PROMETHEUS_URL:-http://${backend_host}:32000}"
  export AIOPSLAB_OPENSEARCH_URL="${AIOPSLAB_OPENSEARCH_URL:-http://${backend_host}:32200}"
  export AIOPSLAB_JAEGER_URL="${AIOPSLAB_JAEGER_URL:-http://${backend_host}:30080/jaeger/ui}"
}

compose() {
  docker compose --project-directory "${GRAFANA_DIR}" -f "${COMPOSE_FILE}" "$@"
}

compose_control() {
  env \
    GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-control-command-only}" \
    AIOPSLAB_APP_PROMETHEUS_URL="${AIOPSLAB_APP_PROMETHEUS_URL:-http://127.0.0.1}" \
    AIOPSLAB_INFRA_PROMETHEUS_URL="${AIOPSLAB_INFRA_PROMETHEUS_URL:-http://127.0.0.1}" \
    AIOPSLAB_OPENSEARCH_URL="${AIOPSLAB_OPENSEARCH_URL:-http://127.0.0.1}" \
    AIOPSLAB_JAEGER_URL="${AIOPSLAB_JAEGER_URL:-http://127.0.0.1}" \
    docker compose --project-directory "${GRAFANA_DIR}" -f "${COMPOSE_FILE}" "$@"
}

command="${1:-}"
case "${command}" in
  up)
    configure_backend_urls
    compose up -d
    connect_kind_network
    ;;
  down)
    compose_control down
    ;;
  status)
    compose_control ps
    ;;
  logs)
    compose_control logs -f grafana
    ;;
  validate)
    configure_backend_urls
    compose config --quiet
    docker exec aiopslab-grafana \
      wget -qO- http://localhost:3000/api/health
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

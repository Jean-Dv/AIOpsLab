#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
ETL_DIR="${PROJECT_DIR}/deploy/etl"
COMPOSE_FILE="${ETL_DIR}/docker-compose.yaml"
ENV_FILE="${ETL_DIR}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  ENV_FILE="${ETL_DIR}/.env.example"
fi

usage() {
  cat <<'EOF'
Usage: scripts/etl.sh {bootstrap|redeploy|up|deploy-app|status|validate|profile|logs|down}

  bootstrap   create/reuse kind and deploy the ETL, Astronomy Shop, and Grafana
  redeploy    rebuild and recreate the complete environment, retaining evidence volumes
  up          build and start only Kafka, Flink, ClickHouse, and MinIO
  deploy-app  deploy Astronomy Shop with the secondary Kafka route
  status      show containers, pods, topics, and Flink jobs
  validate    check health, topics, job, schema, and Collector connectivity
  profile     generate a JSON profile; optional second argument filters one run_id
  logs        follow ETL component logs
  down        stop the ETL while retaining its volumes
EOF
}

kind_gateway() {
  docker network inspect kind --format '{{range .IPAM.Config}}{{println .Gateway}}{{end}}' | \
    awk '!/:/ { print; exit }'
}

compose() {
  local gateway="${AIOPSLAB_KAFKA_EXTERNAL_HOST:-}"
  if [[ -z "${gateway}" ]] && docker network inspect kind >/dev/null 2>&1; then
    gateway="$(kind_gateway)"
  fi
  AIOPSLAB_KAFKA_EXTERNAL_HOST="${gateway:-127.0.0.1}" \
    docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

ensure_cluster() {
  if kind get clusters 2>/dev/null | grep -qx kind; then
    kubectl config use-context kind-kind >/dev/null
    return
  fi
  local config="kind/kind-config-x86.yaml"
  if [[ "$(uname -m)" == "aarch64" || "$(uname -m)" == "arm64" ]]; then
    config="kind/kind-config-arm.yaml"
  fi
  kind create cluster --name kind --config "${PROJECT_DIR}/${config}"
  kubectl wait --for=condition=Ready node --all --timeout=300s
}

create_kafka_route() {
  local gateway
  gateway="$(kind_gateway)"
  kubectl create namespace astronomy-shop --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: aiopslab-etl-kafka
  namespace: astronomy-shop
spec:
  ports:
    - name: kafka
      port: 29092
      targetPort: 29092
---
apiVersion: v1
kind: Endpoints
metadata:
  name: aiopslab-etl-kafka
  namespace: astronomy-shop
subsets:
  - addresses:
      - ip: ${gateway}
    ports:
      - name: kafka
        port: 29092
EOF
}

helm_container() {
  docker run --rm --network host \
    -v "${HOME}/.kube:/root/.kube:ro" \
    -v "${PROJECT_DIR}:/workspace:ro" \
    alpine/helm:4.0.4 "$@"
}

deploy_infra_prometheus() {
  local chart_dir
  chart_dir="$(mktemp -d)"
  trap 'rm -rf "${chart_dir}"' RETURN
  cp -R "${PROJECT_DIR}/aiopslab/observer/prometheus/prometheus/." "${chart_dir}/"
  # This repository vendors every declared subchart under charts/. Running
  # `helm dependency build` here would require a repository configuration in
  # the short-lived container and would make an otherwise reproducible deploy
  # depend on the external Helm index.
  docker run --rm --network host \
    -v "${HOME}/.kube:/root/.kube:ro" -v "${chart_dir}:/chart:ro" \
    alpine/helm:4.0.4 upgrade --install prometheus /chart \
      --namespace observe --create-namespace \
      --set server.persistentVolume.enabled=false \
      --set kube-state-metrics.image.registry=registry.k8s.io \
      --set kube-state-metrics.image.repository=kube-state-metrics/kube-state-metrics \
      --wait --timeout 15m
}

deploy_astronomy_shop() {
  create_kafka_route
  helm_container upgrade --install astronomy-shop opentelemetry-demo \
    --repo https://open-telemetry.github.io/opentelemetry-helm-charts \
    --version 0.42.6 --namespace astronomy-shop --create-namespace \
    --values /workspace/aiopslab/observer/grafana/astronomy-shop-values.yaml \
    --force-conflicts \
    --wait --atomic --timeout 30m
  # The chart's flagd init container uses the floating busybox image with
  # imagePullPolicy=Always. A registry delay can otherwise make an experimental
  # condition transition exceed its rollout timeout even when the image is
  # already present on the controlled Kind node.
  kubectl patch deployment/flagd --namespace astronomy-shop --type=json \
    -p='[{"op":"replace","path":"/spec/template/spec/initContainers/0/imagePullPolicy","value":"IfNotPresent"}]'
  # A single-worker controlled cluster should not require enough spare memory
  # for two complete flagd pods just to change an experimental condition. The
  # runner waits for readiness before opening the next phase, so Recreate keeps
  # phase boundaries explicit without contaminating the measured interval.
  kubectl patch deployment/flagd --namespace astronomy-shop --type=merge \
    -p='{"spec":{"strategy":{"type":"Recreate","rollingUpdate":null}}}'
  kubectl rollout status deployment/flagd --namespace astronomy-shop --timeout=600s
  # A recreated Kafka broker assigns new topic IDs. Restarting the Collector
  # clears stale broker metadata and drains only against the current topics.
  kubectl rollout restart daemonset/otel-collector-agent --namespace astronomy-shop
  kubectl rollout status daemonset/otel-collector-agent \
    --namespace astronomy-shop --timeout=300s
}

telemetry_end_offsets() {
  local topic total=0
  for topic in otlp.metrics.v1 otlp.logs.v1 otlp.traces.v1; do
    total="$((total + $(compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server kafka:9092 --topic "${topic}" --time -1 | \
      awk -F: '{sum += $3} END {print sum + 0}')))"
  done
  echo "${total}"
}

validate_live_route() {
  local before current attempt
  before="$(telemetry_end_offsets)"
  for attempt in {1..24}; do
    sleep 5
    current="$(telemetry_end_offsets)"
    if (( current > before )); then
      echo "Live OTLP route validated: Kafka offsets increased from ${before} to ${current}."
      return
    fi
  done
  echo "Validation failed: OTLP Kafka offsets did not increase within 120 seconds." >&2
  return 1
}

status() {
  compose ps
  echo
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --list || true
  echo
  compose exec -T jobmanager flink list -r || true
  echo
  kubectl get pods --namespace astronomy-shop 2>/dev/null || true
}

validate() {
  compose config --quiet
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --describe \
    --topic 'otlp.metrics.v1|otlp.logs.v1|otlp.traces.v1|experiment.control.v1'
  compose exec -T jobmanager flink list -r | \
    grep -F 'AIOpsLab multimodal telemetry ETL (RUNNING)'
  compose exec -T clickhouse sh -euc \
    "clickhouse-client --user=\"\$CLICKHOUSE_USER\" --password=\"\$CLICKHOUSE_PASSWORD\" --query \"SELECT count() FROM system.tables WHERE database = 'aiopslab' AND name IN ('gold_service_window_v2', 'feature_tabular_v1', 'feature_sequence_v1', 'evaluation_reference_v1')\"" | grep -qx '4'
  if compose exec -T clickhouse sh -euc \
    "clickhouse-client --user=\"\$CLICKHOUSE_USER\" --password=\"\$CLICKHOUSE_PASSWORD\" --query \"SELECT name FROM system.columns WHERE database = 'aiopslab' AND table = 'feature_tabular_v1' AND name IN ('condition', 'fault_type', 'affected_component')\"" | grep -q .; then
    echo "Validation failed: an evaluation label leaked into feature_tabular_v1." >&2
    return 1
  fi
  compose run --rm --no-deps --entrypoint /bin/sh minio-init -euc \
    'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls local/aiopslab-bronze-v2 >/dev/null && mc ls local/aiopslab-silver-v2 >/dev/null'
  kubectl get endpoints aiopslab-etl-kafka --namespace astronomy-shop -o wide
  local collector
  collector="$(kubectl get pods --namespace astronomy-shop \
    -l app.kubernetes.io/name=opentelemetry-collector \
    -o jsonpath='{.items[0].metadata.name}')"
  kubectl logs --namespace astronomy-shop "${collector}" --tail=100 | \
    grep -E 'kafka/(metrics|logs|traces)' >/dev/null || true
  if kubectl logs --namespace astronomy-shop "${collector}" --tail=300 | \
      grep -Eq 'call to /(stats/summary|pods) endpoint failed'; then
    echo "Validation failed: kubeletstats cannot collect container resource metrics." >&2
    return 1
  fi
  validate_live_route
  echo "ETL validated: infrastructure is healthy, topics exist, and the Flink job is active."
}

command="${1:-}"
case "${command}" in
  bootstrap)
    ensure_cluster
    compose up -d --build
    deploy_infra_prometheus
    deploy_astronomy_shop
    "${PROJECT_DIR}/scripts/grafana.sh" up
    validate
    ;;
  redeploy)
    ensure_cluster
    compose down
    compose up -d --build --force-recreate
    deploy_infra_prometheus
    deploy_astronomy_shop
    "${PROJECT_DIR}/scripts/grafana.sh" up
    validate
    ;;
  up)
    compose up -d --build
    ;;
  deploy-app)
    ensure_cluster
    deploy_astronomy_shop
    ;;
  status)
    status
    ;;
  validate)
    validate
    ;;
  profile)
    PROFILE_RUN_ID="${2:-}" compose run --rm --build profiler
    ;;
  logs)
    compose logs -f kafka jobmanager taskmanager clickhouse minio
    ;;
  down)
    compose down
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

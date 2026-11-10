# Local AIOpsLab deployment with Grafana

This guide starts the complete local environment used to observe Astronomy Shop:
a two-node Kubernetes cluster managed by kind, infrastructure Prometheus,
Astronomy Shop, OpenTelemetry Collector, application Prometheus, OpenSearch,
Jaeger, and Grafana.

Grafana runs **only as a Docker container**. It is not installed on the host.
Helm also runs from a Docker image in the commands below.

The streaming ETL is documented separately in
[the ETL operational guide](../etl/README.md) and the
[complete ETL design document](../../docs/etl_multimodal.md).

## Deployed architecture

| Component | Runtime | Purpose |
|---|---|---|
| <code>kind-control-plane</code> and <code>kind-worker</code> | Docker | Local Kubernetes cluster |
| Infrastructure Prometheus | Kubernetes, <code>observe</code> namespace | Cluster and pod metrics |
| Astronomy Shop | Kubernetes, <code>astronomy-shop</code> namespace | Instrumented distributed system |
| Application Prometheus | Kubernetes | Application metrics and trace-derived metrics |
| OpenSearch | Kubernetes | Log storage and queries |
| Jaeger | Kubernetes | Trace storage and queries |
| Grafana | Docker Compose | Query and presentation layer |

## 1. Requirements

- Linux x86-64 or ARM64. WSL2 with Docker Desktop is recommended on Windows.
- A running Docker engine.
- kind 0.27.0 or compatible.
- kubectl compatible with Kubernetes 1.32.
- At least 8 CPU cores, 16 GB of available RAM, and 25 GB of free disk space.
- Internet access during the first deployment to download images and the
  Astronomy Shop chart.

A host installation of Helm or Grafana is not required.

Check the tools:

~~~bash
docker version
docker compose version
kind version
kubectl version --client
~~~

Run all following commands from the repository root:

~~~bash
cd ~/Dev/university/project_final/AIOpsLab
~~~

## 2. Configure AIOpsLab

If <code>aiopslab/config.yml</code> does not exist, create it from the example:

~~~bash
cp aiopslab/config.yml.example aiopslab/config.yml
~~~

Relevant local values:

~~~yaml
k8s_host: kind
k8s_user: <local-user>
ssh_key_path: ~/.ssh/id_rsa
data_dir: data
qualitative_eval: false
print_session: false
~~~

This file is local and excluded from Git.

## 3. Create or reuse the kind cluster

Check whether the cluster already exists:

~~~bash
kind get clusters
~~~

If <code>kind</code> appears, do not create it again. Select its context and
inspect the nodes:

~~~bash
kubectl config use-context kind-kind
kubectl get nodes
~~~

If it does not exist, use the configuration matching the host architecture:

~~~bash
# x86-64
kind create cluster --name kind --config kind/kind-config-x86.yaml

# ARM64: use this command instead
# kind create cluster --name kind --config kind/kind-config-arm.yaml
~~~

Wait until both nodes are ready:

~~~bash
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl get nodes -o wide
~~~

## 4. Deploy infrastructure Prometheus

The AIOpsLab chart is installed with ephemeral storage. The original
<code>kube-state-metrics</code> mirror is also replaced with the official
Kubernetes registry.

~~~bash
docker run --rm --network host \
  -v "$HOME/.kube:/root/.kube:ro" \
  -v "$PWD:/workspace:ro" \
  alpine/helm:4.0.4 \
  upgrade --install prometheus \
  /workspace/aiopslab/observer/prometheus/prometheus \
  --namespace observe \
  --create-namespace \
  --set server.persistentVolume.enabled=false \
  --set kube-state-metrics.image.registry=registry.k8s.io \
  --set kube-state-metrics.image.repository=kube-state-metrics/kube-state-metrics \
  --wait \
  --timeout 15m
~~~

Inspect the namespace:

~~~bash
kubectl get pods,services --namespace observe
~~~

All pods should report <code>Running</code>. The Prometheus server should expose
NodePort <code>32000</code>.

## 5. Deploy Astronomy Shop and its telemetry sources

<code>aiopslab/observer/grafana/astronomy-shop-values.yaml</code> disables the
Grafana instance bundled with the chart, enables Jaeger, and exposes the
backends queried by the Docker-managed Grafana instance.

~~~bash
docker run --rm --network host \
  -v "$HOME/.kube:/root/.kube:ro" \
  -v "$PWD:/workspace:ro" \
  alpine/helm:4.0.4 \
  upgrade --install astronomy-shop opentelemetry-demo \
  --repo https://open-telemetry.github.io/opentelemetry-helm-charts \
  --version 0.42.6 \
  --namespace astronomy-shop \
  --create-namespace \
  --values /workspace/aiopslab/observer/grafana/astronomy-shop-values.yaml \
  --wait \
  --timeout 30m
~~~

The first deployment may take 15 to 30 minutes because OpenSearch and the
microservices download several images. The command is idempotent. If it is
interrupted or times out, inspect the pods and run it again; downloaded images
will be reused.

Inspect the result:

~~~bash
kubectl get pods --namespace astronomy-shop
kubectl get services --namespace astronomy-shop
~~~

All pods should eventually report <code>Running</code>. The load generator
remains active and continuously produces requests, metrics, logs, and traces.

## 6. Configure and start Grafana in Docker

Create the local credentials file once:

~~~bash
cp deploy/grafana/.env.example deploy/grafana/.env
~~~

Edit <code>deploy/grafana/.env</code> and assign a local password:

~~~dotenv
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<secure-local-password>
GRAFANA_PORT=3000
~~~

Do not commit this file. It is already excluded by <code>.gitignore</code>.

Start Grafana:

~~~bash
./scripts/grafana.sh up
~~~

The script discovers the <code>kind-control-plane</code> address, configures the
data sources, and connects <code>aiopslab-grafana</code> to the cluster Docker
network and the ETL network when available.

Wait briefly while Grafana installs the pinned OpenSearch and ClickHouse
plugins, then validate the container:

~~~bash
./scripts/grafana.sh status
./scripts/grafana.sh validate
~~~

A successful validation reports <code>database: ok</code>, and the container
appears as <code>healthy</code>.

## 7. Access the environment

- Grafana: <http://localhost:3000>
- Astronomy Shop dashboard:
  <http://localhost:3000/d/aiopslab-astronomy-shop>
- Multimodal ETL dashboard:
  <http://localhost:3000/d/aiopslab-etl-multimodal>

The **AIOpsLab — Astronomy Shop** dashboard includes request rate, errors,
latency, CPU, memory, recent logs, and trace access. The ETL dashboard queries
ClickHouse for Gold features and signal coverage.

On Linux, obtain the node address used to open Astronomy Shop:

~~~bash
KIND_NODE_IP=$(docker inspect \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  kind-control-plane)
echo "http://$KIND_NODE_IP:30080/"
~~~

If Docker Desktop or WSL2 cannot directly access that network, keep this command
running in another terminal and use <http://localhost:8080>:

~~~bash
kubectl port-forward \
  --namespace astronomy-shop \
  service/frontend-proxy 8080:8080
~~~

### Ports

| Port | Source | Content |
|---:|---|---|
| 3000 | Docker | Grafana UI and API |
| 30080 | Kubernetes | Astronomy Shop and Jaeger proxy |
| 32000 | Kubernetes | Infrastructure Prometheus |
| 32001 | Kubernetes | Application Prometheus |
| 32200 | Kubernetes | OpenSearch logs |

NodePorts do not include authentication and are intended only for a controlled
local environment.

## 8. Validate real telemetry

The integration test queries data sources through the Grafana API. It runs in a
temporary container and does not install dependencies on the host:

~~~bash
docker run --rm \
  --network aiopslab-observability_default \
  -v "$PWD:/workspace:ro" \
  -w /workspace \
  -e AIOPSLAB_GRAFANA_INTEGRATION=true \
  -e GRAFANA_URL=http://aiopslab-grafana:3000 \
  -e GRAFANA_ADMIN_USER=admin \
  -e GRAFANA_ADMIN_PASSWORD='<configured-password>' \
  python:3.12-slim \
  sh -c 'pip install --quiet pytest requests && pytest -p no:cacheprovider tests/integration/test_grafana_observability.py -q'
~~~

The expected result is <code>4 passed</code>. This verifies:

1. Grafana health and data-source provisioning;
2. application metric queries;
3. the existence of <code>otel-logs-*</code> indices;
4. the existence of traced services in Jaeger.

## 9. Stop and restart

### Option A: stop only Grafana

~~~bash
./scripts/grafana.sh down
~~~

The <code>aiopslab-grafana-data</code> volume is retained. Astronomy Shop and
telemetry collection continue running in Kubernetes.

Restart Grafana:

~~~bash
./scripts/grafana.sh up
./scripts/grafana.sh validate
~~~

### Option B: pause everything while retaining the environment

Stop Grafana first, then stop the kind nodes:

~~~bash
./scripts/grafana.sh down
docker stop kind-worker kind-control-plane
~~~

This releases CPU and memory without deleting the cluster or Grafana volume.

Resume the environment:

~~~bash
docker start kind-control-plane kind-worker
kubectl wait --for=condition=Ready node --all --timeout=180s
./scripts/grafana.sh up
./scripts/grafana.sh validate
~~~

Finally, inspect the workloads:

~~~bash
kubectl get pods --namespace observe
kubectl get pods --namespace astronomy-shop
~~~

### Option C: remove the complete environment

The following operation deletes the cluster and all telemetry stored in it:

~~~bash
./scripts/grafana.sh down
kind delete cluster --name kind
~~~

The Grafana volume is deliberately retained. To also delete edited dashboards,
users, and persistent Grafana configuration, run:

~~~bash
docker volume rm aiopslab-grafana-data
~~~

Volume deletion is irreversible and is not required to redeploy the cluster.

## 10. Update or repair a deployment

The two <code>helm upgrade --install</code> commands can be safely repeated
without deleting the environment. Use them after chart-value changes or when a
first deployment was interrupted.

Useful diagnostics:

~~~bash
kubectl get events --namespace astronomy-shop --sort-by=.lastTimestamp
kubectl describe pod --namespace astronomy-shop <pod>
kubectl logs --namespace astronomy-shop <pod> --all-containers
docker logs --tail 200 aiopslab-grafana
./scripts/grafana.sh status
~~~

If a data source stops responding after Docker restarts, recreate only the
Grafana container so it discovers the node address again:

~~~bash
./scripts/grafana.sh down
./scripts/grafana.sh up
./scripts/grafana.sh validate
~~~

## Persistence and scope

- The Grafana Docker volume remains persistent until explicitly deleted.
- Prometheus and OpenSearch use ephemeral storage in this profile. Telemetry
  survives stopping and restarting the kind containers, but is lost when the
  cluster is deleted and may be lost when their pods are recreated.
- The configuration prioritizes a reproducible experimental environment rather
  than a production deployment or exposure to untrusted networks.

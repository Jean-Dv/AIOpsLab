# Despliegue local de AIOpsLab con Grafana

Esta guía levanta el entorno local completo utilizado para observar Astronomy
Shop: un clúster Kubernetes de dos nodos con `kind`, Prometheus de
infraestructura, Astronomy Shop, OpenTelemetry Collector, Prometheus de
aplicación, OpenSearch, Jaeger y Grafana.

Grafana se ejecuta **exclusivamente como contenedor Docker**. No se instala en
el sistema anfitrión. Helm también se ejecuta desde una imagen Docker en los
comandos de esta guía.

## Arquitectura desplegada

| Componente | Ejecución | Función |
|---|---|---|
| `kind-control-plane` y `kind-worker` | Docker | Clúster Kubernetes local |
| Prometheus de infraestructura | Kubernetes, namespace `observe` | Métricas del clúster y los pods |
| Astronomy Shop | Kubernetes, namespace `astronomy-shop` | Sistema distribuido instrumentado |
| Prometheus de aplicación | Kubernetes | Métricas y métricas derivadas de trazas |
| OpenSearch | Kubernetes | Almacenamiento y consulta de logs |
| Jaeger | Kubernetes | Almacenamiento y consulta de trazas |
| Grafana | Docker Compose | Consulta y presentación de las tres señales |

## 1. Requisitos

- Linux x86-64 o ARM64. En Windows se recomienda WSL2 con Docker Desktop.
- Docker en ejecución.
- `kind` 0.27.0 o compatible.
- `kubectl` compatible con Kubernetes 1.32.
- Al menos 8 CPU, 16 GB de RAM disponibles y 25 GB libres en disco.
- Acceso a Internet durante el primer despliegue para descargar imágenes y el
  chart de Astronomy Shop.

No se requiere una instalación local de Helm ni Grafana.

Compruebe las herramientas:

```bash
docker version
docker compose version
kind version
kubectl version --client
```

Todos los comandos siguientes se ejecutan desde la raíz del repositorio:

```bash
cd ~/Dev/university/project_final/AIOpsLab
```

## 2. Configurar AIOpsLab

Si `aiopslab/config.yml` no existe, créelo a partir del ejemplo:

```bash
cp aiopslab/config.yml.example aiopslab/config.yml
```

Para el entorno local, los valores relevantes son:

```yaml
k8s_host: kind
k8s_user: <usuario-local>
ssh_key_path: ~/.ssh/id_rsa
data_dir: data
qualitative_eval: false
print_session: false
```

Este archivo es local y está excluido de Git.

## 3. Crear o reutilizar el clúster kind

Compruebe primero si existe el clúster:

```bash
kind get clusters
```

Si aparece `kind`, no lo vuelva a crear. Seleccione su contexto y compruebe los
nodos:

```bash
kubectl config use-context kind-kind
kubectl get nodes
```

Si no existe, créelo con la configuración correspondiente a la arquitectura:

```bash
# x86-64
kind create cluster --name kind --config kind/kind-config-x86.yaml

# ARM64, use este comando en lugar del anterior
# kind create cluster --name kind --config kind/kind-config-arm.yaml
```

Espere hasta que los dos nodos estén disponibles:

```bash
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl get nodes -o wide
```

## 4. Desplegar Prometheus de infraestructura

El chart incluido en AIOpsLab se instala con almacenamiento efímero. Además,
se reemplaza el mirror original de `kube-state-metrics` por el registro oficial
de Kubernetes.

```bash
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
```

Compruebe el namespace:

```bash
kubectl get pods,services --namespace observe
```

Todos los pods deben aparecer como `Running`; el servidor de Prometheus debe
exponer el `NodePort` `32000`.

## 5. Desplegar Astronomy Shop y sus fuentes de telemetría

El archivo
`aiopslab/observer/grafana/astronomy-shop-values.yaml` desactiva el Grafana
incluido en el chart, habilita Jaeger y expone los backends que consultará el
Grafana administrado con Docker.

```bash
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
```

El primer despliegue puede tardar entre 15 y 30 minutos porque OpenSearch y
los microservicios descargan varias imágenes. El comando es idempotente: si se
interrumpe o vence el tiempo de espera, revise los pods y ejecútelo de nuevo.
Las imágenes descargadas se reutilizarán.

Compruebe el resultado:

```bash
kubectl get pods --namespace astronomy-shop
kubectl get services --namespace astronomy-shop
```

Todos los pods deben quedar en estado `Running`. El generador de carga se
mantiene activo y produce solicitudes, métricas, logs y trazas continuamente.

## 6. Configurar e iniciar Grafana en Docker

Prepare el archivo local de credenciales una sola vez:

```bash
cp deploy/grafana/.env.example deploy/grafana/.env
```

Edite `deploy/grafana/.env` y asigne una contraseña local:

```dotenv
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<contraseña-local-segura>
GRAFANA_PORT=3000
```

No agregue este archivo al repositorio. Ya está excluido mediante `.gitignore`.

Inicie Grafana:

```bash
./scripts/grafana.sh up
```

El script descubre la IP de `kind-control-plane`, configura las cuatro fuentes
de datos y conecta el contenedor `aiopslab-grafana` a la red Docker del
clúster.

Espere unos segundos durante la primera instalación del plugin de OpenSearch y
valide el contenedor:

```bash
./scripts/grafana.sh status
./scripts/grafana.sh validate
```

Una validación correcta muestra `database: ok` y el contenedor aparece como
`healthy`.

## 7. Acceso al entorno

- Grafana: <http://localhost:3000>
- Dashboard: <http://localhost:3000/d/aiopslab-astronomy-shop>

El dashboard **AIOpsLab — Astronomy Shop** incluye tasa de solicitudes,
errores, latencia, CPU, memoria, logs recientes y acceso a trazas.

En Linux, obtenga la IP del nodo para abrir Astronomy Shop:

```bash
KIND_NODE_IP=$(docker inspect \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  kind-control-plane)
echo "http://${KIND_NODE_IP}:30080/"
```

Si Docker Desktop o WSL2 no permite acceder directamente a esa red, mantenga
el siguiente comando abierto en otra terminal y use
<http://localhost:8080>:

```bash
kubectl port-forward \
  --namespace astronomy-shop \
  service/frontend-proxy 8080:8080
```

### Puertos

| Puerto | Origen | Contenido |
|---:|---|---|
| 3000 | Docker | Interfaz y API de Grafana |
| 30080 | Kubernetes | Astronomy Shop y proxy de Jaeger |
| 32000 | Kubernetes | Prometheus de infraestructura |
| 32001 | Kubernetes | Prometheus de aplicación |
| 32200 | Kubernetes | OpenSearch para logs |

Los `NodePort` no incorporan autenticación y están destinados únicamente a un
entorno local controlado.

## 8. Validar telemetría real

La validación automatizada consulta las fuentes mediante la API de Grafana. Se
ejecuta dentro de un contenedor temporal y no instala dependencias en el host:

```bash
docker run --rm \
  --network aiopslab-observability_default \
  -v "$PWD:/workspace:ro" \
  -w /workspace \
  -e AIOPSLAB_GRAFANA_INTEGRATION=true \
  -e GRAFANA_URL=http://aiopslab-grafana:3000 \
  -e GRAFANA_ADMIN_USER=admin \
  -e GRAFANA_ADMIN_PASSWORD='<contraseña-configurada>' \
  python:3.12-slim \
  sh -c 'pip install --quiet pytest requests && pytest -p no:cacheprovider tests/integration/test_grafana_observability.py -q'
```

El resultado esperado es `4 passed`. Esto comprueba:

1. salud de Grafana y aprovisionamiento de las fuentes;
2. consulta de métricas de aplicación;
3. existencia de índices de logs `otel-logs-*`;
4. existencia de servicios trazados en Jaeger.

## 9. Apagar y volver a levantar

### Opción A: detener solamente Grafana

```bash
./scripts/grafana.sh down
```

El volumen `aiopslab-grafana-data` se conserva. Astronomy Shop y la recolección
de telemetría continúan ejecutándose en Kubernetes.

Para volver a iniciar Grafana:

```bash
./scripts/grafana.sh up
./scripts/grafana.sh validate
```

### Opción B: pausar todo conservando el entorno

Detenga primero Grafana y luego los nodos de kind:

```bash
./scripts/grafana.sh down
docker stop kind-worker kind-control-plane
```

Esto libera CPU y memoria sin eliminar el clúster ni el volumen de Grafana.

Para reanudar:

```bash
docker start kind-control-plane kind-worker
kubectl wait --for=condition=Ready node --all --timeout=180s
./scripts/grafana.sh up
./scripts/grafana.sh validate
```

Finalmente, compruebe los workloads:

```bash
kubectl get pods --namespace observe
kubectl get pods --namespace astronomy-shop
```

### Opción C: eliminar completamente el entorno

Esta operación elimina el clúster y toda la telemetría almacenada en él:

```bash
./scripts/grafana.sh down
kind delete cluster --name kind
```

El volumen de Grafana se conserva deliberadamente. Si también desea eliminar
dashboards editados, usuarios y configuración persistida, ejecute:

```bash
docker volume rm aiopslab-grafana-data
```

La eliminación del volumen es irreversible. No es necesaria para volver a
desplegar el clúster.

## 10. Actualizar o reparar un despliegue

Los dos comandos `helm upgrade --install` de esta guía pueden ejecutarse de
nuevo sin eliminar el entorno. Úselos si cambian los valores del chart o si un
primer despliegue fue interrumpido.

Comandos útiles de diagnóstico:

```bash
kubectl get events --namespace astronomy-shop --sort-by=.lastTimestamp
kubectl describe pod --namespace astronomy-shop <pod>
kubectl logs --namespace astronomy-shop <pod> --all-containers
docker logs --tail 200 aiopslab-grafana
./scripts/grafana.sh status
```

Si una fuente deja de responder después de reiniciar Docker, recree únicamente
el contenedor de Grafana para que descubra de nuevo la IP del nodo:

```bash
./scripts/grafana.sh down
./scripts/grafana.sh up
./scripts/grafana.sh validate
```

## Persistencia y alcance

- El volumen Docker de Grafana es persistente hasta que se elimine
  explícitamente.
- Los Prometheus y OpenSearch de este perfil usan almacenamiento efímero. La
  telemetría se conserva al detener y volver a iniciar los contenedores kind,
  pero se pierde al borrar el clúster y puede perderse si se recrean sus pods.
- La configuración prioriza un entorno experimental reproducible, no una
  instalación productiva ni expuesta a redes no confiables.

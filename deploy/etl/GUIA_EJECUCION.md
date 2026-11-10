# Guía para desplegar y ejecutar el ETL multimodal

Esta guía opera la versión `gold_service_window_v2.2`. Los comandos se ejecutan
desde la raíz del repositorio `AIOpsLab`. El procedimiento conserva las capas
v1 y los volúmenes históricos; no elimina evidencia existente.

## 1. Requisitos y variables locales

Se requieren Docker con Compose, `kind`, `kubectl` y Python 3.11 o posterior.
Docker debe disponer de memoria suficiente para Kubernetes, Flink, Kafka,
ClickHouse, MinIO y Grafana al mismo tiempo.
El sistema de archivos que contiene Docker debe permanecer por debajo del 90 %
de uso efectivo. El ejecutor comprueba este límite antes del lote y antes de
cada repetición; no lo omita, porque la presión de disco puede ralentizar etcd
y desestabilizar el plano de control de Kubernetes.

La primera vez, cree el archivo local de configuración:

```bash
cp deploy/etl/.env.example deploy/etl/.env
```

Revise las credenciales locales y las versiones fijadas en `.env`. No cambie
versiones entre repeticiones de un mismo protocolo.

## 2. Subir o volver a desplegar todos los entornos

Para crear o reutilizar el clúster `kind` y subir ETL, Astronomy Shop,
Prometheus y Grafana:

```bash
./scripts/etl.sh bootstrap
```

Cuando los entornos existen pero están detenidos, o después de modificar el
job, reconstruya y recree todos los servicios:

```bash
./scripts/etl.sh redeploy
```

`redeploy` ejecuta `docker compose down` sin `-v`; por tanto, conserva Kafka,
ClickHouse y MinIO. También aplica de forma idempotente las migraciones SQL,
reinstala Astronomy Shop y actualiza Grafana.

Compruebe el estado y el contrato v2:

```bash
./scripts/etl.sh status
./scripts/etl.sh validate
```

La validación exige un job Flink activo, los cuatro temas Kafka, los buckets
MinIO, la ruta desde Kubernetes y las tablas/vistas v2. También falla si
`condition`, `fault_type` o `affected_component` aparecen en
`feature_tabular_v1`.

Interfaces locales:

| Componente | Dirección |
|---|---|
| Grafana | <http://localhost:3000> |
| Flink | <http://localhost:8081> |
| ClickHouse | <http://localhost:8123> |
| MinIO | <http://localhost:9001> |

## 3. Esperar el drenaje y comprobar el host

Después de una reconstrucción, el grupo v2 puede reprocesar mensajes retenidos.
Observe su atraso:

```bash
docker exec aiopslab-etl-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe --group aiopslab-flink-telemetry-etl-v2
```

Para una run nominal, el `LAG` debe permanecer pequeño y acotado mientras
entran datos nuevos; no se exige observar exactamente cero entre checkpoints.
El ejecutor acepta por defecto hasta 1000 registros totales y rechaza un atraso
mayor. Ajuste `--max-lag` solo después de medir el régimen estacionario y deje
el valor registrado en el manifiesto. Confirme también que el job no tenga
reinicios ni checkpoints fallidos en Flink.

Compruebe además el disco y el plano de control:

```bash
df -h /
kubectl get pods -n kube-system \
  -l tier=control-plane
```

El preflight exige siete lecturas consecutivas —30 segundos entre la primera y
la última— en las que `etcd`, API server,
controller manager y scheduler estén listos y con los mismos UID y contadores
de reinicio. Cada repetición vuelve a comprobarlos y compara el estado final
con el inicial. Si un componente se reinicia durante la observación, la run se
marca `failed` aunque las cuatro fases hayan terminado.

El perfil local usa el backend RocksDB con checkpoints incrementales cada 60
segundos, una pausa mínima de 30 segundos y uno solo concurrente. El primer
checkpoint puede contener el estado completo; los siguientes reutilizan los
archivos que no cambiaron. Estos valores conservan checkpoints periódicos sin
mantener escrituras de snapshots completos de forma casi continua. No reduzca
los intervalos ni cambie el backend sin medir primero duración, tamaño, E/S y
estabilidad de Kubernetes.

## 4. Ejecutar una prueba funcional

La prueba corta verifica la secuencia completa, pero queda clasificada como
`diagnostic` y no forma parte de las tres repeticiones nominales:

```bash
python3 deploy/etl/experiment.py \
  --repetitions 1 \
  --duration-scale 0.01
```

Use `--allow-backlog` únicamente para diagnosticar el flujo mientras el
consumidor se pone al día. Esa opción impide que la ejecución sea nominal.

### 4.1 Perfilar un periodo estacionario

Antes de las repeticiones nominales, capture al menos diez minutos sin activar
una condición. El modo estacionario usa el mismo manifiesto, referencias,
perfil y sumas de comprobación que las demás ejecuciones:

```bash
python3 deploy/etl/experiment.py \
  --scenario astronomy-stationary-v1 \
  --repetitions 1 \
  --stationary-seconds 600
```

La ejecución siempre se clasifica como `diagnostic`. Revise en
`telemetry-profile.json` las secciones `metric_semantics`,
`identity_resolution_sources` y `gold_quality_by_service`. Para Gold v2.2 la
CPU es exclusivamente `container.cpu.usage`, almacenada como
`cpu_usage_cores` con unidad `{cpu}`, y la memoria es
`container.memory.working_set` en bytes. No se mezclan con tiempo acumulado de
CPU, razones respecto a límites ni métricas de proceso.
La clave canónica es `service.name + k8s.namespace.name`; `service.namespace`
se conserva como contexto descriptivo y no divide las ventanas del mismo
servicio Kubernetes.

## 5. Ejecutar el protocolo nominal

Con el atraso estable dentro del límite de la sección 3, ejecute las tres
repeticiones de 35 minutos:

```bash
python3 deploy/etl/experiment.py
```

Cada repetición contiene 5 minutos de calentamiento, 10 de referencia, 10 con
`adFailure` y 10 de recuperación. La bandera se restaura en el bloque de cierre
aunque falle la ejecución. El script espera primero 45 segundos y después exige
tres conteos Gold no vacíos e iguales, separados por diez segundos. Si el corte
no se estabiliza en tres minutos, el paquete registra el error y la repetición
no debe aceptarse como nominal.

El manifiesto también conserva `host_disk_before`, `control_plane_before` y
`control_plane_after`. Si una repetición falla, mantenga su paquete y vuelva a
ejecutar el lote completo de tres después de corregir el entorno; no mezcle una
repetición anterior con otras producidas bajo una configuración operativa
distinta.

Para cambiar duraciones sin alterar el factor global:

```bash
python3 deploy/etl/experiment.py \
  --repetitions 1 \
  --warmup 300 \
  --normal 600 \
  --fault-duration 600 \
  --recovery 600
```

Una sola repetición queda clasificada como diagnóstica. No renombre su estado
para incorporarla a la cohorte nominal.

## 6. Conservar y consultar la evidencia

Cada repetición recibe un UUID y crea:

```text
deploy/etl/reports/runs/<run_id>/
├── manifest.json
├── configuration/
├── offsets-and-checkpoints.json
├── telemetry-profile.json
├── gold-quality-report.json
├── phase-timeline.json
├── logs/
└── checksums.txt
```

El manifiesto distingue `nominal`, `diagnostic`, `complete`,
`diagnostic_complete` y `failed`. Un paquete fallido se conserva; no debe
sobrescribirse con una repetición posterior.
`offsets-and-checkpoints.json` conserva el historial de estabilización en
`after.gold_stability`; el perfil y el informe de calidad se generan únicamente
después de alcanzar ese corte.

Liste las ejecuciones registradas:

```bash
docker exec aiopslab-etl-clickhouse clickhouse-client \
  --user aiopslab --password aiopslab-local \
  --query "SELECT run_id, repetition, status, collection_start, collection_end FROM aiopslab.experiment_runs FINAL ORDER BY collection_start DESC"
```

Genere de nuevo el perfil de una ejecución concreta:

```bash
./scripts/etl.sh profile <run_id>
```

Valide los conteos y la cobertura Gold:

```bash
docker exec aiopslab-etl-clickhouse clickhouse-client \
  --user aiopslab --password aiopslab-local \
  --query "SELECT * FROM aiopslab.gold_quality_by_run_v2 WHERE run_id = '<run_id>' FORMAT Vertical"
```

La cohorte primaria de esta iteración es `k8s_namespace = 'astronomy-shop'`.
Compruebe la cobertura por servicio dentro de ella; CPU y memoria deben estar
presentes al menos en el 90 % de las ventanas que contienen métricas:

```bash
docker exec aiopslab-etl-clickhouse clickhouse-client \
  --user aiopslab --password aiopslab-local \
  --query "SELECT service_name, count() AS windows, countIf(metric_point_count > 0) AS metric_windows, countIf(cpu_usage_cores IS NOT NULL) / metric_windows AS cpu_coverage, countIf(memory_working_set_bytes IS NOT NULL) / metric_windows AS memory_coverage FROM aiopslab.gold_service_window_v2_latest WHERE run_id = '<run_id>' AND k8s_namespace = 'astronomy-shop' GROUP BY service_name ORDER BY service_name"
```

Informe por separado las filas cuyo `k8s_namespace` esté vacío; no las fusione
con la cohorte primaria únicamente por coincidencia de `service_name`.

Verifique la integridad del paquete:

```bash
(cd "deploy/etl/reports/runs/<run_id>" && sha256sum -c checksums.txt)
```

Archive el paquete fuera del árbol de trabajo antes de una limpieza deliberada:

```bash
tar -C deploy/etl/reports/runs \
  -czf "run-<run_id>.tar.gz" "<run_id>"
sha256sum "run-<run_id>.tar.gz"
```

## 7. Contratos disponibles

- `gold_service_window_v2_latest`: Gold observado por servicio y ventana.
- `gold_service_window_grid_v2`: rejilla regular; `row_present=0` identifica
  pasos generados sin observación.
- `feature_tabular_v1`: entrada tabular sin etiquetas experimentales.
- `feature_sequence_v1`: formato largo de secuencias de doce pasos.
- `evaluation_reference_v1`: condiciones conocidas, separadas de las entradas.
- `pattern_results_v1`: salida común para métodos y explicaciones.

Las vistas de características conservan `run_id`, servicio y tiempo para crear
particiones por ejecución. No divida ventanas aleatoriamente ni ajuste
imputación o escalado con datos de validación o prueba.

Los eventos sin `service.name` pueden usar como respaldo un controlador
Kubernetes inequívoco —deployment, StatefulSet, DaemonSet, CronJob o Job—. Un
pod o contenedor aislado no se interpreta como servicio. Los eventos que siguen
sin identidad se conservan en Silver para auditoría, pero no entran en Gold.

## 8. Detener y reanudar

Detenga ETL y Grafana sin borrar datos:

```bash
./scripts/etl.sh down
./scripts/grafana.sh down
```

Para reanudar, use `./scripts/etl.sh bootstrap`; si cambió código o esquemas,
use `./scripts/etl.sh redeploy`.

No ejecute `docker compose down -v` ni elimine los volúmenes
`aiopslab-etl-*` mientras exista evidencia sin archivar.

## 9. Diagnóstico rápido

```bash
./scripts/etl.sh logs
kubectl get pods -n astronomy-shop
docker logs aiopslab-etl-taskmanager --tail 200
```

Si aparece un timeout en `kubectl rollout status deployment/flagd`, no asuma
que el problema se limita a `flagd`. Compruebe la variante efectiva, el rollout
y el plano de control:

```bash
kubectl get configmap flagd-config -n astronomy-shop \
  -o jsonpath='{.data.demo\.flagd\.json}' | \
  jq -r '.flags.adFailure.defaultVariant'
kubectl rollout status deployment/flagd \
  -n astronomy-shop --timeout=600s
kubectl get pods -n kube-system -l tier=control-plane \
  -o custom-columns='NAME:.metadata.labels.component,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount'
df -h /
```

No inicie otra run si la bandera no está en `off`, algún componente no está
listo, aumentan los reinicios o el disco alcanza el 90 % efectivo. Libere solo
cachés o artefactos regenerables; no elimine volúmenes ni paquetes de evidencia.
Después espere a que el lag quede dentro del umbral y ejecute de nuevo
`./scripts/etl.sh validate`.

El runner usa una revisión única de la plantilla de `flagd`, espera hasta 600
segundos y verifica la variante observada. Si falla la ejecución y también la
restauración, conserva ambos errores por separado; el segundo no oculta al
primero. La run fallida permanece inválida aunque la bandera se restaure luego.

Conserve el paquete fallido y genere un `run_id` nuevo después de corregir la
causa; nunca reutilice el UUID anterior ni combine repeticiones producidas con
configuraciones operativas distintas.

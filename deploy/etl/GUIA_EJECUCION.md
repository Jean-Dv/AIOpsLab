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

El preflight se repite antes de cada repetición y exige lecturas consecutivas
estables en las que `etcd`, API server,
controller manager y scheduler estén listos y con los mismos UID y contadores
de reinicio. También vuelve a validar el ETL y rechaza un lag de Kafka no
medible o superior al umbral. Cada repetición compara el estado final del plano
de control con el inicial y confirma que la bandera conserva la variante base
inactiva. Si un componente se reinicia, cambia la variante base o la condición
ya está activa, la run se marca `failed` o no llega a iniciarse.

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
tres conteos Gold no vacíos e iguales, separados por diez segundos, mientras el
lag de Kafka permanece medible y dentro del umbral. Si el corte no se estabiliza
en cinco minutos, el empaquetado falla, la repetición se marca `failed` y el
lote se detiene. El ejecutor también compara la duración real de cada fase con
la programada; una suspensión del host o una desviación superior al 2 % —con
un margen mínimo de cinco segundos— invalida la repetición.

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

## 5.1. Ejecutar el lote ciego que desacopla tiempo y condición

Este lote es distinto de las tres repeticiones nominales anteriores. Su
propósito es evaluar los algoritmos sobre datos aún no inspeccionados y evitar
que la posición temporal de `adFailure` sea siempre la misma. Mantiene un
horizonte de observación de 50 minutos después de 5 minutos de calentamiento
en cuatro celdas: control sin falla, falla temprana, intermedia y tardía. Cada
celda tiene tres repeticiones, para un total de 12 runs de 55 minutos, más el
tiempo variable de preflight, drenaje y estabilización.

> **Paso previo obligatorio:** no prepare ni ejecute este lote hasta que el
> repositorio académico contenga un generador ciego de puntajes probado contra
> `confirmatory_manifest_v1`. Ese generador debe producir las diez variantes y
> el manifiesto de predicciones con hashes sin leer asignaciones, fases ni datos
> de referencia. Implementarlo y validarlo es el siguiente paso técnico.

Prepare la asignación antes de subir los servicios. Este paso no ejecuta runs:

Desde el repositorio académico hermano, valide primero el manifiesto
confirmatorio congelado:

```bash
python3 experiments/algorithm_comparison/scripts/validate_confirmatory_manifest.py
```

Después, desde `AIOpsLab`, prepare la asignación:

```bash
python3 deploy/etl/blind_batch.py prepare \
  --protocol deploy/etl/blind_protocol_v1.json
```

Por defecto se usa
`../semillero-monitoreo-distribuido/experiments/algorithm_comparison/config/confirmatory_manifest_v1.json`.
Si los repositorios no son hermanos, indique su ubicación con
`--confirmatory-manifest`. La preparación copia sus bytes a
`confirmatory-manifest.snapshot.json` y añade el SHA-256 a los archivos público
y de compromiso; cualquier diferencia posterior hace fallar `verify`.

El comando imprime el directorio del lote. Conserve ese valor como
`<batch-dir>` y verifique el compromiso antes de desplegar:

```bash
python3 deploy/etl/blind_batch.py verify --batch-dir <batch-dir>
```

`commitment.json`, `confirmatory-manifest.snapshot.json` y
`public-cohort.json` pueden entregarse al responsable del modelado.
`allocation.sealed.json` contiene la correspondencia entre código y celda,
tiene permisos `0600` y debe quedar bajo custodia del operador. Es una
separación ciega procedimental, no cifrado: el analista no debe leer el archivo
sellado, los paquetes por run, `phase-timeline.json`, las tablas de condiciones
ni los datos de referencia antes de congelar sus predicciones.

Después de `redeploy`, `status`, `validate` y la estabilización indicada en la
sección 3, ejecute el lote completo:

```bash
python3 deploy/etl/blind_batch.py run --batch-dir <batch-dir>
```

El orquestador conserva las mismas salvaguardas de disco, plano de control,
bandera, lag, duración, estabilización Gold y empaquetado. Cada run tiene una
sola repetición, escala `1.0`, no permite `--allow-backlog` y queda clasificada
como `blind_confirmatory_candidate`. El control ejecuta únicamente calentamiento
y operación normal; nunca modifica la bandera. Si una run falla, el lote se
detiene y conserva el estado y su paquete para diagnóstico. Corrija la causa y
reanude con el mismo comando: las runs completas no se repiten.

El analista debe producir un archivo con una fila por código opaco y las
predicciones, puntuaciones o rankings acordados, sin conocer la asignación. Una
vez cerrado ese archivo, congele su hash y revele la asignación:

```bash
python3 deploy/etl/blind_batch.py reveal \
  --batch-dir <batch-dir> \
  --predictions <predictions-file>
```

La revelación solo se permite si las 12 runs están completas y tienen
`run_id`. El comando crea `predictions-commitment.json` y
`allocation-revealed.json`; así puede demostrarse que los resultados del modelo
existían antes de conocer las condiciones. No edite manualmente los cinco
archivos de control del lote.

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

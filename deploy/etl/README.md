# Continuous multimodal telemetry ETL/ELT

The complete explanation of the architecture, methodology, contracts, delivery
semantics, experimental protocol, diagrams, and limitations is available in
[`docs/etl_multimodal.md`](../../docs/etl_multimodal.md). This file is the short
operational guide for deploying and using the pipeline.

This implementation duplicates metrics, logs, and traces from the Astronomy
Shop Collector to a dedicated Kafka broker, preserves the original OTLP in
Parquet, and produces Silver events and Gold windows with Flink. ClickHouse
serves the windows to Grafana. Grafana does not acquire, merge, or transform the
signals.

```text
Astronomy Shop -> OpenTelemetry Collector -> original backends
                                  |
                                  +-> Kafka -> Flink -> ClickHouse -> Grafana
                                                |
                                                +-> Parquet / MinIO
```

## Data contracts

- **Bronze:** intact binary OTLP envelope, signal, topic, partition, offset,
  Kafka/ingestion timestamps, and experiment identity. Files are partitioned by
  signal, UTC date, and `run_id` under `s3://aiopslab-bronze/otlp`.
- **Silver v2:** one event per metric point, log record, or span. The three
  independent Parquet tables (`metrics`, `logs`, and `traces`) share
  identifiers, complete OTLP attributes, metric unit and instrument type, span
  kind, Kubernetes resources, event time, and provenance. Schema errors go to a
  fourth quarantine table. v2 is stored separately from the retained v1 files.
- **Gold v2.2:** one service observation in a 30-second window sliding every 10
  seconds, keyed by `service.name + k8s.namespace.name`, with an initial
  30-second lateness allowance. `service.namespace`, pod, and deployment are
  context, not keys. CPU is `container.cpu.usage` in cores and memory is
  `container.memory.working_set` in bytes; unsupported semantics remain null.
  Errors remain separate by signal, and request rate and latency use server
  spans.
- **Feature sets:** `feature_tabular_v1` completes the 10-second grid and
  `feature_sequence_v1` groups twelve ordered steps. Neither exposes evaluation
  labels. Missing rows and missing values are not replaced by numeric zero.
- **Reference data:** `condition`, `fault_type`, and `affected_component`
  arrive through `experiment.control.v1` and are retained for evaluation. The
  ClickHouse `feature_tabular_v1` and `feature_sequence_v1` views deliberately
  exclude them to prevent data leakage.

The stable `event_id` marks replays. Parquet files commit with checkpoints. The
ClickHouse JDBC sink is at-least-once; the latest view deduplicates by the
versioned logical `feature_id` and retains the greatest emission version over
the `ReplacingMergeTree` table.

For the complete operator procedure, including redeployment, preflight checks,
smoke tests, the 105-minute protocol, evidence packages, recovery, and shutdown,
see [`GUIA_EJECUCION.md`](GUIA_EJECUCION.md).

## Deployment

Requirements: Docker, Docker Compose, kind, and kubectl. Helm, Flink, Kafka,
ClickHouse, MinIO, and Grafana run in containers; no server is installed on the
host operating system.

The original community MinIO images were removed from Docker Hub. For
reproducibility, this setup uses immutable public copies from
`bitnamilegacy/minio` and `bitnamilegacy/minio-client`, with equivalent server
and client versions pinned in `.env.example`.

Copy the local variables if you need to change credentials or versions:

```bash
cp deploy/etl/.env.example deploy/etl/.env
```

Create or reuse the cluster and deploy the complete environment:

```bash
./scripts/etl.sh bootstrap
```

To rebuild and recreate every service while retaining Kafka, ClickHouse, and
MinIO evidence volumes:

```bash
./scripts/etl.sh redeploy
```

Operate individual components:

```bash
./scripts/etl.sh up
./scripts/etl.sh deploy-app
./scripts/grafana.sh up
./scripts/etl.sh status
./scripts/etl.sh validate
```

The script creates a selectorless Service in `astronomy-shop` that points to the
Docker `kind` network gateway. Kafka advertises an internal listener
(`kafka:9092`) for Flink and an external listener
(`<kind-network-gateway>:29092`) for the Collector. This keeps the ETL broker
separate from Astronomy Shop's functional Kafka instance.

## Controlled experiment

By default, each repetition runs five minutes of warm-up, ten minutes of normal
operation, ten minutes with an advertising-service failure, and ten minutes of
recovery. The protocol performs three repetitions:

```bash
python3 deploy/etl/experiment.py
```

The runner discovers the actual feature-flag name when the chart uses
`adServiceFailure` instead of the requested `adFailure` alias. It always
restores the original variant and records runs and phase intervals in ClickHouse
and Kafka. Before the batch and each repetition it rejects effective host-disk
usage above 90%, requires a stable Kubernetes control plane, and invalidates a
run if a control-plane pod restarts during the observation. The local profile
uses incremental RocksDB checkpoints every 60 seconds, without concurrency and
with a 30-second minimum pause, to prevent continuous full-snapshot I/O from
starving the Kind control plane.

For a short functional test that must not be treated as an experimental run:

```bash
python3 deploy/etl/experiment.py --repetitions 1 --duration-scale 0.01
```

For a stationary diagnostic without fault injection:

```bash
python3 deploy/etl/experiment.py \
  --scenario astronomy-stationary-v1 \
  --repetitions 1 \
  --stationary-seconds 600
```

After a run, generate the field inventory and quantitative profile (optionally
restricted to one run):

```bash
./scripts/etl.sh profile
./scripts/etl.sh profile <run_id>
```

Each experiment also writes an immutable-style evidence package under
`deploy/etl/reports/runs/<run_id>/`. It includes the manifest, configuration,
offset/checkpoint snapshots, phase timeline, telemetry profile, Gold quality
report, component logs, and SHA-256 checksums. Package collection starts only
after a non-empty Gold count remains unchanged for three consecutive polls.

The JSON profile is written to `deploy/etl/reports/`. It includes volume and
bytes per signal, rates, services, pods, metric/operation names, severities,
`trace_id`/`span_id` coverage, delay, ordering, duplicates, schema errors,
resource signatures, and multimodal coverage. These measurements are the input
for adjusting partitions, resources, windows, and watermarks; the initial
values are not presented as final choices.

## Integration state

Gold v2.2, the run protocol, packaging, Grafana integration, and the deployment
guards are implemented. Development smoke executions are intentionally kept
local and are not experimental evidence. Run the full protocol only after the
v2 Flink consumer has caught up, validation succeeds, and the target namespace
scope has been fixed. The three nominal repetitions must be produced without
changing versions, load, contracts, or operational configuration between them;
only those packages may support quantitative acceptance of Gold.

## Local interfaces

| Interface | URL |
|---|---|
| Grafana | <http://localhost:3000> |
| Flink | <http://localhost:8081> |
| ClickHouse HTTP | <http://localhost:8123> |
| MinIO API | <http://localhost:9000> |
| MinIO Console | <http://localhost:9001> |
| Kafka for the Collector | `<kind-network-gateway>:29092` |

User interfaces bind to `127.0.0.1`. Kafka binds only to the kind network
gateway so the Collector can reach it. This experimental profile does not
include production authentication, high availability, or hardening.

## Shutdown and retained data

```bash
./scripts/etl.sh down
```

This command preserves the `aiopslab-etl-kafka-data`,
`aiopslab-etl-clickhouse-data`, and `aiopslab-etl-minio-data` volumes. There
is no automatic deletion command because these volumes contain historical
experiment evidence.

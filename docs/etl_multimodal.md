# Continuous multimodal telemetry ETL/ELT

> **Version note (November 10, 2026).** Sections that name
> `gold_multimodal`, `pattern_input`, pod-level keys, or a combined error rate
> document the retained v1 prototype. The active implementation is
> `gold_service_window_v2.2` with service-level keys, separate log/span errors,
> server-span request semantics, explicit metric units, a complete time grid,
> `feature_tabular_v1`, `feature_sequence_v1`, and separate
> `evaluation_reference_v1`. The current operator guide and evidence-package
> procedure are in [`deploy/etl/GUIA_EJECUCION.md`](../deploy/etl/GUIA_EJECUCION.md).

## 1. Purpose and scope

This document describes the AIOpsLab pipeline that jointly acquires, preserves,
transforms, and queries metrics, logs, and traces from Astronomy Shop. Its
technical goal is to produce reproducible multimodal observations associated
with system components and time windows. Those observations will later support
analysis and machine learning.

The solution is a streaming ETL/ELT pipeline:

- it is **ELT** because the original OTLP message is preserved without loss
  before downstream representations are allowed to evolve;
- it is **ETL** because it normalizes all three signals, computes windowed
  features, and materializes an analytical representation in ClickHouse;
- it is **experimental** because the current windows, watermarks, partitions,
  and features are initial values that must be adjusted using a quantitative
  profile of real telemetry.

Grafana does not acquire or semantically merge signals. It only queries the
original backends and the Gold representation, then presents the results.

## 2. Overall architecture

~~~mermaid
flowchart LR
    subgraph K8S[kind cluster: Astronomy Shop]
        APP[Astronomy Shop] -->|OTLP| COL[OpenTelemetry Collector]
        COL -->|metrics| PROM[Prometheus]
        COL -->|logs| OS[OpenSearch]
        COL -->|traces| JAEGER[Jaeger]
    end

    subgraph DOCKER[Docker ETL services]
        KAFKA[(Dedicated Kafka)] --> FLINK[Apache Flink]
        FLINK -->|Bronze OTLP| BRONZE[(Parquet / MinIO)]
        FLINK -->|Normalized Silver| SILVER[(Parquet / MinIO)]
        FLINK -->|Windowed Gold| CH[(ClickHouse)]
        CH --> ML[Future ML component]
        CH --> GRAFANA[Grafana]
    end

    COL -->|OTLP protobuf by signal| KAFKA
    PROM --> GRAFANA
    OS --> GRAFANA
    JAEGER --> GRAFANA
~~~

The ETL Kafka broker is separate from the functional Kafka instance bundled
with Astronomy Shop. This isolation prevents an experiment, replay, or pipeline
failure from affecting both the observed application and the data pipeline.

### 2.1 Component responsibilities

| Component | Responsibility | Explicitly out of scope |
|---|---|---|
| OpenTelemetry Collector | Receive OTLP, retain the original routes, and duplicate each signal to Kafka | Multimodal correlation and machine learning |
| Dedicated Kafka | Decouple producer and consumer, absorb bursts, and support offset-based replay | Semantic transformation |
| Flink | Preserve Bronze, normalize Silver, manage event time, and build Gold | Result presentation |
| MinIO + Parquet | Historical Bronze/Silver archive partitioned by signal, date, and run | Low-latency interactive queries |
| ClickHouse | Persist and query Gold features and experiment metadata | OTLP acquisition |
| Grafana | Query backends and present original telemetry and derived features | Semantic fusion and model execution |
| Experiment runner | Control normal, fault, and recovery phases and record reference data | Feature generation |

### 2.2 Reproducible versions

Versions are pinned rather than left floating. Docker Compose fixes Kafka 4.2.0,
Flink 1.20.1 with Java 17, ClickHouse 25.3.8.23, MinIO 2025.4.22, and MinIO
Client 2025.4.16. Grafana is fixed at 13.2.2, while Astronomy Shop uses chart
0.42.6.

The Collector Kafka exporter is still a beta capability. The chart version is
therefore pinned, and exporter compatibility is part of the explicit
experimental validation rather than an assumption across upgrades.

## 3. Design methodology

The implementation follows four core principles.

### 3.1 Preserve before transforming

Each Kafka record contains a binary OTLP export request. Flink writes it to
Bronze before or in parallel with normalization. If the Silver schema,
correlation logic, or learning method changes, the dataset can be rebuilt from
Kafka or Bronze evidence without necessarily repeating the experiment.

### 3.2 Separate logical contracts from storage engines

Bronze, Silver, Gold, and Results are conceptual data contracts. In the local
implementation, Bronze and Silver use Parquet/MinIO while Gold uses ClickHouse.
Field meaning does not depend on those engines. This separation makes it
possible to compare the local architecture with a managed alternative without
redefining the unit of analysis.

### 3.3 Use event time and expose data quality

Telemetry may arrive late or out of order. Flink assigns the OTLP timestamp as
event time, uses watermarks with an initial 30-second out-of-order allowance,
and builds 30-second sliding windows every 10 seconds. Duplicates, late events,
schema errors, and missing signals are not hidden. They become explicit quality
features.

### 3.4 Separate model features from evaluation labels

<code>condition</code>, <code>fault_type</code>, and
<code>affected_component</code> are retained to compare results with known
conditions. They are excluded from the <code>pattern_input</code> view because
using them as model inputs would introduce data leakage.

### 3.5 Relationship to the DSRM cycle

The pipeline supports an iterative design and evaluation cycle:

1. **Problem identification:** the previous extraction was batch-oriented,
   separated signals, and discarded attributes needed for reproducibility.
2. **Objective definition:** continuous acquisition, original-data retention,
   joint representation, lineage, and reproducible queries.
3. **Design and development:** independent components connected through Kafka
   and explicit Bronze, Silver, Gold, and Results contracts.
4. **Demonstration:** controlled warm-up, normal, fault, and recovery phases.
5. **Evaluation:** throughput, delay, duplicates, coverage, semantic
   consistency, and Gold behavior are quantitatively profiled.
6. **Iteration:** observed delay percentiles and actual rates drive changes to
   watermarks, windows, resources, and partitions.

The initial configuration is therefore an engineering hypothesis to be tested,
not a final result.

## 4. Detailed event flow

~~~mermaid
sequenceDiagram
    participant A as Astronomy Shop
    participant C as OTel Collector
    participant K as Dedicated Kafka
    participant E as Experiment control
    participant F as Flink
    participant M as MinIO
    participant H as ClickHouse
    participant G as Grafana

    A->>C: OTLP metrics, logs, and traces
    par Original routes
        C->>C: Signal-specific processing
    and Durable duplication
        C->>K: OTLP protobuf in independent topics
    end
    E->>K: Condition interval and run_id
    K->>F: Payload plus topic, partition, and offset
    F->>F: Attach experiment interval
    par Preservation
        F->>M: Original OTLP in Bronze Parquet
    and Transformation
        F->>F: Expand points, logs, and spans into Silver
        F->>M: Silver Parquet by signal
        F->>F: Watermarks, deduplication, and windows
        F->>H: Gold vector by component and window
    end
    H->>G: SQL queries
~~~

### 4.1 Acquisition and duplication

The Collector retains the existing exporters:

- metrics to Prometheus;
- logs to OpenSearch;
- traces to Jaeger.

It also uses three Kafka exporters with <code>otlp_proto</code> encoding,
sending queues, and retries enabled:

| Signal | Topic | Partitions | Initial retention |
|---|---|---:|---:|
| Metrics | <code>otlp.metrics.v1</code> | 3 | 72 hours |
| Logs | <code>otlp.logs.v1</code> | 3 | 72 hours |
| Traces | <code>otlp.traces.v1</code> | 3 | 72 hours |
| Experiment control | <code>experiment.control.v1</code> | 3 | 72 hours |

The <code>v1</code> suffix versions the transport contract. An incompatible
change must create a new version rather than silently reinterpret stored
messages.

### 4.2 Bronze: original evidence

Bronze stores the binary OTLP message with its provenance:

| Field | Meaning |
|---|---|
| <code>signal</code> | <code>metrics</code>, <code>logs</code>, or <code>traces</code> |
| <code>topic</code>, <code>partition</code>, <code>offset</code> | reproducible Kafka location |
| <code>kafkaTimestamp</code>, <code>receivedAt</code> | transport and ingestion times |
| <code>payload</code> | unmodified OTLP protobuf request |
| <code>runId</code>, <code>scenarioId</code> | experiment identity |
| <code>condition</code>, <code>faultType</code>, <code>affectedComponent</code> | attached evaluation reference |

Files are stored under <code>s3://aiopslab-bronze/otlp</code> using partitions
such as:

~~~text
signal=traces/date=2026-10-28/run_id=<uuid>/bronze-....parquet
~~~

The original message remains available even if normalization produces a schema
error.

### 4.3 Silver: shared event contract

The normalizer expands every OTLP request into one logical row per metric point,
log record, or span. All three tables share identity, provenance, time,
resource, and experiment-reference fields.

~~~mermaid
flowchart TB
    OTLP[Binary OTLP request]
    OTLP --> METRIC[One event per metric point]
    OTLP --> LOG[One event per log record]
    OTLP --> TRACE[One event per span]
    METRIC --> COMMON[Shared Silver contract]
    LOG --> COMMON
    TRACE --> COMMON
    COMMON --> QM[metrics Parquet]
    COMMON --> QL[logs Parquet]
    COMMON --> QT[traces Parquet]
    OTLP -->|parse failure| QE[schema_errors Parquet]
~~~

Important shared fields include:

- stable <code>eventId</code>;
- <code>eventTime</code>, <code>observedTime</code>, and
  <code>receivedAt</code>;
- source topic, partition, and offset;
- run, scenario, and condition;
- <code>service.name</code>, <code>service.namespace</code>, and Kubernetes
  resources;
- <code>traceId</code>, <code>spanId</code>, and
  <code>parentSpanId</code> when available;
- complete event and resource attributes serialized as JSON;
- <code>late</code>, <code>duplicate</code>, and
  <code>schemaError</code> indicators.

Signal-specific normalization:

- **metrics:** retain name and numeric value. Histograms provisionally use
  <code>sum / count</code>; summaries emit one event per quantile;
- **logs:** retain body, severity, and trace links, then create a basic template
  for grouping messages with variable values;
- **traces:** retain operation, hierarchy, duration, error status, and dependency
  target when OTLP attributes provide one;
- **errors:** an unreadable request produces a row in
  <code>schema_errors</code>; the original payload remains in Bronze.

### 4.4 Identity, ordering, and deduplication

<code>eventId</code> is the SHA-256 digest of
<code>topic:partition:offset:index</code>, where <code>index</code> is the point
position within the OTLP request. Re-reading the same Kafka record therefore
produces the same identifiers.

Flink retains a <code>seen-event</code> state for six hours per event identifier.
A replay is marked as a duplicate and archived in Silver. Gold increments
<code>duplicate_event_count</code> but does not add the event to feature values
again.

An event is marked late when
<code>receivedAt - eventTime &gt; 30 seconds</code>. The watermark initially
allows 30 seconds of disorder, declares a source idle after 30 seconds, and the
window allows 30 additional seconds of lateness. These values must be reviewed
against the observed p99 delay.

### 4.5 Experiment reference data

The control topic distributes intervals containing run ID, scenario, condition,
fault type, affected component, and start/end timestamps. Flink stores the
intervals as broadcast state and chooses the most recent interval containing
the Bronze Kafka timestamp.

This ingestion-time association is traceable, but it must be compared with an
event-time association if profiling reveals meaningful delays near phase
boundaries.

### 4.6 Gold v1 retained: joint unit of analysis

Gold is not a Cartesian join between metric points, logs, and spans. It is one
component observation per time window:

~~~text
(run_id, window, service_name, service_namespace,
 k8s_namespace, pod_name, deployment_name)
~~~

The initial window is 30 seconds long and advances every 10 seconds. Grouping
implements outer-join semantics: if a signal is absent, the row is retained and
<code>missing_metrics</code>, <code>missing_logs</code>, or
<code>missing_traces</code> is set.

| Group | Implemented features |
|---|---|
| Metrics | point count, mean CPU, and mean memory selected by metric name |
| Logs | count, error count, severity counts, and template counts |
| Traces | span count, error spans, p50/p95/p99 duration, and dependencies |
| Joint | request rate and error rate |
| Quality | missing signals, late events, duplicates, and schema errors |
| Reference | condition, fault, and affected component for evaluation only |

Current feature definitions are explicit and provisional:

- <code>request_rate = span_count / window_seconds</code>;
- <code>error_rate = (error_logs + error_spans) / (logs + spans)</code>;
- latency percentiles come from span durations;
- CPU and memory are selected by matching metric names;
- a window crossing multiple experiment references is marked
  <code>mixed</code>.

These rules enable initial profiling. They do not replace feature selection
based on the real metric inventory and semantic conventions.

The active v2.2 contract replaces these provisional rules. Its logical key is
`run_id + window + service.name + k8s.namespace.name`; `service.namespace`, pod,
and deployment remain descriptive context. Request rate and latency use server
spans, log and span errors remain separate, CPU accepts only the
`container.cpu.usage` gauge in `{cpu}`, and memory accepts only the
`container.memory.working_set` gauge in bytes. Unresolved identities remain in
Silver and do not enter Gold. A 180-second stationary confirmation produced 916
unique logical keys and 95% CPU/memory coverage for the examined application
services in the `astronomy-shop` namespace. Signals without Kubernetes
namespace remain a separate quality stratum rather than being merged by name.

### 4.7 Results

ClickHouse exposes three interfaces:

- <code>gold_multimodal_latest</code>: the current version of every Gold vector;
- <code>signal_coverage</code>: per-minute signal coverage and quality counters;
- <code>pattern_input</code>: inputs available to the learning method, excluding
  evaluation labels.

The starter Grafana dashboard shows request rate, p95 latency, multimodal
coverage, and a table of windows with evaluation references. The reference
table is intended for inspection and evaluation. A model must read
<code>pattern_input</code> or another view that preserves the same separation.

## 5. Delivery and recovery semantics

Guarantees differ by destination:

| Path | Implemented guarantee | Mechanism |
|---|---|---|
| Collector to Kafka | retries and queue with broker acknowledgement | Kafka exporter with <code>acks=1</code> |
| Kafka to Flink state | checkpoint-consistent | offsets restored with job state |
| Flink to Parquet | exactly once relative to completed checkpoints | files finalized on checkpoint completion |
| Flink to ClickHouse | at least once | JDBC batches with three retries |
| Gold queries | logical idempotency | <code>feature_id</code>, <code>version</code>, and <code>ReplacingMergeTree</code> |

ClickHouse may receive the same window again after recovery.
<code>gold_multimodal_latest</code> presents the most recent version instead of
showing retries as separate observations. This query-level idempotency does not
turn JDBC into a distributed exactly-once transaction.

Checkpoints and savepoints are stored under
<code>s3://aiopslab-checkpoints</code>. Kafka, ClickHouse, and MinIO volumes are
not deleted by <code>./scripts/etl.sh down</code>.

## 6. Experimental protocol

~~~mermaid
flowchart LR
    W[Warm-up<br/>5 min] --> N[Normal<br/>10 min]
    N --> F[adFailure<br/>10 min]
    F --> R[Recovery<br/>10 min]
    R --> REP{More<br/>repetitions?}
    REP -->|yes, up to 3| W
    REP -->|no| PROFILE[Profiling and validation]
~~~

Each repetition creates a <code>run_id</code> and records:

- application version, configuration, and load profile;
- scenario, repetition, and run status;
- collection start and end;
- normal, fault, and recovery intervals;
- starting/ending offsets and observed rate by topic.

The runner modifies the advertising-service failure flag, waits through each
phase, and restores the original flag variant even when an exception occurs. A
<code>duration-scale</code> below one enables smoke tests, but those tests must
not be treated as full experimental repetitions.

## 7. Quantitative profiling and adjustment criteria

The profiler reads Bronze/Silver from MinIO and Gold from ClickHouse without
modifying either dataset. It writes a JSON report containing:

- records, events, and bytes by signal;
- event and byte rates;
- services, pods, metrics, operations, and severities;
- <code>trace_id</code> and <code>span_id</code> coverage;
- delay, disorder, duplicates, and schema errors;
- resource-attribute signatures for schema-change detection;
- services present in one, two, or all three signals;
- multimodal coverage and Gold run counts.

The measurements must answer at least these questions:

1. Does the watermark exceed p99 delay without creating excessive result
   latency?
2. Can three partitions sustain the maximum rate of each signal?
3. Are <code>service.name</code> and Kubernetes resources consistent across
   signals?
4. What percentage of logs can be directly correlated with spans?
5. Do the selected CPU, memory, latency, and error measurements have consistent
   semantics across services?
6. Do 30-second windows capture the normal-fault-recovery transition without
   diluting it?

### 7.1 Preliminary integrated profile

Local smoke executions were used during development only to exercise the
end-to-end lifecycle and to refine the implementation. Their generated
packages and measurements are intentionally not versioned and must not be
treated as experimental evidence. Quantitative volume, coverage, delay,
checkpoint, and Gold-acceptance results remain pending until the three nominal
repetitions run with a caught-up consumer and an unchanged configuration.

## 8. Deployment and network boundaries

The Collector runs in the kind cluster while Kafka runs in the Docker Compose
network. The script creates a selectorless Kubernetes Service pointing to the
IPv4 gateway of the Docker kind network:

~~~text
Collector -> aiopslab-etl-kafka:29092 -> <kind-gateway>:29092 -> Kafka
Flink     -> kafka:9092                                   -> Kafka
~~~

Kafka advertises an internal listener for Flink and an external listener bound
only to the kind gateway. ClickHouse, MinIO, Flink, and Grafana expose their user
interfaces only on <code>127.0.0.1</code>.

The environment is intentionally local and experimental. It does not implement
TLS, production secret management, high availability, multi-broker replication,
or production hardening.

## 9. Operation

Prepare optional local variables:

~~~bash
cp deploy/etl/.env.example deploy/etl/.env
~~~

Deploy and validate the full environment:

~~~bash
./scripts/etl.sh bootstrap
./scripts/etl.sh validate
~~~

Inspect status and logs:

~~~bash
./scripts/etl.sh status
./scripts/etl.sh logs
~~~

Run a short functional test first:

~~~bash
python3 deploy/etl/experiment.py --repetitions 1 --duration-scale 0.01
~~~

Then run the full protocol and profile the result:

~~~bash
python3 deploy/etl/experiment.py
./scripts/etl.sh profile
~~~

Stop services without deleting evidence:

~~~bash
./scripts/etl.sh down
~~~

## 10. Implementation and validation status

As of October 28, 2026, the following checks have passed:

- the <code>aiopslab/telemetry-etl:1.0.0</code> image built successfully, the
  Flink job compiled, and all five Java unit tests passed;
- all three experiment-runner unit tests passed;
- the Collector rendered with all three Kafka exporters while preserving
  Prometheus, OpenSearch, and Jaeger;
- the kind control-plane and worker reached <code>Ready</code>;
- Kafka 4.2.0 became healthy, all four topics were created, and Kubernetes
  reached the broker;
- the live OTLP path produced committed Bronze and Silver Parquet files in
  MinIO and Gold rows in ClickHouse;
- the first backlog-drain session exposed three heap failures while the
  ClickHouse driver assembled 200-row JDBC batches; the configurable batch was
  reduced to 20 rows, and replacement job
  <code>969b5a8c439a3bade289b2c7bc54299b</code> kept ten subtasks active, completed
  12 checkpoints with none failed, and reported no exceptions during the
  follow-up observation;
- one scaled normal-fault-recovery run completed and restored
  <code>adFailure</code> to <code>off</code>;
- ClickHouse retained the four phase intervals, Gold labels, and
  <code>mixed</code> boundary windows while <code>pattern_input</code> exposed no
  evaluation-label columns;
- the Grafana ClickHouse data source returned <code>OK</code>, and the
  <code>aiopslab-etl-multimodal</code> dashboard was provisioned;
- the profiler built and generated a quantitative JSON inventory from MinIO
  and ClickHouse.

The full three-repetition, 105-minute protocol remains pending. The scaled run
is integration evidence, not an experimental result suitable for evaluating a
learning method or selecting the final architecture.

## 11. Limitations and pending decisions

- Windows and lateness settings are initial values and must be adjusted after
  a steady-state profile. The first profile includes retained-record catch-up
  and therefore overstates normal reception delay.
- The deduplication TTL is six hours and must be compared with the longest
  expected replay.
- Events beyond allowed window lateness do not yet have a dedicated side output
  for extreme-lateness analysis.
- CPU and memory now use explicit metric name, unit, and instrument contracts;
  their applicability to later environments still requires validation.
- Request rate is approximated from spans rather than the temporal difference of
  a monotonic OTLP counter.
- Metric histograms are reduced to a mean; Bronze allows a different
  representation to be implemented without repeating the experiment.
- Direct log-to-span identifiers are preserved, but Gold aggregates by component
  and does not yet materialize a separate table of individual links.
- The JDBC sink is at least once; idempotency is resolved at query time.
- The 20-row JDBC batch prevented the observed heap failure in a short loaded
  run, but it still requires a prolonged maximum-throughput stress test.
- Evaluation labels support comparison but do not establish causality.
- Some application signals carry `service.name` without Kubernetes namespace;
  they remain separate until instrumentation or a verified association rule
  supplies the missing identity.
- The machine-learning method has not been selected and must be compared
  empirically on <code>feature_tabular_v1</code> and, when justified,
  <code>feature_sequence_v1</code>.

## 12. Implementation map

| Element | File |
|---|---|
| Docker orchestration | <code>deploy/etl/docker-compose.yaml</code> |
| Local versions and credentials | <code>deploy/etl/.env.example</code> |
| Collector exporters | <code>aiopslab/observer/grafana/astronomy-shop-values.yaml</code> |
| Main Flink job | <code>deploy/etl/flink-job/src/main/java/org/aiopslab/etl/TelemetryEtlJob.java</code> |
| OTLP normalization | <code>deploy/etl/flink-job/src/main/java/org/aiopslab/etl/OtlpNormalizer.java</code> |
| Gold windows | <code>deploy/etl/flink-job/src/main/java/org/aiopslab/etl/FeatureWindowFunction.java</code> |
| ClickHouse schema | <code>deploy/etl/clickhouse/init/001_schema.sql</code> |
| Experimental protocol | <code>deploy/etl/experiment.py</code> |
| Quantitative profiler | <code>deploy/etl/profiling/profile.py</code> |
| Grafana data source | <code>deploy/grafana/provisioning/datasources/aiopslab.yaml</code> |
| Starter dashboard | <code>deploy/grafana/dashboards/etl-multimodal.json</code> |
| Integrated operation | <code>scripts/etl.sh</code> |

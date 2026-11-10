CREATE DATABASE IF NOT EXISTS aiopslab;

CREATE TABLE IF NOT EXISTS aiopslab.experiment_runs
(
    run_id String,
    scenario_id LowCardinality(String),
    application_version String,
    configuration String,
    load_profile String,
    collection_start DateTime64(3, 'UTC'),
    collection_end Nullable(DateTime64(3, 'UTC')),
    repetition UInt16,
    status LowCardinality(String),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY run_id;

CREATE TABLE IF NOT EXISTS aiopslab.experiment_conditions
(
    run_id String,
    phase_id String,
    condition LowCardinality(String),
    fault_type LowCardinality(String),
    affected_component LowCardinality(String),
    phase_start DateTime64(3, 'UTC'),
    phase_end Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (run_id, phase_id);

CREATE TABLE IF NOT EXISTS aiopslab.gold_multimodal
(
    feature_id String,
    run_id String,
    scenario_id LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    service_name LowCardinality(String),
    service_namespace LowCardinality(String),
    k8s_namespace LowCardinality(String),
    pod_name LowCardinality(String),
    deployment_name LowCardinality(String),

    metric_point_count UInt64,
    request_rate Float64,
    error_rate Float64,
    latency_p50_ms Float64,
    latency_p95_ms Float64,
    latency_p99_ms Float64,
    cpu_mean Float64,
    memory_mean_bytes Float64,

    log_count UInt64,
    log_error_count UInt64,
    severity_counts_json String,
    event_template_counts_json String,

    span_count UInt64,
    error_span_count UInt64,
    span_duration_p95_ms Float64,
    dependency_edge_count UInt64,
    dependency_edges_json String,

    missing_metrics UInt8,
    missing_logs UInt8,
    missing_traces UInt8,
    late_event_count UInt64,
    duplicate_event_count UInt64,
    schema_error_count UInt64,

    condition LowCardinality(String),
    fault_type LowCardinality(String),
    affected_component LowCardinality(String),
    emitted_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (run_id, window_start, service_name, k8s_namespace, pod_name, feature_id)
TTL toDateTime(window_start) + INTERVAL 90 DAY DELETE;

CREATE VIEW IF NOT EXISTS aiopslab.gold_multimodal_latest AS
SELECT * EXCEPT(version)
FROM aiopslab.gold_multimodal FINAL;

CREATE VIEW IF NOT EXISTS aiopslab.signal_coverage AS
SELECT
    run_id,
    toStartOfMinute(window_start) AS minute,
    count() AS windows,
    1 - avg(missing_metrics) AS metric_coverage,
    1 - avg(missing_logs) AS log_coverage,
    1 - avg(missing_traces) AS trace_coverage,
    sum(late_event_count) AS late_events,
    sum(duplicate_event_count) AS duplicate_events,
    sum(schema_error_count) AS schema_errors
FROM aiopslab.gold_multimodal_latest
GROUP BY run_id, minute;

CREATE VIEW IF NOT EXISTS aiopslab.pattern_input AS
SELECT
    feature_id,
    run_id,
    window_start,
    window_end,
    service_name,
    service_namespace,
    k8s_namespace,
    pod_name,
    deployment_name,
    metric_point_count,
    request_rate,
    error_rate,
    latency_p50_ms,
    latency_p95_ms,
    latency_p99_ms,
    cpu_mean,
    memory_mean_bytes,
    log_count,
    log_error_count,
    span_count,
    error_span_count,
    span_duration_p95_ms,
    dependency_edge_count,
    missing_metrics,
    missing_logs,
    missing_traces,
    late_event_count
FROM aiopslab.gold_multimodal_latest;

-- The pattern_input view deliberately excludes condition, fault_type, and
-- affected_component. These columns remain available for evaluation only.

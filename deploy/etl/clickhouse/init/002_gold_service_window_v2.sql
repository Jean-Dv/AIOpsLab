CREATE DATABASE IF NOT EXISTS aiopslab;

CREATE TABLE IF NOT EXISTS aiopslab.gold_service_window_v2
(
    feature_id String,
    run_id String,
    scenario_id LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    service_name LowCardinality(String),
    service_namespace LowCardinality(String),
    k8s_namespace LowCardinality(String),
    pod_count UInt32,
    deployment_count UInt32,
    pod_names_json String,
    deployment_names_json String,

    metric_point_count UInt64,
    cpu_sample_count UInt64,
    cpu_utilization Nullable(Float64),
    cpu_usage_cores Nullable(Float64),
    cpu_sources_json String,
    memory_sample_count UInt64,
    memory_working_set_bytes Nullable(Float64),
    memory_sources_json String,

    log_count UInt64,
    log_error_count UInt64,
    log_error_ratio Nullable(Float64),
    severity_counts_json String,
    event_template_counts_json String,

    span_count UInt64,
    server_span_count UInt64,
    error_span_count UInt64,
    span_error_ratio Nullable(Float64),
    request_rate Nullable(Float64),
    request_rate_source LowCardinality(String),
    latency_sample_count UInt64,
    latency_p50_ms Nullable(Float64),
    latency_p95_ms Nullable(Float64),
    latency_p99_ms Nullable(Float64),
    dependency_edge_count UInt64,
    dependency_edges_json String,
    span_kind_counts_json String,

    missing_metrics UInt8,
    missing_logs UInt8,
    missing_traces UInt8,
    identity_mismatch_suspected UInt8,
    late_event_count UInt64,
    duplicate_event_count UInt64,
    schema_error_count UInt64,
    trace_id_present_count UInt64,
    span_id_present_count UInt64,
    service_name_present_count UInt64,
    k8s_namespace_present_count UInt64,
    row_present UInt8,

    source_references_json String,
    gold_contract_version LowCardinality(String),
    normalizer_version LowCardinality(String),
    log_template_version LowCardinality(String),
    emitted_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (
    gold_contract_version, run_id, window_start,
    service_name, service_namespace, k8s_namespace, feature_id
)
TTL toDateTime(window_start) + INTERVAL 90 DAY DELETE;

ALTER TABLE aiopslab.gold_service_window_v2
    ADD COLUMN IF NOT EXISTS cpu_usage_cores Nullable(Float64) AFTER cpu_utilization;

CREATE OR REPLACE VIEW aiopslab.gold_service_window_v2_latest AS
SELECT * EXCEPT(version)
FROM
(
    -- feature_id is derived from the versioned logical key. Deduplicating by
    -- that identifier also covers a late re-emission whose descriptive
    -- service_namespace changed after stronger pod context arrived.
    SELECT *
    FROM aiopslab.gold_service_window_v2 FINAL
    ORDER BY version DESC
    LIMIT 1 BY feature_id
);

-- Complete 10-second grid between the first and last observed window for each
-- service/run. A generated step has row_present=0 and NULL numeric values.
CREATE OR REPLACE VIEW aiopslab.gold_service_window_grid_v2 AS
WITH service_ranges AS
(
    SELECT
        run_id,
        any(scenario_id) AS scenario_id,
        any(gold_contract_version) AS gold_contract_version,
        service_name,
        service_namespace,
        k8s_namespace,
        min(window_start) AS first_window,
        max(window_start) AS last_window
    FROM aiopslab.gold_service_window_v2_latest
    GROUP BY run_id, service_name, service_namespace, k8s_namespace
),
grid AS
(
    SELECT
        run_id,
        scenario_id,
        gold_contract_version,
        service_name,
        service_namespace,
        k8s_namespace,
        first_window + toIntervalSecond(step * 10) AS window_start
    FROM service_ranges
    ARRAY JOIN range(toUInt32(dateDiff('second', first_window, last_window) / 10) + 1) AS step
)
SELECT
    if(g.feature_id = '', hex(SHA256(concat(
        'gold_service_window_v2:', grid.run_id, ':', toString(grid.window_start), ':',
        grid.service_name, ':', grid.service_namespace, ':', grid.k8s_namespace
    ))), g.feature_id) AS feature_id,
    grid.run_id AS run_id,
    grid.scenario_id AS scenario_id,
    grid.window_start AS window_start,
    grid.window_start + INTERVAL 30 SECOND AS window_end,
    grid.service_name AS service_name,
    grid.service_namespace AS service_namespace,
    grid.k8s_namespace AS k8s_namespace,
    if(g.feature_id = '', 0, g.pod_count) AS pod_count,
    if(g.feature_id = '', 0, g.deployment_count) AS deployment_count,
    if(g.feature_id = '', CAST(NULL, 'Nullable(UInt64)'), toNullable(g.metric_point_count)) AS metric_point_count,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.request_rate) AS request_rate,
    if(g.feature_id = '', CAST(NULL, 'Nullable(UInt64)'), toNullable(g.span_count)) AS span_count,
    if(g.feature_id = '', CAST(NULL, 'Nullable(UInt64)'), toNullable(g.server_span_count)) AS server_span_count,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.span_error_ratio) AS span_error_ratio,
    if(g.feature_id = '', CAST(NULL, 'Nullable(UInt64)'), toNullable(g.log_count)) AS log_count,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.log_error_ratio) AS log_error_ratio,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.latency_p50_ms) AS latency_p50_ms,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.latency_p95_ms) AS latency_p95_ms,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.latency_p99_ms) AS latency_p99_ms,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.cpu_usage_cores) AS cpu_usage_cores,
    if(g.feature_id = '', CAST(NULL, 'Nullable(Float64)'), g.memory_working_set_bytes) AS memory_working_set_bytes,
    if(g.feature_id = '', CAST(NULL, 'Nullable(UInt64)'), toNullable(g.dependency_edge_count)) AS dependency_edge_count,
    if(g.feature_id = '', 1, g.missing_metrics) AS missing_metrics,
    if(g.feature_id = '', 1, g.missing_logs) AS missing_logs,
    if(g.feature_id = '', 1, g.missing_traces) AS missing_traces,
    if(g.feature_id = '', 0, g.identity_mismatch_suspected) AS identity_mismatch_suspected,
    if(g.feature_id = '', 0, g.late_event_count) AS late_event_count,
    if(g.feature_id = '', 0, g.duplicate_event_count) AS duplicate_event_count,
    if(g.feature_id = '', 0, g.schema_error_count) AS schema_error_count,
    if(g.feature_id = '', 0, 1) AS row_present,
    if(g.feature_id = '', '{}', g.source_references_json) AS source_references_json,
    grid.gold_contract_version AS gold_contract_version
FROM grid
LEFT JOIN aiopslab.gold_service_window_v2_latest AS g
    ON grid.run_id = g.run_id
   AND grid.window_start = g.window_start
   AND grid.service_name = g.service_name
   AND grid.service_namespace = g.service_namespace
   AND grid.k8s_namespace = g.k8s_namespace;

-- Model input: identifiers remain available for splitting and traceability, but
-- condition, fault_type and affected_component are never exposed here.
CREATE OR REPLACE VIEW aiopslab.feature_tabular_v1 AS
SELECT *
FROM aiopslab.gold_service_window_grid_v2;

-- Long-form T x F contract. sequence_id groups twelve consecutive 10-second
-- steps; sequence_step preserves order and row_present exposes discontinuities.
CREATE OR REPLACE VIEW aiopslab.feature_sequence_v1 AS
SELECT
    *,
    hex(SHA256(concat(
        run_id, ':', service_name, ':', service_namespace, ':', k8s_namespace, ':',
        toString(intDiv(step_index, 12))
    ))) AS sequence_id,
    step_index % 12 AS sequence_step,
    12 AS expected_steps
FROM
(
    SELECT
        *,
        toUInt64(dateDiff(
            'second',
            min(window_start) OVER (
                PARTITION BY run_id, service_name, service_namespace, k8s_namespace
            ),
            window_start
        ) / 10) AS step_index
    FROM aiopslab.feature_tabular_v1
);

CREATE OR REPLACE VIEW aiopslab.evaluation_reference_v1 AS
SELECT
    run_id,
    phase_start AS window_start,
    phase_end AS window_end,
    condition,
    fault_type,
    affected_component,
    'experiment_control_interval_v1' AS label_assignment_basis
FROM aiopslab.experiment_conditions FINAL;

CREATE TABLE IF NOT EXISTS aiopslab.pattern_results_v1
(
    result_id String,
    feature_id String,
    sequence_id String,
    model_name LowCardinality(String),
    model_version String,
    feature_set_version LowCardinality(String),
    preprocessing_version String,
    pattern_score Nullable(Float64),
    decision LowCardinality(String),
    threshold_version String,
    explanation_json String,
    inference_time_ms Float64,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (model_name, model_version, created_at, result_id);

CREATE OR REPLACE VIEW aiopslab.signal_coverage_v2 AS
SELECT
    run_id,
    toStartOfMinute(window_start) AS minute,
    count() AS windows,
    1 - avg(missing_metrics) AS metric_coverage,
    1 - avg(missing_logs) AS log_coverage,
    1 - avg(missing_traces) AS trace_coverage,
    avg(identity_mismatch_suspected) AS identity_mismatch_ratio,
    sum(late_event_count) AS late_events,
    sum(duplicate_event_count) AS duplicate_events,
    sum(schema_error_count) AS schema_errors
FROM aiopslab.gold_service_window_v2_latest
GROUP BY run_id, minute;

CREATE OR REPLACE VIEW aiopslab.gold_quality_by_run_v2 AS
SELECT
    run_id,
    count() AS observed_windows,
    uniqExact(tuple(window_start, service_name, service_namespace, k8s_namespace)) AS logical_keys,
    uniqExact(service_name) AS services,
    avg(missing_metrics) AS missing_metrics_ratio,
    avg(missing_logs) AS missing_logs_ratio,
    avg(missing_traces) AS missing_traces_ratio,
    avg(identity_mismatch_suspected) AS identity_mismatch_ratio,
    sum(late_event_count) AS late_events,
    sum(duplicate_event_count) AS duplicate_events,
    sum(schema_error_count) AS schema_errors,
    countIf(cpu_usage_cores IS NOT NULL) / count() AS cpu_coverage,
    countIf(memory_working_set_bytes IS NOT NULL) / count() AS memory_coverage,
    countIf(request_rate IS NOT NULL) / count() AS request_rate_coverage
FROM aiopslab.gold_service_window_v2_latest
GROUP BY run_id;

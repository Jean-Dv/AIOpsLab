package org.aiopslab.etl;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class TelemetryEtlJob {
    private static final String GOLD_INSERT = """
            INSERT INTO aiopslab.gold_service_window_v2 (
              feature_id, run_id, scenario_id, window_start, window_end,
              service_name, service_namespace, k8s_namespace, pod_count, deployment_count,
              pod_names_json, deployment_names_json, metric_point_count, cpu_sample_count,
              cpu_utilization, cpu_usage_cores, cpu_sources_json,
              memory_sample_count, memory_working_set_bytes,
              memory_sources_json, log_count, log_error_count, log_error_ratio,
              severity_counts_json, event_template_counts_json, span_count, server_span_count,
              error_span_count, span_error_ratio, request_rate, request_rate_source,
              latency_sample_count, latency_p50_ms, latency_p95_ms, latency_p99_ms,
              dependency_edge_count, dependency_edges_json, span_kind_counts_json,
              missing_metrics, missing_logs, missing_traces, identity_mismatch_suspected,
              late_event_count, duplicate_event_count, schema_error_count,
              trace_id_present_count, span_id_present_count, service_name_present_count,
              k8s_namespace_present_count, row_present, source_references_json,
              gold_contract_version, normalizer_version, log_template_version, emitted_at, version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private TelemetryEtlJob() {}

    public static void main(String[] args) throws Exception {
        String kafka = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String defaultRun = env("AIOPSLAB_RUN_ID", "unassigned");
        String defaultScenario = env("AIOPSLAB_SCENARIO_ID", "unassigned");

        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        // Continuous snapshots can starve etcd on the shared Docker
        // filesystem, so keep a measured idle interval between non-concurrent
        // incremental checkpoints.
        execution.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        execution.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        execution.getCheckpointConfig().setCheckpointTimeout(300_000L);
        execution.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        execution.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("isolation.level", "read_committed");
        // Bound concurrent fetch buffers while still allowing Kafka to return a
        // larger first batch when a single OTLP export exceeds this soft limit.
        consumerProperties.setProperty("max.partition.fetch.bytes", "4194304");
        consumerProperties.setProperty("fetch.max.bytes", "12582912");

        KafkaSource<BronzeEnvelope> otlpSource = KafkaSource.<BronzeEnvelope>builder()
                .setBootstrapServers(kafka)
                .setTopics(List.of("otlp.metrics.v1", "otlp.logs.v1", "otlp.traces.v1"))
                .setGroupId("aiopslab-flink-telemetry-etl-v2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(consumerProperties)
                .setDeserializer(new KafkaEnvelopeDeserializer())
                .build();

        KafkaSource<String> controlSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafka)
                .setTopics("experiment.control.v1")
                .setGroupId("aiopslab-flink-experiment-control-v2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<BronzeEnvelope> raw = execution
                .fromSource(otlpSource, WatermarkStrategy.noWatermarks(), "OTLP Kafka source")
                .uid("otlp-kafka-source-v2");
        BroadcastStream<String> control = execution
                .fromSource(controlSource, WatermarkStrategy.noWatermarks(), "Experiment controls")
                .uid("experiment-control-source-v2")
                .broadcast(BronzeControlEnricher.CONTROL_STATE);

        DataStream<BronzeEnvelope> bronze = raw.connect(control)
                .process(new BronzeControlEnricher(defaultRun, defaultScenario))
                .name("Attach experiment reference intervals")
                .uid("bronze-control-enricher-v2");
        bronze.sinkTo(bronzeSink()).name("Bronze OTLP Parquet v2").uid("bronze-parquet-sink-v2");

        DataStream<TelemetryEvent> silver = bronze
                .flatMap(new OtlpNormalizer())
                .name("Normalize OTLP")
                .uid("otlp-normalizer-v2")
                .keyBy(event -> event.eventId)
                .process(new DeduplicateEvents())
                .name("Mark duplicates")
                .uid("silver-deduplicate-v2")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TelemetryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withIdleness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, previous) -> event.eventTime));
        silver.filter(event -> event.signal.equals("metrics"))
                .sinkTo(silverSink("metrics"))
                .name("Silver metrics Parquet v2").uid("silver-metrics-parquet-sink-v2");
        silver.filter(event -> event.signal.equals("logs"))
                .sinkTo(silverSink("logs"))
                .name("Silver logs Parquet v2").uid("silver-logs-parquet-sink-v2");
        silver.filter(event -> event.signal.equals("traces"))
                .sinkTo(silverSink("traces"))
                .name("Silver traces Parquet v2").uid("silver-traces-parquet-sink-v2");
        silver.filter(event -> event.schemaError)
                .sinkTo(silverSink("schema_errors"))
                .name("Silver schema errors Parquet v2").uid("silver-errors-parquet-sink-v2");

        DataStream<TelemetryEvent> serviceScoped = silver
                .filter(event -> !event.serviceName.isBlank()
                        && !event.serviceName.equals("unknown"))
                .name("Keep canonical service identities for Gold")
                .uid("gold-service-identity-filter-v2");

        DataStream<FeatureWindow> gold = serviceScoped
                .keyBy(event -> event.runId + "|" + event.componentKey())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .allowedLateness(Time.seconds(30))
                .process(new FeatureWindowFunction())
                .name("Build canonical service windows v2")
                .uid("gold-window-builder-v2");
        gold.sinkTo(clickHouseSink()).name("ClickHouse Gold v2").uid("gold-clickhouse-sink-v2");

        execution.execute("AIOpsLab multimodal telemetry ETL");
    }

    private static FileSink<BronzeEnvelope> bronzeSink() {
        return FileSink.forBulkFormat(
                        new Path("s3://aiopslab-bronze-v2/otlp"),
                        ParquetAvroWriters.forReflectRecord(BronzeEnvelope.class))
                .withBucketAssigner(new TelemetryBucketAssigner<>())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("bronze").withPartSuffix(".parquet").build())
                .build();
    }

    private static FileSink<TelemetryEvent> silverSink(String table) {
        return FileSink.forBulkFormat(
                        new Path("s3://aiopslab-silver-v2/" + table),
                        ParquetAvroWriters.forReflectRecord(TelemetryEvent.class))
                .withBucketAssigner(new TelemetryBucketAssigner<>())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("silver").withPartSuffix(".parquet").build())
                .build();
    }

    private static JdbcSink<FeatureWindow> clickHouseSink() {
        return JdbcSink.<FeatureWindow>builder()
                .withQueryStatement(GOLD_INSERT, (statement, row) -> {
            int i = 1;
            statement.setString(i++, row.featureId);
            statement.setString(i++, row.runId);
            statement.setString(i++, row.scenarioId);
            statement.setTimestamp(i++, new Timestamp(row.windowStart));
            statement.setTimestamp(i++, new Timestamp(row.windowEnd));
            statement.setString(i++, row.serviceName);
            statement.setString(i++, row.serviceNamespace);
            statement.setString(i++, row.k8sNamespace);
            statement.setLong(i++, row.podCount);
            statement.setLong(i++, row.deploymentCount);
            statement.setString(i++, row.podNamesJson);
            statement.setString(i++, row.deploymentNamesJson);
            statement.setLong(i++, row.metricPointCount);
            statement.setLong(i++, row.cpuSampleCount);
            setNullableDouble(statement, i++, row.cpuUtilization);
            setNullableDouble(statement, i++, row.cpuUsageCores);
            statement.setString(i++, row.cpuSourcesJson);
            statement.setLong(i++, row.memorySampleCount);
            setNullableDouble(statement, i++, row.memoryWorkingSetBytes);
            statement.setString(i++, row.memorySourcesJson);
            statement.setLong(i++, row.logCount);
            statement.setLong(i++, row.logErrorCount);
            setNullableDouble(statement, i++, row.logErrorRatio);
            statement.setString(i++, row.severityCountsJson);
            statement.setString(i++, row.eventTemplateCountsJson);
            statement.setLong(i++, row.spanCount);
            statement.setLong(i++, row.serverSpanCount);
            statement.setLong(i++, row.errorSpanCount);
            setNullableDouble(statement, i++, row.spanErrorRatio);
            setNullableDouble(statement, i++, row.requestRate);
            statement.setString(i++, row.requestRateSource);
            statement.setLong(i++, row.latencySampleCount);
            setNullableDouble(statement, i++, row.latencyP50Ms);
            setNullableDouble(statement, i++, row.latencyP95Ms);
            setNullableDouble(statement, i++, row.latencyP99Ms);
            statement.setLong(i++, row.dependencyEdgeCount);
            statement.setString(i++, row.dependencyEdgesJson);
            statement.setString(i++, row.spanKindCountsJson);
            statement.setInt(i++, row.missingMetrics ? 1 : 0);
            statement.setInt(i++, row.missingLogs ? 1 : 0);
            statement.setInt(i++, row.missingTraces ? 1 : 0);
            statement.setInt(i++, row.identityMismatchSuspected ? 1 : 0);
            statement.setLong(i++, row.lateEventCount);
            statement.setLong(i++, row.duplicateEventCount);
            statement.setLong(i++, row.schemaErrorCount);
            statement.setLong(i++, row.traceIdPresentCount);
            statement.setLong(i++, row.spanIdPresentCount);
            statement.setLong(i++, row.serviceNamePresentCount);
            statement.setLong(i++, row.k8sNamespacePresentCount);
            statement.setInt(i++, row.rowPresent ? 1 : 0);
            statement.setString(i++, row.sourceReferencesJson);
            statement.setString(i++, row.goldContractVersion);
            statement.setString(i++, row.normalizerVersion);
            statement.setString(i++, row.logTemplateVersion);
            statement.setTimestamp(i++, new Timestamp(row.emittedAt));
            statement.setLong(i, row.version);
        })
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        // ClickHouse's JDBC driver builds the complete VALUES string in
                        // heap. Keep batches deliberately small because Gold rows include
                        // JSON maps whose size varies with service cardinality.
                        .withBatchSize(Integer.parseInt(env("CLICKHOUSE_BATCH_SIZE", "20")))
                        .withBatchIntervalMs(500)
                        .withMaxRetries(3)
                        .build())
                .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(env("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/aiopslab"))
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .withUsername(env("CLICKHOUSE_USER", "aiopslab"))
                        .withPassword(env("CLICKHOUSE_PASSWORD", "aiopslab-local"))
                        .build());
    }

    private static void setNullableDouble(java.sql.PreparedStatement statement, int index,
            Double value) throws java.sql.SQLException {
        if (value == null || !Double.isFinite(value)) statement.setNull(index, Types.DOUBLE);
        else statement.setDouble(index, value);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

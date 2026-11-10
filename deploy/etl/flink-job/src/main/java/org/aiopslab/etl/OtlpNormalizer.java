package org.aiopslab.etl;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/** Expands OTLP export requests into one stable Silver event per data point. */
public class OtlpNormalizer implements FlatMapFunction<BronzeEnvelope, TelemetryEvent> {
    private static final long LATE_THRESHOLD_MS = 30_000L;

    @Override
    public void flatMap(BronzeEnvelope envelope, Collector<TelemetryEvent> output) {
        try {
            switch (envelope.signal) {
                case "metrics" -> normalizeMetrics(envelope, output);
                case "logs" -> normalizeLogs(envelope, output);
                case "traces" -> normalizeTraces(envelope, output);
                default -> output.collect(schemaError(envelope, "unknown_signal"));
            }
        } catch (Exception exception) {
            output.collect(schemaError(envelope, exception.getClass().getSimpleName()));
        }
    }

    private void normalizeMetrics(BronzeEnvelope envelope, Collector<TelemetryEvent> output)
            throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(envelope.payload);
        int index = 0;
        for (var resourceMetrics : request.getResourceMetricsList()) {
            Map<String, Object> resource = TelemetrySupport.attributes(
                    resourceMetrics.getResource().getAttributesList());
            for (var scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                for (Metric metric : scopeMetrics.getMetricsList()) {
                    for (NumberDataPoint point : metric.getGauge().getDataPointsList()) {
                        output.collect(metricEvent(envelope, resource, metric.getName(),
                                metric.getUnit(), "gauge", number(point), point.getTimeUnixNano(),
                                point.getAttributesList(), index++));
                    }
                    for (NumberDataPoint point : metric.getSum().getDataPointsList()) {
                        output.collect(metricEvent(envelope, resource, metric.getName(),
                                metric.getUnit(), "sum", number(point), point.getTimeUnixNano(),
                                point.getAttributesList(), index++));
                    }
                    for (HistogramDataPoint point : metric.getHistogram().getDataPointsList()) {
                        double value = point.getCount() == 0 ? Double.NaN : point.getSum() / point.getCount();
                        output.collect(metricEvent(envelope, resource, metric.getName(),
                                metric.getUnit(), "histogram_mean", value, point.getTimeUnixNano(),
                                point.getAttributesList(), index++));
                    }
                    for (ExponentialHistogramDataPoint point :
                            metric.getExponentialHistogram().getDataPointsList()) {
                        double value = point.getCount() == 0 ? Double.NaN : point.getSum() / point.getCount();
                        output.collect(metricEvent(envelope, resource, metric.getName(),
                                metric.getUnit(), "exponential_histogram_mean", value,
                                point.getTimeUnixNano(), point.getAttributesList(), index++));
                    }
                    for (SummaryDataPoint point : metric.getSummary().getDataPointsList()) {
                        for (var quantile : point.getQuantileValuesList()) {
                            String name = metric.getName() + ".quantile." + quantile.getQuantile();
                            output.collect(metricEvent(envelope, resource, name, metric.getUnit(),
                                    "summary_quantile", quantile.getValue(), point.getTimeUnixNano(),
                                    point.getAttributesList(), index++));
                        }
                    }
                }
            }
        }
    }

    private void normalizeLogs(BronzeEnvelope envelope, Collector<TelemetryEvent> output)
            throws Exception {
        ExportLogsServiceRequest request = ExportLogsServiceRequest.parseFrom(envelope.payload);
        int index = 0;
        for (var resourceLogs : request.getResourceLogsList()) {
            Map<String, Object> resource = TelemetrySupport.attributes(
                    resourceLogs.getResource().getAttributesList());
            for (var scopeLogs : resourceLogs.getScopeLogsList()) {
                for (LogRecord record : scopeLogs.getLogRecordsList()) {
                    TelemetryEvent event = base(envelope, resource, record.getTimeUnixNano(), index++);
                    event.signal = "logs";
                    event.observedTime = TelemetrySupport.nanosToMillis(
                            record.getObservedTimeUnixNano(), event.receivedAt);
                    event.traceId = TelemetrySupport.hex(record.getTraceId());
                    event.spanId = TelemetrySupport.hex(record.getSpanId());
                    event.body = String.valueOf(TelemetrySupport.anyValue(record.getBody()));
                    event.name = TelemetrySupport.logTemplate(event.body);
                    event.severity = record.getSeverityText();
                    event.severityNumber = record.getSeverityNumberValue();
                    event.error = event.severityNumber >= 17;
                    event.attributesJson = TelemetrySupport.json(
                            TelemetrySupport.attributes(record.getAttributesList()));
                    output.collect(event);
                }
            }
        }
    }

    private void normalizeTraces(BronzeEnvelope envelope, Collector<TelemetryEvent> output)
            throws Exception {
        ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(envelope.payload);
        int index = 0;
        for (var resourceSpans : request.getResourceSpansList()) {
            Map<String, Object> resource = TelemetrySupport.attributes(
                    resourceSpans.getResource().getAttributesList());
            for (var scopeSpans : resourceSpans.getScopeSpansList()) {
                for (Span span : scopeSpans.getSpansList()) {
                    TelemetryEvent event = base(envelope, resource, span.getStartTimeUnixNano(), index++);
                    Map<String, Object> spanAttributes = TelemetrySupport.attributes(span.getAttributesList());
                    event.signal = "traces";
                    event.traceId = TelemetrySupport.hex(span.getTraceId());
                    event.spanId = TelemetrySupport.hex(span.getSpanId());
                    event.parentSpanId = TelemetrySupport.hex(span.getParentSpanId());
                    event.name = span.getName();
                    event.spanKind = span.getKind().name();
                    event.body = TelemetrySupport.attribute(spanAttributes,
                            "peer.service", "server.address", "rpc.service", "network.peer.address");
                    event.durationMs = Math.max(0,
                            (span.getEndTimeUnixNano() - span.getStartTimeUnixNano()) / 1_000_000.0);
                    event.error = span.getStatus().getCodeValue() == 2;
                    event.attributesJson = TelemetrySupport.json(spanAttributes);
                    output.collect(event);
                }
            }
        }
    }

    private TelemetryEvent metricEvent(BronzeEnvelope envelope, Map<String, Object> resource,
            String name, String unit, String instrumentType, double value, long timestamp,
            java.util.List<io.opentelemetry.proto.common.v1.KeyValue> attrs, int index) {
        TelemetryEvent event = base(envelope, resource, timestamp, index);
        event.signal = "metrics";
        event.name = name;
        event.unit = unit;
        event.instrumentType = instrumentType;
        event.numericValue = value;
        event.attributesJson = TelemetrySupport.json(TelemetrySupport.attributes(attrs));
        return event;
    }

    private TelemetryEvent base(BronzeEnvelope envelope, Map<String, Object> resource,
            long timestampNanos, int index) {
        TelemetryEvent event = new TelemetryEvent();
        event.eventId = TelemetrySupport.sha256(envelope.topic + ":" + envelope.partition + ":"
                + envelope.offset + ":" + index);
        event.eventTime = TelemetrySupport.nanosToMillis(timestampNanos, envelope.kafkaTimestamp);
        event.receivedAt = envelope.receivedAt;
        event.observedTime = envelope.receivedAt;
        event.sourceTopic = envelope.topic;
        event.sourcePartition = envelope.partition;
        event.sourceOffset = envelope.offset;
        event.runId = envelope.runId;
        event.scenarioId = envelope.scenarioId;
        event.condition = envelope.condition;
        event.faultType = envelope.faultType;
        event.affectedComponent = envelope.affectedComponent;
        event.serviceName = canonicalServiceName(resource);
        event.serviceNamespace = valueOr(resource, "", "service.namespace");
        event.k8sNamespace = valueOr(resource, "", "k8s.namespace.name");
        event.podName = valueOr(resource, "", "k8s.pod.name");
        event.deploymentName = valueOr(resource, "", "k8s.deployment.name");
        event.resourceAttributesJson = TelemetrySupport.json(resource);
        event.late = event.receivedAt - event.eventTime > LATE_THRESHOLD_MS;
        return event;
    }

    private static String valueOr(Map<String, Object> values, String fallback, String... keys) {
        String value = TelemetrySupport.attribute(values, keys);
        return value.isBlank() ? fallback : value;
    }

    /**
     * Resolve a service identity without guessing from pod names or merging
     * infrastructure scopes. service.name remains authoritative. A Kubernetes
     * workload controller is used only when the signal has no service.name.
     */
    static String canonicalServiceName(Map<String, Object> resource) {
        String declared = valueOr(resource, "", "service.name");
        if (isKnownIdentity(declared)) return declared;
        String workload = valueOr(resource, "",
                "k8s.deployment.name",
                "k8s.statefulset.name",
                "k8s.daemonset.name",
                "k8s.cronjob.name",
                "k8s.job.name");
        return isKnownIdentity(workload) ? workload : "unknown";
    }

    private static boolean isKnownIdentity(String value) {
        return value != null && !value.isBlank()
                && !value.equalsIgnoreCase("unknown")
                && !value.toLowerCase(java.util.Locale.ROOT).startsWith("unknown_service");
    }

    private static double number(NumberDataPoint point) {
        return switch (point.getValueCase()) {
            case AS_DOUBLE -> point.getAsDouble();
            case AS_INT -> point.getAsInt();
            case VALUE_NOT_SET -> Double.NaN;
        };
    }

    private TelemetryEvent schemaError(BronzeEnvelope envelope, String reason) {
        TelemetryEvent event = new TelemetryEvent();
        event.eventId = TelemetrySupport.sha256(envelope.topic + ":" + envelope.partition + ":"
                + envelope.offset + ":schema-error");
        event.signal = "schema_error";
        event.eventTime = envelope.kafkaTimestamp > 0 ? envelope.kafkaTimestamp : envelope.receivedAt;
        event.receivedAt = envelope.receivedAt;
        event.sourceTopic = envelope.topic;
        event.sourcePartition = envelope.partition;
        event.sourceOffset = envelope.offset;
        event.runId = envelope.runId;
        event.scenarioId = envelope.scenarioId;
        event.condition = envelope.condition;
        event.name = reason;
        event.schemaError = true;
        return event;
    }
}

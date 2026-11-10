package org.aiopslab.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Builds the service/window Gold v2 contract without evaluation labels. */
public class FeatureWindowFunction
        extends ProcessWindowFunction<TelemetryEvent, FeatureWindow, String, TimeWindow> {
    private static final Set<String> CPU_USAGE_CORES_METRICS = Set.of(
            "container.cpu.usage");
    private static final Set<String> MEMORY_WORKING_SET_METRICS = Set.of(
            "container.memory.working_set");

    @Override
    public void process(String key, Context context, Iterable<TelemetryEvent> input,
            Collector<FeatureWindow> output) {
        output.collect(build(context.window().getStart(), context.window().getEnd(), key, input));
    }

    static FeatureWindow build(long windowStart, long windowEnd, String key,
            Iterable<TelemetryEvent> input) {
        FeatureWindow result = new FeatureWindow();
        result.windowStart = windowStart;
        result.windowEnd = windowEnd;
        result.emittedAt = System.currentTimeMillis();
        result.version = result.emittedAt;

        List<Double> serverDurations = new ArrayList<>();
        List<Double> cpu = new ArrayList<>();
        List<Double> memory = new ArrayList<>();
        Map<String, Long> severities = new HashMap<>();
        Map<String, Long> templates = new HashMap<>();
        Map<String, Long> spanKinds = new HashMap<>();
        Set<String> edges = new HashSet<>();
        Set<String> pods = new HashSet<>();
        Set<String> deployments = new HashSet<>();
        Set<String> cpuSources = new HashSet<>();
        Set<String> memorySources = new HashSet<>();
        Set<String> podServiceNamespaces = new TreeSet<>();
        Set<String> fallbackServiceNamespaces = new TreeSet<>();
        Map<String, Set<String>> signalPods = new HashMap<>();
        Map<String, long[]> sourceOffsets = new HashMap<>();

        for (TelemetryEvent event : input) {
            if (result.serviceName.equals("unknown")) {
                result.runId = event.runId;
                result.scenarioId = event.scenarioId;
                result.serviceName = event.serviceName;
                result.k8sNamespace = event.k8sNamespace;
            }
            if (!event.serviceNamespace.isBlank()) {
                fallbackServiceNamespaces.add(event.serviceNamespace);
                if (!event.podName.isBlank()) {
                    podServiceNamespaces.add(event.serviceNamespace);
                }
            }
            if (!event.podName.isBlank()) {
                pods.add(event.podName);
                signalPods.computeIfAbsent(event.signal, ignored -> new HashSet<>())
                        .add(event.podName);
            }
            if (!event.deploymentName.isBlank()) deployments.add(event.deploymentName);
            if (!event.traceId.isBlank()) result.traceIdPresentCount++;
            if (!event.spanId.isBlank()) result.spanIdPresentCount++;
            if (!event.serviceName.isBlank() && !event.serviceName.equals("unknown")) {
                result.serviceNamePresentCount++;
            }
            if (!event.k8sNamespace.isBlank()) result.k8sNamespacePresentCount++;
            if (event.late) result.lateEventCount++;
            String sourceKey = event.sourceTopic + ":" + event.sourcePartition;
            sourceOffsets.compute(sourceKey, (ignored, range) -> {
                if (range == null) return new long[] {event.sourceOffset, event.sourceOffset};
                range[0] = Math.min(range[0], event.sourceOffset);
                range[1] = Math.max(range[1], event.sourceOffset);
                return range;
            });
            if (event.duplicate) {
                result.duplicateEventCount++;
                continue;
            }
            if (event.schemaError) {
                result.schemaErrorCount++;
                continue;
            }

            switch (event.signal) {
                case "metrics" -> addMetric(result, event, cpu, memory, cpuSources, memorySources);
                case "logs" -> addLog(result, event, severities, templates);
                case "traces" -> addSpan(result, event, serverDurations, spanKinds, edges);
                default -> result.schemaErrorCount++;
            }
        }

        double seconds = Math.max(1.0, (result.windowEnd - result.windowStart) / 1000.0);
        result.requestRate = result.serverSpanCount == 0 ? null : result.serverSpanCount / seconds;
        result.spanErrorRatio = ratio(result.errorSpanCount, result.spanCount);
        result.logErrorRatio = ratio(result.logErrorCount, result.logCount);
        result.latencySampleCount = serverDurations.size();
        result.latencyP50Ms = percentileOrNull(serverDurations, 0.50);
        result.latencyP95Ms = percentileOrNull(serverDurations, 0.95);
        result.latencyP99Ms = percentileOrNull(serverDurations, 0.99);
        result.cpuSampleCount = cpu.size();
        result.cpuUsageCores = average(cpu);
        result.memorySampleCount = memory.size();
        result.memoryWorkingSetBytes = average(memory);
        result.severityCountsJson = TelemetrySupport.json(severities);
        result.eventTemplateCountsJson = TelemetrySupport.json(templates);
        result.spanKindCountsJson = TelemetrySupport.json(spanKinds);
        result.dependencyEdgeCount = edges.size();
        result.dependencyEdgesJson = TelemetrySupport.json(edges);
        result.podCount = pods.size();
        result.deploymentCount = deployments.size();
        result.podNamesJson = TelemetrySupport.json(pods);
        result.deploymentNamesJson = TelemetrySupport.json(deployments);
        result.cpuSourcesJson = TelemetrySupport.json(cpuSources);
        result.memorySourcesJson = TelemetrySupport.json(memorySources);
        result.serviceNamespace = firstOrEmpty(
                podServiceNamespaces.isEmpty()
                        ? fallbackServiceNamespaces
                        : podServiceNamespaces);
        result.sourceReferencesJson = TelemetrySupport.json(sourceOffsets);
        result.missingMetrics = result.metricPointCount == 0;
        result.missingLogs = result.logCount == 0;
        result.missingTraces = result.spanCount == 0;
        result.identityMismatchSuspected = hasDisjointSignalPods(signalPods);
        result.featureId = TelemetrySupport.sha256(result.goldContractVersion + ":"
                + result.runId + ":" + result.windowStart + ":" + key);
        return result;
    }

    private static void addMetric(FeatureWindow result, TelemetryEvent event, List<Double> cpu,
            List<Double> memory, Set<String> cpuSources, Set<String> memorySources) {
        result.metricPointCount++;
        if (!Double.isFinite(event.numericValue)) return;
        String name = event.name.toLowerCase(Locale.ROOT);
        String unit = event.unit == null ? "" : event.unit;
        if (CPU_USAGE_CORES_METRICS.contains(name) && unit.equals("{cpu}")
                && event.instrumentType.equals("gauge") && event.numericValue >= 0) {
            cpu.add(event.numericValue);
            cpuSources.add(event.name + "[" + unit + ";" + event.instrumentType + "]");
        }
        if (MEMORY_WORKING_SET_METRICS.contains(name)
                && unit.equals("By") && event.instrumentType.equals("gauge")
                && event.numericValue >= 0) {
            memory.add(event.numericValue);
            memorySources.add(event.name + "[" + unit + ";" + event.instrumentType + "]");
        }
    }

    private static void addLog(FeatureWindow result, TelemetryEvent event,
            Map<String, Long> severities, Map<String, Long> templates) {
        result.logCount++;
        String severity = event.severity.isBlank() ? "UNSPECIFIED" : event.severity;
        severities.merge(severity, 1L, Long::sum);
        templates.merge(event.name, 1L, Long::sum);
        if (event.error) result.logErrorCount++;
    }

    private static void addSpan(FeatureWindow result, TelemetryEvent event,
            List<Double> serverDurations, Map<String, Long> spanKinds, Set<String> edges) {
        result.spanCount++;
        String kind = event.spanKind.isBlank() ? "SPAN_KIND_UNSPECIFIED" : event.spanKind;
        spanKinds.merge(kind, 1L, Long::sum);
        if (event.error) result.errorSpanCount++;
        if (kind.equals("SPAN_KIND_SERVER")) {
            result.serverSpanCount++;
            if (Double.isFinite(event.durationMs)) serverDurations.add(event.durationMs);
        }
        if (!event.body.isBlank()) edges.add(event.serviceName + "->" + event.body);
    }

    private static Double ratio(long numerator, long denominator) {
        return denominator == 0 ? null : (double) numerator / denominator;
    }

    private static Double average(List<Double> values) {
        return values.isEmpty() ? null
                : values.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
    }

    private static String firstOrEmpty(Set<String> values) {
        return values.isEmpty() ? "" : values.iterator().next();
    }

    private static Double percentileOrNull(List<Double> values, double percentile) {
        return values.isEmpty() ? null : TelemetrySupport.percentile(values, percentile);
    }

    static boolean hasDisjointSignalPods(Map<String, Set<String>> signalPods) {
        List<Set<String>> populated = signalPods.values().stream()
                .filter(values -> !values.isEmpty()).toList();
        if (populated.size() < 2) return false;
        Set<String> common = new HashSet<>(populated.get(0));
        for (int i = 1; i < populated.size(); i++) common.retainAll(populated.get(i));
        return common.isEmpty();
    }
}

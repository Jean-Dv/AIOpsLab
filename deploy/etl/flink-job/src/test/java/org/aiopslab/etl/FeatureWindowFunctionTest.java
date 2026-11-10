package org.aiopslab.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FeatureWindowFunctionTest {
    @Test
    void separatesLogAndSpanErrorsAndUsesOnlyServerSpansForLatencyAndRate() {
        TelemetryEvent log = event("logs", "checkout", "pod-a");
        log.error = true;
        log.severity = "ERROR";
        log.name = "request failed";

        TelemetryEvent server = event("traces", "checkout", "pod-a");
        server.spanKind = "SPAN_KIND_SERVER";
        server.durationMs = 120.0;
        server.error = true;

        TelemetryEvent client = event("traces", "checkout", "pod-a");
        client.spanKind = "SPAN_KIND_CLIENT";
        client.durationMs = 999.0;

        FeatureWindow result = FeatureWindowFunction.build(
                0, 30_000, "run-1|checkout||astronomy-shop", List.of(log, server, client));

        assertEquals(1, result.logErrorCount);
        assertEquals(1.0, result.logErrorRatio);
        assertEquals(1, result.errorSpanCount);
        assertEquals(0.5, result.spanErrorRatio);
        assertEquals(1.0 / 30.0, result.requestRate);
        assertEquals(120.0, result.latencyP95Ms);
        assertEquals(1, result.latencySampleCount);
        assertFalse(result.featureId.isBlank());
    }

    @Test
    void keepsMissingValuesNullAndAcceptsOnlyDeclaredMetricSemantics() {
        TelemetryEvent cpu = event("metrics", "frontend", "pod-a");
        cpu.name = "container.cpu.usage";
        cpu.unit = "{cpu}";
        cpu.instrumentType = "gauge";
        cpu.numericValue = 0.25;

        TelemetryEvent ambiguousMemory = event("metrics", "frontend", "pod-a");
        ambiguousMemory.name = "jvm.memory.used";
        ambiguousMemory.unit = "By";
        ambiguousMemory.numericValue = 1024;

        FeatureWindow result = FeatureWindowFunction.build(
                0, 30_000, "run-1|frontend||astronomy-shop", List.of(cpu, ambiguousMemory));

        assertEquals(0.25, result.cpuUsageCores);
        assertNull(result.cpuUtilization);
        assertEquals(1, result.cpuSampleCount);
        assertNull(result.memoryWorkingSetBytes);
        assertEquals(0, result.memorySampleCount);
        assertNull(result.requestRate);
        assertTrue(result.missingLogs);
        assertTrue(result.missingTraces);
    }

    @Test
    void rejectsProcessAndCumulativeMetricsThatWouldMixSemantics() {
        TelemetryEvent processCpu = event("metrics", "frontend", "pod-a");
        processCpu.name = "process.cpu.utilization";
        processCpu.unit = "1";
        processCpu.instrumentType = "gauge";
        processCpu.numericValue = 0.25;

        TelemetryEvent cpuTime = event("metrics", "frontend", "pod-a");
        cpuTime.name = "container.cpu.time";
        cpuTime.unit = "s";
        cpuTime.instrumentType = "sum";
        cpuTime.numericValue = 20;

        TelemetryEvent workingSet = event("metrics", "frontend", "pod-a");
        workingSet.name = "container.memory.working_set";
        workingSet.unit = "By";
        workingSet.instrumentType = "gauge";
        workingSet.numericValue = 4096;

        FeatureWindow result = FeatureWindowFunction.build(
                0, 30_000, "run-1|frontend||astronomy-shop",
                List.of(processCpu, cpuTime, workingSet));

        assertNull(result.cpuUtilization);
        assertNull(result.cpuUsageCores);
        assertEquals(0, result.cpuSampleCount);
        assertEquals(4096.0, result.memoryWorkingSetBytes);
        assertEquals(1, result.memorySampleCount);
    }

    @Test
    void flagsDisjointPodIdentityAcrossSignals() {
        assertTrue(FeatureWindowFunction.hasDisjointSignalPods(Map.of(
                "metrics", Set.of("pod-a"), "logs", Set.of("pod-b"))));
        assertFalse(FeatureWindowFunction.hasDisjointSignalPods(Map.of(
                "metrics", Set.of("pod-a"), "logs", Set.of("pod-a", "pod-b"))));
    }

    @Test
    void canonicalKeyIgnoresDescriptiveServiceNamespaceAndPrefersPodContext() {
        TelemetryEvent application = event("metrics", "checkout", "pod-a");
        application.serviceNamespace = "opentelemetry-demo";
        TelemetryEvent cluster = event("metrics", "checkout", "");
        cluster.serviceNamespace = "astronomy-shop";

        assertEquals(application.componentKey(), cluster.componentKey());

        FeatureWindow result = FeatureWindowFunction.build(
                0, 30_000, "run-1|checkout|astronomy-shop", List.of(cluster, application));

        assertEquals("opentelemetry-demo", result.serviceNamespace);
        assertEquals("astronomy-shop", result.k8sNamespace);
        assertEquals(2, result.metricPointCount);
    }

    private static TelemetryEvent event(String signal, String service, String pod) {
        TelemetryEvent event = new TelemetryEvent();
        event.signal = signal;
        event.runId = "run-1";
        event.scenarioId = "scenario-1";
        event.serviceName = service;
        event.k8sNamespace = "astronomy-shop";
        event.podName = pod;
        event.sourceTopic = "otlp." + signal + ".v1";
        event.sourcePartition = 0;
        event.sourceOffset = 1;
        return event;
    }
}

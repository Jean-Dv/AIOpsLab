package org.aiopslab.etl;

import java.io.Serializable;

/** Canonical Gold v2 observation. Experimental labels live in a separate contract. */
public class FeatureWindow implements Serializable {
    public String featureId = "";
    public String runId = "unassigned";
    public String scenarioId = "unassigned";
    public long windowStart;
    public long windowEnd;
    public String serviceName = "unknown";
    public String serviceNamespace = "";
    public String k8sNamespace = "";
    public long podCount;
    public long deploymentCount;
    public String podNamesJson = "[]";
    public String deploymentNamesJson = "[]";

    public long metricPointCount;
    public long cpuSampleCount;
    public Double cpuUtilization;
    public Double cpuUsageCores;
    public String cpuSourcesJson = "[]";
    public long memorySampleCount;
    public Double memoryWorkingSetBytes;
    public String memorySourcesJson = "[]";

    public long logCount;
    public long logErrorCount;
    public Double logErrorRatio;
    public String severityCountsJson = "{}";
    public String eventTemplateCountsJson = "{}";

    public long spanCount;
    public long serverSpanCount;
    public long errorSpanCount;
    public Double spanErrorRatio;
    public Double requestRate;
    public String requestRateSource = "server_spans";
    public long latencySampleCount;
    public Double latencyP50Ms;
    public Double latencyP95Ms;
    public Double latencyP99Ms;
    public long dependencyEdgeCount;
    public String dependencyEdgesJson = "[]";
    public String spanKindCountsJson = "{}";

    public boolean missingMetrics;
    public boolean missingLogs;
    public boolean missingTraces;
    public boolean identityMismatchSuspected;
    public long lateEventCount;
    public long duplicateEventCount;
    public long schemaErrorCount;
    public long traceIdPresentCount;
    public long spanIdPresentCount;
    public long serviceNamePresentCount;
    public long k8sNamespacePresentCount;
    public boolean rowPresent = true;

    public String sourceReferencesJson = "{}";
    public String goldContractVersion = "gold_service_window_v2.2";
    public String normalizerVersion = "otlp_normalizer_v2.1";
    public String logTemplateVersion = "log_template_v1";
    public long emittedAt;
    public long version;

    public FeatureWindow() {}
}

package org.aiopslab.etl;

import java.io.Serializable;

/** Engine-independent Silver contract shared by metrics, logs and traces. */
public class TelemetryEvent implements Serializable {
    public String eventId = "";
    public String signal = "";
    public long eventTime = 0;
    public long observedTime = 0;
    public long receivedAt = 0;
    public String sourceTopic = "";
    public int sourcePartition = -1;
    public long sourceOffset = -1;
    public String runId = "unassigned";
    public String scenarioId = "unassigned";
    public String condition = "unassigned";
    public String faultType = "";
    public String affectedComponent = "";

    public String serviceName = "unknown";
    public String serviceNamespace = "";
    public String k8sNamespace = "";
    public String podName = "";
    public String deploymentName = "";
    public String traceId = "";
    public String spanId = "";
    public String parentSpanId = "";
    public String name = "";
    public String unit = "";
    public String instrumentType = "";
    public String spanKind = "";
    public String body = "";
    public String severity = "";
    public int severityNumber = 0;
    public double numericValue = Double.NaN;
    public double durationMs = Double.NaN;
    public boolean error = false;
    public String attributesJson = "{}";
    public String resourceAttributesJson = "{}";
    public boolean late = false;
    public boolean duplicate = false;
    public boolean schemaError = false;

    public TelemetryEvent() {}

    public String componentKey() {
        // Gold v2 is intentionally service scoped. Pod/deployment identities are
        // retained as coverage context because they are not consistently present
        // in all three OTLP signals. service.namespace is descriptive context,
        // not part of the canonical service/window identity: receivers can assign
        // a different value to the same Kubernetes workload.
        return String.join("|", serviceName, k8sNamespace);
    }
}

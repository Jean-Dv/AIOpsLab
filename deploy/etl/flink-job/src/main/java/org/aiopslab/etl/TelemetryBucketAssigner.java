package org.aiopslab.etl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class TelemetryBucketAssigner<T> implements BucketAssigner<T, String> {
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneOffset.UTC);

    @Override
    public String getBucketId(T element, Context context) {
        long timestamp;
        String runId;
        String signal;
        if (element instanceof BronzeEnvelope bronze) {
            timestamp = bronze.kafkaTimestamp > 0 ? bronze.kafkaTimestamp : bronze.receivedAt;
            runId = bronze.runId;
            signal = bronze.signal;
        } else if (element instanceof TelemetryEvent silver) {
            timestamp = silver.eventTime;
            runId = silver.runId;
            signal = silver.signal;
        } else {
            timestamp = System.currentTimeMillis();
            runId = "unassigned";
            signal = "unknown";
        }
        return "signal=" + safe(signal) + "/date=" + DAY.format(Instant.ofEpochMilli(timestamp))
                + "/run_id=" + safe(runId);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    private static String safe(String value) {
        return value == null || value.isBlank() ? "unassigned"
                : value.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}

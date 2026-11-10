package org.aiopslab.etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

final class TelemetrySupport {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern UUID = Pattern.compile("(?i)\\b[0-9a-f]{8}-[0-9a-f-]{27,36}\\b");
    private static final Pattern NUMBER = Pattern.compile("\\b\\d+(?:\\.\\d+)?\\b");
    private static final Pattern SPACE = Pattern.compile("\\s+");

    private TelemetrySupport() {}

    static Map<String, Object> attributes(List<KeyValue> values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (KeyValue entry : values) {
            result.put(entry.getKey(), anyValue(entry.getValue()));
        }
        return result;
    }

    static Object anyValue(AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case INT_VALUE -> value.getIntValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BYTES_VALUE -> hex(value.getBytesValue());
            case ARRAY_VALUE -> {
                List<Object> array = new ArrayList<>();
                value.getArrayValue().getValuesList().forEach(item -> array.add(anyValue(item)));
                yield array;
            }
            case KVLIST_VALUE -> attributes(value.getKvlistValue().getValuesList());
            case VALUE_NOT_SET -> null;
        };
    }

    static String json(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException ignored) {
            return "{}";
        }
    }

    static String attribute(Map<String, Object> attributes, String... keys) {
        for (String key : keys) {
            Object value = attributes.get(key);
            if (value != null && !value.toString().isBlank()) {
                return value.toString();
            }
        }
        return "";
    }

    static String hex(ByteString value) {
        StringBuilder result = new StringBuilder(value.size() * 2);
        for (byte item : value.toByteArray()) {
            result.append(String.format(Locale.ROOT, "%02x", item & 0xff));
        }
        return result.toString();
    }

    static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte item : digest) {
                result.append(String.format(Locale.ROOT, "%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static long nanosToMillis(long nanos, long fallback) {
        return nanos > 0 ? nanos / 1_000_000L : fallback;
    }

    static String logTemplate(String body) {
        if (body == null || body.isBlank()) {
            return "<empty>";
        }
        String normalized = UUID.matcher(body).replaceAll("<uuid>");
        normalized = NUMBER.matcher(normalized).replaceAll("<num>");
        normalized = SPACE.matcher(normalized).replaceAll(" ").trim();
        return normalized.length() <= 240 ? normalized : normalized.substring(0, 240);
    }

    static double percentile(List<Double> values, double percentile) {
        if (values.isEmpty()) {
            return Double.NaN;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        double position = percentile * (sorted.size() - 1);
        int lower = (int) Math.floor(position);
        int upper = (int) Math.ceil(position);
        if (lower == upper) {
            return sorted.get(lower);
        }
        double weight = position - lower;
        return sorted.get(lower) * (1.0 - weight) + sorted.get(upper) * weight;
    }
}

package org.aiopslab.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class TelemetrySupportTest {
    @Test
    void percentileInterpolatesAndDoesNotMutateInput() {
        List<Double> values = List.of(40.0, 10.0, 30.0, 20.0);
        assertEquals(25.0, TelemetrySupport.percentile(values, 0.50));
        assertEquals(38.5, TelemetrySupport.percentile(values, 0.95));
        assertEquals(List.of(40.0, 10.0, 30.0, 20.0), values);
    }

    @Test
    void logTemplateRemovesIdentifiersAndNumbers() {
        assertEquals(
                "order <uuid> failed after <num> ms",
                TelemetrySupport.logTemplate(
                        "order 123e4567-e89b-12d3-a456-426614174000 failed after 750 ms"));
    }
}

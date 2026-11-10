package org.aiopslab.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class BronzeControlEnricherTest {
    @Test
    void acceptsAControlEventWithPhaseId() {
        ControlEvent event = BronzeControlEnricher.parseControlEvent(
                "{\"phase_id\":\"phase-1\",\"run_id\":\"run-1\"}");

        assertEquals("phase-1", event.phase_id);
        assertEquals("run-1", event.run_id);
    }

    @Test
    void ignoresAControlEventWithoutPhaseId() {
        assertNull(BronzeControlEnricher.parseControlEvent("{\"run_id\":\"run-1\"}"));
    }

    @Test
    void ignoresMalformedJson() {
        assertNull(BronzeControlEnricher.parseControlEvent("not-json"));
    }
}

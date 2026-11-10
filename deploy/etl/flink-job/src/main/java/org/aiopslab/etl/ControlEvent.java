package org.aiopslab.etl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;

/** A reference interval. It labels evaluation data but is never a model input. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ControlEvent implements Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String run_id = "unassigned";
    public String scenario_id = "unassigned";
    public String phase_id = "";
    public String condition = "unassigned";
    public String fault_type = "";
    public String affected_component = "";
    public long phase_start_ms = 0;
    public Long phase_end_ms;

    public ControlEvent() {}

    public static ControlEvent fromJson(String value) throws Exception {
        return MAPPER.readValue(value, ControlEvent.class);
    }

    public boolean contains(long timestamp) {
        return timestamp >= phase_start_ms && (phase_end_ms == null || timestamp < phase_end_ms);
    }
}

package org.aiopslab.etl;

import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BronzeControlEnricher
        extends BroadcastProcessFunction<BronzeEnvelope, String, BronzeEnvelope> {
    private static final Logger LOG = LoggerFactory.getLogger(BronzeControlEnricher.class);

    public static final MapStateDescriptor<String, ControlEvent> CONTROL_STATE =
            new MapStateDescriptor<>("experiment-reference-intervals", String.class, ControlEvent.class);

    private final String defaultRunId;
    private final String defaultScenarioId;

    public BronzeControlEnricher(String defaultRunId, String defaultScenarioId) {
        this.defaultRunId = defaultRunId;
        this.defaultScenarioId = defaultScenarioId;
    }

    @Override
    public void processBroadcastElement(String value, Context context,
            Collector<BronzeEnvelope> output) throws Exception {
        ControlEvent event = parseControlEvent(value);
        if (event == null) {
            return;
        }
        context.getBroadcastState(CONTROL_STATE).put(event.phase_id, event);
    }

    static ControlEvent parseControlEvent(String value) {
        try {
            ControlEvent event = ControlEvent.fromJson(value);
            if (event.phase_id == null || event.phase_id.isBlank()) {
                LOG.warn("Ignoring experiment.control.v1 message without phase_id.");
                return null;
            }
            return event;
        } catch (Exception error) {
            LOG.warn("Ignoring malformed experiment.control.v1 message: {}", error.getMessage());
            return null;
        }
    }

    @Override
    public void processElement(BronzeEnvelope envelope, ReadOnlyContext context,
            Collector<BronzeEnvelope> output) throws Exception {
        long timestamp = envelope.kafkaTimestamp > 0 ? envelope.kafkaTimestamp : envelope.receivedAt;
        ControlEvent selected = null;
        for (Map.Entry<String, ControlEvent> entry :
                context.getBroadcastState(CONTROL_STATE).immutableEntries()) {
            ControlEvent candidate = entry.getValue();
            if (candidate.contains(timestamp)
                    && (selected == null || candidate.phase_start_ms > selected.phase_start_ms)) {
                selected = candidate;
            }
        }
        if (selected == null) {
            envelope.runId = defaultRunId;
            envelope.scenarioId = defaultScenarioId;
        } else {
            envelope.runId = selected.run_id;
            envelope.scenarioId = selected.scenario_id;
            envelope.condition = selected.condition;
            envelope.faultType = selected.fault_type;
            envelope.affectedComponent = selected.affected_component;
        }
        output.collect(envelope);
    }
}

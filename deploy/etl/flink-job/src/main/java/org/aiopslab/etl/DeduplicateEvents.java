package org.aiopslab.etl;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Marks replayed Kafka records and lets Gold report them without double-counting features. */
public class DeduplicateEvents extends KeyedProcessFunction<String, TelemetryEvent, TelemetryEvent> {
    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event", Boolean.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(6))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build());
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TelemetryEvent event, Context context,
            Collector<TelemetryEvent> output) throws Exception {
        event.duplicate = Boolean.TRUE.equals(seen.value());
        if (!event.duplicate) {
            seen.update(true);
        }
        output.collect(event);
    }
}

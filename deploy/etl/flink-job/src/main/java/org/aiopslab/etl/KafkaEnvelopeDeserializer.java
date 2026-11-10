package org.aiopslab.etl;

import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaEnvelopeDeserializer implements KafkaRecordDeserializationSchema<BronzeEnvelope> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<BronzeEnvelope> output)
            throws IOException {
        BronzeEnvelope envelope = new BronzeEnvelope();
        envelope.topic = record.topic();
        envelope.signal = record.topic().contains("metrics") ? "metrics"
                : record.topic().contains("logs") ? "logs"
                : record.topic().contains("traces") ? "traces" : "unknown";
        envelope.partition = record.partition();
        envelope.offset = record.offset();
        envelope.kafkaTimestamp = record.timestamp();
        envelope.receivedAt = System.currentTimeMillis();
        envelope.payload = record.value() == null ? new byte[0] : record.value();
        output.collect(envelope);
    }

    @Override
    public TypeInformation<BronzeEnvelope> getProducedType() {
        return TypeInformation.of(BronzeEnvelope.class);
    }
}

package org.aiopslab.etl;

import java.io.Serializable;

/** Raw OTLP Kafka record plus the metadata required to reproduce its ingestion. */
public class BronzeEnvelope implements Serializable {
    public String signal = "";
    public String topic = "";
    public int partition = -1;
    public long offset = -1;
    public long kafkaTimestamp = 0;
    public long receivedAt = 0;
    public byte[] payload = new byte[0];
    public String runId = "unassigned";
    public String scenarioId = "unassigned";
    public String condition = "unassigned";
    public String faultType = "";
    public String affectedComponent = "";

    public BronzeEnvelope() {}
}

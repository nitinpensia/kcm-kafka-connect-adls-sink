package io.kcmhub.kafka.connect.adls.dto;

import lombok.Getter;

@Getter
public class PartitionBuffer {
    final String topic;
    final int partition;
    long startOffset = -1L;
    final StringBuilder buffer = new StringBuilder();
    int recordCount = 0;

    public PartitionBuffer(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public void append(long offset, String line) {
        if (recordCount == 0) {
            startOffset = offset;
        }
        buffer.append(line).append("\n");
        recordCount++;
    }

    public boolean isEmpty() {
        return recordCount == 0;
    }

    public void clear() {
        buffer.setLength(0);
        recordCount = 0;
        startOffset = -1L;
    }
}

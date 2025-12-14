package io.kcmhub.kafka.connect.adls;

import io.kcmhub.kafka.connect.adls.dto.PartitionBuffer;

import java.util.ArrayList;
import java.util.List;

class TestableAdlsSinkTask extends AdlsSinkTask {

    static class FlushCall {
        final String topic;
        final int partition;
        final long startOffset;
        final String content;

        FlushCall(String topic, int partition, long startOffset, String content) {
            this.topic = topic;
            this.partition = partition;
            this.startOffset = startOffset;
            this.content = content;
        }
    }

    List<FlushCall> flushes = new ArrayList<>();

    @Override
    protected void flushPartitionBuffer(PartitionBuffer buf) {
        if (buf.isEmpty()) return;
        flushes.add(new FlushCall(
                buf.getTopic(),
                buf.getPartition(),
                buf.getStartOffset(),
                buf.getBuffer().toString()
        ));
        buf.clear();
    }
}


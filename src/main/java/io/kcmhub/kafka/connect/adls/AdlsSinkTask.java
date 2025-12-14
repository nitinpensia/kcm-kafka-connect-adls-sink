package io.kcmhub.kafka.connect.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kcmhub.kafka.connect.adls.dto.PartitionBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class AdlsSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(AdlsSinkTask.class);

    private String accountName;
    private String filesystem;
    private String basePath;
    private String sasToken;
    private int flushMaxRecords;
    private boolean compressGzip;
    private AdlsClientFactory clientFactory = new DefaultAdlsClientFactory();

    // For test purposes
    void setClientFactory(AdlsClientFactory factory) {
        this.clientFactory = factory;
    }

    private DataLakeFileClient buildFileClient(String filePath) {
        return clientFactory.createFileClient(accountName, filesystem, sasToken, filePath);
    }

    // Buffer par topic-partition

    private final Map<TopicPartition, PartitionBuffer> buffers = new HashMap<>();

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        AdlsSinkConnectorConfig config = new AdlsSinkConnectorConfig(props);

        this.accountName = config.getString(AdlsSinkConnectorConfig.ACCOUNT_NAME_CONFIG);
        this.filesystem = config.getString(AdlsSinkConnectorConfig.FILESYSTEM_CONFIG);
        this.basePath = config.getString(AdlsSinkConnectorConfig.BASE_PATH_CONFIG);

        this.sasToken = config.getPassword(AdlsSinkConnectorConfig.SAS_TOKEN_CONFIG).value();
        if (sasToken.startsWith("?")) {
            sasToken = sasToken.substring(1);
        }

        this.flushMaxRecords = config.getInt(AdlsSinkConnectorConfig.FLUSH_MAX_RECORDS_CONFIG);
        this.compressGzip = config.getBoolean(AdlsSinkConnectorConfig.COMPRESS_GZIP_CONFIG);

        log.info("AdlsSinkTask started. account={}, filesystem={}, basePath={}, flushMaxRecords={}, compressGzip={}",
                accountName, filesystem, basePath, flushMaxRecords, compressGzip);
    }

    // ----------------------------------------------------------------------
    //   FORMATTER AVRO / STRUCT / MAP / PRIMITIVES
    // ----------------------------------------------------------------------

    String formatRecordValue(SinkRecord record) {
        Schema schema = record.valueSchema();
        Object value = record.value();

        if (value == null) {
            return "null";
        }

        if (schema == null) {
            // schemaless : String / Map / List…
            return value.toString();
        }

        if (schema.type() == Schema.Type.STRUCT && value instanceof Struct) {
            Struct struct = (Struct) value;
            Map<String, Object> map = new LinkedHashMap<>();
            for (Field field : schema.fields()) {
                map.put(field.name(), struct.get(field));
            }
            return toJson(map);
        }

        if (schema.type() == Schema.Type.MAP && value instanceof Map<?, ?>) {
            Map<?, ?> m = (Map<?, ?>) value;
            return toJson(m);
        }

        if (schema.type() == Schema.Type.ARRAY && value instanceof List<?>) {
            List<?> l = (List<?>) value;
            return toJson(l);
        }

        // primitives & autres
        return value.toString();
    }

    private String toJson(Object obj) {
        if (obj == null) return "null";

        if (obj instanceof Map<?, ?>) {
            Map<?, ?> m = (Map<?, ?>) obj;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (var e : m.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(e.getKey()).append("\":");
                sb.append(toJson(e.getValue()));
                first = false;
            }
            return sb.append("}").toString();
        }

        if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object o : list) {
                if (!first) sb.append(",");
                sb.append(toJson(o));
                first = false;
            }
            return sb.append("]").toString();
        }

        if (obj instanceof String) {
            String s = (String) obj;
            return "\"" + s.replace("\"", "\\\"") + "\"";
        }

        return obj.toString();
    }

    // ----------------------------------------------------------------------
    //   ADLS UTIL
    // ----------------------------------------------------------------------

    private byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
                gos.write(data);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to gzip content", e);
        }
    }

    protected void flushPartitionBuffer(PartitionBuffer buf) {
        if (buf.isEmpty()) return;

        String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        String extension = compressGzip ? ".log.gz" : ".log";

        // nom de fichier basé sur topic / partition / start-offset
        String fileName = String.format(
                "%s-p%d-o%d%s",
                buf.getTopic(),
                buf.getPartition(),
                buf.getStartOffset(),
                extension
        );

        String filePath = String.format("%s/date=%s/%s", basePath, date, fileName);

        String content = buf.getBuffer().toString();
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (compressGzip) {
            bytes = gzip(bytes);
        }

        log.info("Writing {} records ({} bytes) to ADLS file {}",
                buf.getRecordCount(), bytes.length, filePath);

        DataLakeFileClient client = buildFileClient(filePath);
        client.create(true);

        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        client.append(input, 0, bytes.length);
        client.flush(bytes.length, true);

        buf.clear();
    }

    private PartitionBuffer getBuffer(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return buffers.computeIfAbsent(tp, k -> new PartitionBuffer(topic, partition));
    }

    // ----------------------------------------------------------------------
    //   TASK LOGIC
    // ----------------------------------------------------------------------

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;

        for (SinkRecord record : records) {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            long offset = record.kafkaOffset();

            PartitionBuffer buf = getBuffer(topic, partition);
            String formatted = formatRecordValue(record);
            buf.append(offset, formatted);

            if (buf.getRecordCount() >= flushMaxRecords) {
                flushPartitionBuffer(buf);
            }
        }
    }

    @Override
    public void stop() {
        log.info("Flushing remaining buffers before shutdown");
        for (PartitionBuffer buf : buffers.values()) {
            flushPartitionBuffer(buf);
        }
        buffers.clear();
    }
}


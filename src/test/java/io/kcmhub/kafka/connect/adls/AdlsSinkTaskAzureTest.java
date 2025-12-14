package io.kcmhub.kafka.connect.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AdlsSinkTaskAzureTest {

    @Test
    void shouldCallAzureClientWithExpectedOperations() {
        DataLakeFileClient mockClient = Mockito.mock(DataLakeFileClient.class);

        AdlsClientFactory factory = Mockito.mock(AdlsClientFactory.class);
        Mockito.when(factory.createFileClient(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString()
        )).thenReturn(mockClient);

        Map<String, String> props = new HashMap<>();
        props.put("adls.account.name", "acc");
        props.put("adls.filesystem", "fs");
        props.put("adls.sas.token", "token");
        props.put("flush.max.records", "10");

        AdlsSinkTask task = new AdlsSinkTask();
        task.start(props);
        task.setClientFactory(factory); // method to inject mock factory

        SinkRecord r1 = new SinkRecord("topicA", 0, null, null, null, "v1", 100L);
        task.put(List.of(r1));
        task.stop(); // flush

        Mockito.verify(mockClient).create(true);
        Mockito.verify(mockClient, Mockito.atLeastOnce())
                .append(Mockito.any(InputStream.class), Mockito.eq(0L), Mockito.anyLong());
        Mockito.verify(mockClient)
                .flush(Mockito.anyLong(), Mockito.eq(true));
    }
}


package io.kcmhub.kafka.connect.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;

public class DefaultAdlsClientFactory implements AdlsClientFactory {

    @Override
    public DataLakeFileClient createFileClient(String accountName,
                                               String filesystem,
                                               String sasToken,
                                               String path) {
        String endpoint = String.format("https://%s.dfs.core.windows.net", accountName);

        return new DataLakePathClientBuilder()
                .endpoint(endpoint)
                .fileSystemName(filesystem)
                .pathName(path)
                .sasToken(sasToken)
                .buildFileClient();
    }
}

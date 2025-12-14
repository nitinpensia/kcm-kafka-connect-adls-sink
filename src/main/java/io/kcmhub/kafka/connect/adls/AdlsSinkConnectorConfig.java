package io.kcmhub.kafka.connect.adls;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class AdlsSinkConnectorConfig extends AbstractConfig {

    public static final String ACCOUNT_NAME_CONFIG = "adls.account.name";
    public static final String FILESYSTEM_CONFIG = "adls.filesystem";
    public static final String BASE_PATH_CONFIG = "adls.base.path";
    public static final String SAS_TOKEN_CONFIG = "adls.sas.token";
    public static final String FLUSH_MAX_RECORDS_CONFIG = "flush.max.records";
    public static final String COMPRESS_GZIP_CONFIG = "compress.gzip";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ACCOUNT_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "ADLS Gen2 account name")
            .define(FILESYSTEM_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "ADLS Gen2 filesystem/container")
            .define(BASE_PATH_CONFIG, ConfigDef.Type.STRING, "kafka-export", ConfigDef.Importance.MEDIUM,
                    "Base path in ADLS filesystem")
            .define(SAS_TOKEN_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH,
                    "SAS token without leading '?'")
            .define(FLUSH_MAX_RECORDS_CONFIG, ConfigDef.Type.INT, 500, ConfigDef.Importance.MEDIUM,
                    "Maximum number of records per ADLS file")
            .define(COMPRESS_GZIP_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Enable GZIP compression for output files");

    public AdlsSinkConnectorConfig(Map<String, String> originals) {
        super(CONFIG_DEF, originals);
    }
}



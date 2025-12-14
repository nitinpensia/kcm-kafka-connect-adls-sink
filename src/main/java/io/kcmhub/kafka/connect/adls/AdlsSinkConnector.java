package io.kcmhub.kafka.connect.adls;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class AdlsSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        // Ici tu peux valider la config si besoin
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AdlsSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Simple: même config pour toutes les tasks
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(configProps));
        }
        return configs;
    }

    @Override
    public void stop() {
        // Rien de spécial
    }

    @Override
    public ConfigDef config() {
        return AdlsSinkConnectorConfig.CONFIG_DEF;
    }
}


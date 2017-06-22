package io.kyligence.benchmark.loadtest.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by xiefan on 17-1-4.
 */
public abstract class ConfigBase {

    final protected Properties properties;

    final protected String configFile;

    public ConfigBase(String configFile) throws IOException {
        this.properties = new Properties();
        this.configFile = configFile;
        properties.load(new FileInputStream(new File(configFile)));
    }

    final protected String getOptional(String key, String optValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return optValue;
        }
        return value;
    }

    final public String getConfigFilePath() {
        return configFile;
    }

    final public void putAll(Properties properties) {
        this.properties.putAll(properties);
    }
}

/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.bootup.config;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 *
 * @author Kasper Piskorski
 */
public class QueueConfig extends ProcessConfig{

    private static final String CONFIG_PARAM_PREFIX = "queue.internal.";
    private static final String DB_DIR_PARAM = "dir";
    private static final String LOG_DIR_PARAM = "logfile";
    private static final String LOG_FILE = "grakn-queue.log";
    private static final String DATA_SUBDIR = "redis/";

    private static final String RECORD_SEPARATOR = "\n";
    private static final String KEY_VALUE_SEPARATOR = " ";

    private QueueConfig(Map<String, Object> params) {
        super(params);
    }

    public static QueueConfig of(String config) {
        return new QueueConfig(QueueConfig.parseStringToMap(config));
    }
    public static QueueConfig from(Path configPath){
        String configString = ConfigProcessor.getConfigStringFromFile(configPath);
        return of(configString);
    }

    private static Map<String, Object> parseStringToMap(String configString){
        Properties props = new Properties();
        try {
            InputStream inputStream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props.stringPropertyNames().stream().collect(Collectors.toMap(prop -> prop, props::getProperty));
    }

    @Override
    public String toConfigString() {
        return Joiner
                .on(RECORD_SEPARATOR)
                .withKeyValueSeparator(KEY_VALUE_SEPARATOR)
                .join(params());
    }

    private QueueConfig updateDirs(GraknConfig config) {
        String dbDir = config.getProperty(GraknConfigKey.DATA_DIR);
        String logDir = config.getProperty(GraknConfigKey.LOG_DIR);

        ImmutableMap<String, Object> dirParams = ImmutableMap.of(
                DB_DIR_PARAM, dbDir + DATA_SUBDIR,
                LOG_DIR_PARAM, logDir + LOG_FILE
        );
        return new QueueConfig(this.updateParamsFromMap(dirParams));
    }

    @Override
    public QueueConfig updateGenericParams(GraknConfig config) {
        return new QueueConfig(this.updateParamsFromConfig(CONFIG_PARAM_PREFIX, config));
    }

    @Override
    public QueueConfig updateFromConfig(GraknConfig config) {
        return this
                .updateGenericParams(config);
                //.updateDirs(config);
    }
}

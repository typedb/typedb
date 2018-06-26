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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.bootup.config;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Container class for storing and manipulating queue configuration.
 * NB:
 * - Redis allows for multiple value params hence we need a map of lists.
 * - Redis data dir must already exist when starting redis
 *
 * @author Kasper Piskorski
 */
public class QueueConfig extends ProcessConfig<List<Object>>{

    private static final String CONFIG_PARAM_PREFIX = "queue.internal.";
    private static final String LOG_FILE = "grakn-queue.log";
    private static final String DATA_SUBDIR = "redis/";

    private static final String DB_DIR_CONFIG_KEY = "dir";
    private static final String LOG_DIR_CONFIG_KEY = "logfile";

    private static final String RECORD_SEPARATOR = "\n";
    private static final String KEY_VALUE_SEPARATOR = " ";

    private QueueConfig(Map<String, List<Object>> params) {
        super(params);
    }

    public static QueueConfig of(Map<String, List<Object>> params) {
        return new QueueConfig(params);
    }
    public static QueueConfig from(Path configPath){ return of(parseFileToMap(configPath));}

    private static Map<String, List<Object>> parseFileToMap(Path configPath){
        Map<String, List<Object>> map = new HashMap<>();
        try {
            PropertiesConfiguration props = new PropertiesConfiguration(configPath.toFile());
            props.getKeys().forEachRemaining(key -> map.put(key, props.getList(key)));

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return map;
    }

    @Override
    Map.Entry<String, List<Object>> propToEntry(String param, String value) {
        return new AbstractMap.SimpleImmutableEntry<>(param, Collections.singletonList(value));
    }

    @Override
    public String toConfigString() {
        return params().entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(value -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), value)))
                .map(e -> e.getKey() + KEY_VALUE_SEPARATOR + e.getValue())
                .collect(Collectors.joining(RECORD_SEPARATOR));
    }

    private Path getAbsoluteLogPath(GraknConfig config){
        //NB redis gets confused with relative log paths
        Path projectPath = GraknConfig.PROJECT_PATH;
        String logPathString = config.getProperty(GraknConfigKey.LOG_DIR) + LOG_FILE;
        Path logPath = Paths.get(logPathString);
        return logPath.isAbsolute() ? logPath : Paths.get(projectPath.toString(), logPathString);
    }

    private QueueConfig updateDirs(GraknConfig config) {
        String dbDir = config.getProperty(GraknConfigKey.DATA_DIR);

        ImmutableMap<String, List<Object>> dirParams = ImmutableMap.of(
                DB_DIR_CONFIG_KEY, Collections.singletonList("\"" + dbDir + DATA_SUBDIR + "\""),
                LOG_DIR_CONFIG_KEY, Collections.singletonList("\"" + getAbsoluteLogPath(config) + "\"")
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
                .updateGenericParams(config)
                .updateDirs(config);
    }
}

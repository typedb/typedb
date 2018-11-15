/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.bootup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import grakn.core.commons.config.Config;
import grakn.core.commons.config.ConfigKey;
import grakn.core.commons.config.SystemProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Container class for storing and manipulating storage configuration.
 */
public class StorageConfig {
    //TODO reimplement without importing many packages from com.fasterxml.jackson

    private static final String EMPTY_VALUE = "";
    private static final String CONFIG_PARAM_PREFIX = "storage.internal.";
    private static final String SAVED_CACHES_SUBDIR = "cassandra/saved_caches";
    private static final String COMMITLOG_SUBDIR = "cassandra/commitlog";
    private static final String DATA_SUBDIR = "cassandra/data";

    private static final String DATA_FILE_DIR_CONFIG_KEY = "data_file_directories";
    private static final String SAVED_CACHES_DIR_CONFIG_KEY = "saved_caches_directory";
    private static final String COMMITLOG_DIR_CONFIG_KEY = "commitlog_directory";
    private static final String STORAGE_CONFIG_PATH = "services/cassandra/";
    private static final String STORAGE_CONFIG_NAME = "cassandra.yaml";

    static void initialise() {
        try {
            Config inputConfig = Config.read(Paths.get(SystemProperty.CONFIGURATION_FILE.value()));
            String oldConfig;
            byte[] bytes = Files.readAllBytes(Paths.get(STORAGE_CONFIG_PATH, STORAGE_CONFIG_NAME));
            oldConfig = new String(bytes, StandardCharsets.UTF_8);


            ObjectMapper mapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));

            TypeReference<Map<String, Object>> reference = new TypeReference<Map<String, Object>>() {};
            Map<String, Object> yamlParams = mapper.readValue(oldConfig, reference);
            Map<String, Object> oldConfigMap = Maps.transformValues(yamlParams, value -> value == null ? EMPTY_VALUE : value);
            Map<String, Object> newConfigMap = new HashMap<>(oldConfigMap);

            String dataDir = inputConfig.getProperty(ConfigKey.DATA_DIR);

            ImmutableMap<String, Object> cassandraDataDirs = ImmutableMap.of(
                    DATA_FILE_DIR_CONFIG_KEY, Collections.singletonList(dataDir + DATA_SUBDIR),
                    SAVED_CACHES_DIR_CONFIG_KEY, dataDir + SAVED_CACHES_SUBDIR,
                    COMMITLOG_DIR_CONFIG_KEY, dataDir + COMMITLOG_SUBDIR
            );
            newConfigMap.putAll(cassandraDataDirs);

            //overwrite params with params from grakn config
            inputConfig.properties()
                    .stringPropertyNames()
                    .stream()
                    .filter(prop -> prop.contains(CONFIG_PARAM_PREFIX))
                    .forEach(prop -> {
                        String param = prop.replaceAll(CONFIG_PARAM_PREFIX, "");
                        if (newConfigMap.containsKey(param)) {
                            newConfigMap.put(param, inputConfig.properties().getProperty(prop));
                        }
                    });

            mapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
            ByteArrayOutputStream outputstream = new ByteArrayOutputStream();
            mapper.writeValue(outputstream, newConfigMap);

            String newConfigStr = outputstream.toString(StandardCharsets.UTF_8.name());
            Files.write(Paths.get(STORAGE_CONFIG_PATH, STORAGE_CONFIG_NAME), newConfigStr.getBytes(StandardCharsets.UTF_8));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

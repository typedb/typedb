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

package ai.grakn.bootup;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author Kasper Piskorski
 */
public class StorageConfig {

    private final ImmutableMap<String, Object> yamlParams;
    private static final String EMPTY_VALUE = "";
    private static final String CONFIG_PARAM_PREFIX = "storage.internal.";

    private StorageConfig(Map<String, Object> yamlParams){
        this.yamlParams = ImmutableMap.copyOf(yamlParams);
    }

    private static Map<String, Object> parseStringToMap(String yaml){
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
        try {
            TypeReference<Map<String, Object>> reference = new TypeReference<Map<String, Object>>(){};
            Map<String, Object> yamlParams = mapper.readValue(yaml, reference);
            return yamlParams.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() == null ? EMPTY_VALUE : e.getValue()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StorageConfig of(String yaml) {
        return new StorageConfig(parseStringToMap(yaml));
    }

    public String toYamlString() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
        try {
            ByteArrayOutputStream outputstream = new ByteArrayOutputStream();
            mapper.writeValue(outputstream, yamlParams);
            return outputstream.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private StorageConfig updateDataDirs(GraknConfig config) {
        String dbDir = config.getProperty(GraknConfigKey.DB_DIR);
        ImmutableMap<String, Object> dataParams = ImmutableMap.of(
                "data_file_directories", Collections.singletonList(dbDir + "cassandra/data"),
                "saved_caches_directory", dbDir + "cassandra/saved_caches",
                "commitlog_directory", dbDir + "cassandra/commitlog"
        );
        Map<String, Object> updatedParams = Maps.newHashMap(yamlParams);
        dataParams.keySet().stream()
                .filter(updatedParams::containsKey)
                .forEach(dp -> updatedParams.put(dp, dataParams.get(dp)));
        return new StorageConfig(updatedParams);
    }

    private StorageConfig updateGenericParams(GraknConfig config) {
        //overwrite params with params from grakn config
        Map<String, Object> updatedParams = Maps.newHashMap(yamlParams);
        config.properties()
                .stringPropertyNames()
                .stream()
                .filter(prop -> prop.contains(CONFIG_PARAM_PREFIX))
                .forEach(prop -> {
                    String param = prop.replaceAll(CONFIG_PARAM_PREFIX, "");
                    if (updatedParams.containsKey(param)) {
                        updatedParams.put(param, config.properties().getProperty(prop));
                    }
                });
        return new StorageConfig(updatedParams);
    }

    public StorageConfig updateFromConfig(GraknConfig config){
        return this
                .updateGenericParams(config)
                .updateDataDirs(config);
    }
}

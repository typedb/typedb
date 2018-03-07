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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author Kasper Piskorski
 */
public class StorageConfig {

    private final ImmutableMap<String, Object> params;

    private static final String CONFIG_PARAM_PREFIX = "storage.internal.";

    private static final Map<String, String> dataParams = ImmutableMap.of(
            "data_file_directories", "cassandra/data",
            "saved_caches_directory", "cassandra/saved_caches",
            "commitlog_directory", "cassandra/commitlog"
    );

    private StorageConfig(Map<String, Object> p){
        this.params = ImmutableMap.copyOf(p);
    }

    public static StorageConfig of(String yaml) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            TypeReference<Map<String, Object>> reference = new TypeReference<Map<String, Object>>(){};
            return new StorageConfig(mapper.readValue(yaml, reference));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String toYaml() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            Map<String, Object> curatedParams = params.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() == null ? "" : e.getValue()));
            ByteArrayOutputStream outputstream = new ByteArrayOutputStream();
            mapper.writeValue(outputstream, curatedParams);
            return outputstream.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> params(){ return params;}

    public StorageConfig updateDataParams(GraknConfig config) {
        String dbDir = config.getProperty(GraknConfigKey.DB_DIR);
        if (dbDir.isEmpty()) return this;

        Map<String, Object> updatedParams = Maps.newHashMap(params());

        dataParams.keySet().stream()
                .filter(updatedParams::containsKey)
                .forEach(dp -> updatedParams.put(dp, Paths.get(dbDir, dataParams.get(dp))));
        return new StorageConfig(updatedParams);
    }

    public StorageConfig update(GraknConfig config){
        //overwrite params with params from grakn config
        Map<String, Object> params = Maps.newHashMap(params());
        config.properties()
                .stringPropertyNames()
                .stream()
                .filter(prop -> prop.contains(CONFIG_PARAM_PREFIX))
                .forEach(prop -> {
                    String param = prop.replaceAll(CONFIG_PARAM_PREFIX, "");
                    if (params.containsKey(param)) {
                        params.put(param, config.properties().getProperty(prop));
                    }
                });
        return new StorageConfig(params);
    }
}

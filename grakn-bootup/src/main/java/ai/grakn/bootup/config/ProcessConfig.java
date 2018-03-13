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

import ai.grakn.engine.GraknConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;


/**
 *
 * @author Kasper Piskorski
 */
public abstract class ProcessConfig {

    private final ImmutableMap<String, Object> params;

    ProcessConfig(Map<String, Object> params) {
        this.params = ImmutableMap.copyOf(params);
    }

    ImmutableMap<String, Object> params() { return params; }

    Map<String, Object> updateParamsFromConfig(String CONFIG_PARAM_PREFIX, GraknConfig config) {
        //overwrite params with params from grakn config
        Map<String, Object> updatedParams = Maps.newHashMap(params());
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
        return updatedParams;
    }

    Map<String, Object> updateParamsFromMap(Map<String, Object> newParams){
        Map<String, Object> updatedParams = Maps.newHashMap(params());
        updatedParams.putAll(newParams);
        return updatedParams;
    }

    public abstract String toConfigString();

    public abstract ProcessConfig updateGenericParams(GraknConfig config);

    public abstract ProcessConfig updateFromConfig(GraknConfig config);
}

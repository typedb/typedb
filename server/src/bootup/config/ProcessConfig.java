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

package ai.grakn.core.server.bootup.config;

import ai.grakn.util.GraknConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;


/**
 *
 *  @param <V> config parameter value type
 *
 * @author Kasper Piskorski
 */
public abstract class ProcessConfig<V> {

    private final ImmutableMap<String, V> params;

    ProcessConfig(Map<String, V> params) {
        this.params = ImmutableMap.copyOf(params);
    }

    public ImmutableMap<String, V> params() { return params; }

    Map<String, V> updateParamsFromMap(Map<String, V> newParams){
        Map<String, V> updatedParams = Maps.newHashMap(params());
        updatedParams.putAll(newParams);
        return updatedParams;
    }

    Map<String, V> updateParamsFromConfig(String CONFIG_PARAM_PREFIX, GraknConfig config) {
        //overwrite params with params from grakn config
        Map<String, V> updatedParams = Maps.newHashMap(params());
        config.properties()
                .stringPropertyNames()
                .stream()
                .filter(prop -> prop.contains(CONFIG_PARAM_PREFIX))
                .forEach(prop -> {
                    String param = prop.replaceAll(CONFIG_PARAM_PREFIX, "");
                    if (updatedParams.containsKey(param)) {
                        Map.Entry<String, V> entry = propToEntry(param, prop);
                        updatedParams.put(entry.getKey(), entry.getValue());
                    }
                });
        return updatedParams;
    }
    
    abstract Map.Entry<String, V> propToEntry(String param, String value);

    public abstract String toConfigString();

    public abstract ProcessConfig updateGenericParams(GraknConfig config);

    public abstract ProcessConfig updateFromConfig(GraknConfig config);
}

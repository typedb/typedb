/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.graphdb.configuration;

import com.google.common.collect.ImmutableSet;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Collection of constants used throughput the JanusGraph codebase.
 */
public class JanusGraphConstants {

    /**
     * Runtime version of JanusGraph, as read from a properties file inside the core jar
     */
    public static final String VERSION = "0.0.1";

    /**
     * Name of the ids.store-name used by JanusGraph which is configurable
     */
    public static final String JANUSGRAPH_ID_STORE_NAME = "janusgraph_ids";

    /**
     * Storage format version currently used by JanusGraph, version 1 is for JanusGraph 0.2.x and below
     */
    public static final String STORAGE_VERSION = "2";


    private static Set<String> getPropertySet(Properties props, String key) {
        ImmutableSet.Builder<String> buildSet = ImmutableSet.builder();
        buildSet.addAll(Stream.of(props.getProperty(key, "").split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList()));
        return buildSet.build();
    }
}

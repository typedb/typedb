// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

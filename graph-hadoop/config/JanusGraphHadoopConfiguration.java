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

package grakn.core.graph.hadoop.config;

import grakn.core.graph.diskstorage.Backend;
import grakn.core.graph.diskstorage.configuration.ConfigNamespace;
import grakn.core.graph.diskstorage.configuration.ConfigOption;

public class JanusGraphHadoopConfiguration {

    public static final ConfigNamespace MAPRED_NS =
            new ConfigNamespace(null, "janusgraphmr", "JanusGraph MapReduce configuration root");

    // ScanJob configuration

    public static final ConfigNamespace SCAN_NS =
            new ConfigNamespace(MAPRED_NS, "scanjob", "ScanJob configuration");

    public static final ConfigOption<String> SCAN_JOB_CONFIG_ROOT =
            new ConfigOption<>(SCAN_NS, "conf-root",
                    "A string in the form \"PACKAGE.CLASS#STATICFIELD\" representing the config namespace root to use for the ScanJob",
                    ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace SCAN_JOB_CONFIG_KEYS =
            new ConfigNamespace(SCAN_NS, "conf", "ScanJob configuration");

    public static final ConfigOption<String> SCAN_JOB_CLASS =
            new ConfigOption<>(SCAN_NS, "class",
                    "A string in the form \"PACKAGE.CLASS\" representing the ScanJob to use.  Must have a no-arg constructor.",
                    ConfigOption.Type.LOCAL, String.class);

    // JanusGraph Hadoop I/O format configuration

    public static final ConfigNamespace IOFORMAT_NS =
            new ConfigNamespace(MAPRED_NS, "ioformat", "JanusGraph input configuration");

    public static final ConfigNamespace GRAPH_CONFIG_KEYS =
            new ConfigNamespace(IOFORMAT_NS, "conf", "Settings to be passed to JanusGraphFactory.open");

    public static final ConfigOption<Boolean> FILTER_PARTITIONED_VERTICES =
            new ConfigOption<>(IOFORMAT_NS, "filter-partitioned-vertices",
                    "True to drop partitioned vertices and relations incident on partitioned vertices when reading " +
                            "from JanusGraph.  This currently must be true when partitioned vertices are present in the " +
                            "input; if it is false when a partitioned vertex is encountered, then an exception is thrown.  " +
                            "This limitation may be lifted in a later version of JanusGraph-Hadoop.",
                    ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String> COLUMN_FAMILY_NAME =
            new ConfigOption<>(IOFORMAT_NS, "cf-name",
                    "The name of the column family from which the Hadoop input format should read.  " +
                            "Usually edgestore or graphindex.", ConfigOption.Type.LOCAL, Backend.EDGESTORE_NAME);

    // JanusGraph bulkload vertex program configuration

    public static final ConfigNamespace BULKLOAD_NS =
            new ConfigNamespace(MAPRED_NS, "bulkload", "JanusGraph BulkLoaderVertexProgram configuration");

    public static final ConfigNamespace BULKLOAD_GRAPH_CONFIG_KEYS =
            new ConfigNamespace(BULKLOAD_NS, "conf", "Settings to be passed to JanusGraphFactory.open");

    public static final ConfigOption<Boolean> BULKLOAD_SCHEMA_CHECK =
            new ConfigOption<>(BULKLOAD_NS, "filter-partitioned-vertices",
                    "Whether to enforce best-effort checks on edge multiplicity and property cardinality.  " +
                            "These checks do not read the existing properties and edges in JanusGraph.  They only consider " +
                            "those elements visible from a single MapReduce worker.  Hence, these checks do not " +
                            "guarantee that invalid input data will be detected and rejected.",
                    ConfigOption.Type.LOCAL, false);

}

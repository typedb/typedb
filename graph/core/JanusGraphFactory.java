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

package grakn.core.graph.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import grakn.core.graph.core.log.LogProcessorFramework;
import grakn.core.graph.core.log.TransactionRecovery;
import grakn.core.graph.diskstorage.Backend;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.configuration.MergedConfiguration;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.diskstorage.configuration.ReadConfiguration;
import grakn.core.graph.diskstorage.configuration.WriteConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.CommonsConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import grakn.core.graph.diskstorage.configuration.builder.ReadConfigurationBuilder;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.configuration.builder.MergedConfigurationBuilder;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.log.StandardLogProcessorFramework;
import grakn.core.graph.graphdb.log.StandardTransactionLogProcessor;
import grakn.core.graph.util.system.ConfigurationUtil;
import grakn.core.graph.util.system.IOUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.time.Instant;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

/**
 * JanusGraphFactory is used to open or instantiate a JanusGraph graph database.
 */
public class JanusGraphFactory {

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static StandardJanusGraph open(BasicConfiguration configuration) {
        return open(configuration.getConfiguration());
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static StandardJanusGraph open(ReadConfiguration configuration) {
        return open(configuration, null);
    }

    /**
     * Opens a {@link JanusGraph} database configured according to the provided configuration.
     * This method shouldn't be called by end users; it is used by internal server processes to
     * open graphs defined at server start that do not include the graphname property.
     *
     * @param configuration Configuration for the graph database
     * @param backupName    Backup name for graph
     * @return JanusGraph graph database
     */
    private static StandardJanusGraph open(ReadConfiguration configuration, String backupName) {
        // Create BasicConfiguration out of ReadConfiguration for local configuration
        BasicConfiguration localBasicConfiguration = new BasicConfiguration(ROOT_NS, configuration, BasicConfiguration.Restriction.NONE);

        // Initialise Store Manager used to connect to 'system_properties' to read global configuration
        KeyColumnValueStoreManager storeManager = getStoreManager(localBasicConfiguration);

        // Configurations read from system_properties
        ReadConfiguration globalConfig = ReadConfigurationBuilder.buildGlobalConfiguration(localBasicConfiguration, storeManager, new KCVSConfigurationBuilder());
        // Create BasicConfiguration out of ReadConfiguration for global configuration
        BasicConfiguration globalBasicConfig = new BasicConfiguration(ROOT_NS, globalConfig, BasicConfiguration.Restriction.NONE);

        // Merge and sanitise local and global configuration to get Merged configuration which incorporates all necessary configs.
        MergedConfiguration mergedConfig = MergedConfigurationBuilder.build(localBasicConfiguration, globalBasicConfig, storeManager);

        // Initialise the 2 components needed by StandardJanusGraph
        Backend backend = new Backend(mergedConfig, storeManager);
        GraphDatabaseConfiguration dbConfig = new GraphDatabaseConfiguration(configuration, mergedConfig, storeManager.getFeatures());


        // When user specifies graphname property is because he wishes to register the graph with the GraphManager
        // The GraphManager though needs to be enabled using the YAML properties file
        String graphName = localBasicConfiguration.has(GRAPH_NAME) ? localBasicConfiguration.get(GRAPH_NAME) : backupName;
        return new StandardJanusGraph(dbConfig, backend);
    }


    /**
     * Drop graph database, deleting all data in storage and indexing backends. Graph can be open or closed (will be
     * closed as part of the drop operation).
     *
     * <p><b>WARNING: This is an irreversible operation that will delete all graph and index data.</b></p>
     *
     * @param graph JanusGraph graph database. Can be open or closed.
     * @throws BackendException If an error occurs during deletion
     */
    public static void drop(JanusGraph graph) throws BackendException {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph instanceof StandardJanusGraph, "Invalid graph instance detected: %s", graph.getClass());
        StandardJanusGraph g = (StandardJanusGraph) graph;

        if (graph.isOpen()) {
            graph.close();
        }
        grakn.core.graph.diskstorage.configuration.Configuration backendConfiguration = g.getConfiguration().getConfiguration();
        KeyColumnValueStoreManager storeManager = getStoreManager(backendConfiguration);
        Backend backend = new Backend(backendConfiguration, storeManager);
        try {
            backend.clearStorage();
        } finally {
            IOUtils.closeQuietly(backend);
        }
    }

    /**
     * Returns a {@link Builder} that allows to set the configuration options for opening a JanusGraph graph database.
     * <p>
     * In the builder, the configuration options for the graph can be set individually. Once all options are configured,
     * the graph can be opened with {@link JanusGraphFactory.Builder#open()}.
     */
    public static Builder build() {
        return new Builder();
    }

    //--------------------- BUILDER -------------------------------------------

    public static class Builder {

        private final WriteConfiguration writeConfiguration;

        private Builder() {
            writeConfiguration = new CommonsConfiguration();
        }

        /**
         * Configures the provided configuration path to the given value.
         */
        public Builder set(String path, Object value) {
            writeConfiguration.set(path, value);
            return this;
        }

        /**
         * Opens a JanusGraph graph with the previously configured options.
         */
        public StandardJanusGraph open() {
            ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                    writeConfiguration.copy(), BasicConfiguration.Restriction.NONE);
            return JanusGraphFactory.open(mc);
        }
    }

    /**
     * Returns a {@link LogProcessorFramework} for processing transaction LOG entries
     * against the provided graph instance.
     */
    public static LogProcessorFramework openTransactionLog(JanusGraph graph) {
        return new StandardLogProcessorFramework((StandardJanusGraph) graph);
    }

    /**
     * Returns a {@link TransactionRecovery} process for recovering partially failed transactions. The recovery process
     * will start processing the write-ahead transaction LOG at the specified transaction time.
     */
    public static TransactionRecovery startTransactionRecovery(JanusGraph graph, Instant start) {
        return new StandardTransactionLogProcessor((StandardJanusGraph) graph, start);
    }

    //###################################
    //          HELPER METHODS
    //###################################


    @VisibleForTesting
    public static KeyColumnValueStoreManager getStoreManager(grakn.core.graph.diskstorage.configuration.Configuration configuration) {
        String className;
        String backendName = configuration.get(STORAGE_BACKEND);
        switch (backendName) {
            case "cql":
                className = "grakn.core.graph.diskstorage.cql.CQLStoreManager";
                break;
            case "inmemory":
                className = "grakn.core.graph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager";
                break;
            case "foundationdb":
                className = "io.grakn.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager";
                OrderedKeyValueStoreManager foundationManager = ConfigurationUtil.instantiate(className, new Object[]{configuration}, new Class[]{Configuration.class});
                return new OrderedKeyValueStoreManagerAdapter(foundationManager);
            default:
                throw new IllegalArgumentException("Could not find implementation class for backend: " + backendName);
        }

        return ConfigurationUtil.instantiate(className, new Object[]{configuration}, new Class[]{grakn.core.graph.diskstorage.configuration.Configuration.class});
    }

    private static ReadConfiguration getLocalConfiguration(String backendShortcut) {
        BaseConfiguration config = new BaseConfiguration();
        ModifiableConfiguration writeConfig = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(config), BasicConfiguration.Restriction.NONE);
        writeConfig.set(STORAGE_BACKEND, backendShortcut);
        return new CommonsConfiguration(config);
    }

}

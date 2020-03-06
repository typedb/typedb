/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.core.graph.diskstorage.Backend;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.PermanentBackendException;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.MergedConfiguration;
import grakn.core.graph.diskstorage.configuration.ReadConfiguration;
import grakn.core.graph.diskstorage.configuration.WriteConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.CommonsConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import grakn.core.graph.diskstorage.configuration.builder.ReadConfigurationBuilder;
import grakn.core.graph.diskstorage.cql.CQLStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.configuration.builder.MergedConfigurationBuilder;
import grakn.core.graph.graphdb.database.StandardJanusGraph;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;

/**
 * JanusGraphFactory is used to open or instantiate a JanusGraph graph database.
 */
public class JanusGraphFactory {

    /**
     * Opens a JanusGraph database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return JanusGraph graph database
     */
    public static StandardJanusGraph open(ReadConfiguration configuration) {
        // Create BasicConfiguration out of ReadConfiguration for local configuration
        BasicConfiguration localBasicConfiguration = new BasicConfiguration(ROOT_NS, configuration);

        // Initialise Store Manager used to connect to 'system_properties' to read global configuration
        KeyColumnValueStoreManager storeManager = getStoreManager(localBasicConfiguration);

        // Configurations read from system_properties
        ReadConfiguration globalConfig = ReadConfigurationBuilder.buildGlobalConfiguration(localBasicConfiguration, storeManager, new KCVSConfigurationBuilder());

        // Create BasicConfiguration out of ReadConfiguration for global configuration
        BasicConfiguration globalBasicConfig = new BasicConfiguration(ROOT_NS, globalConfig);

        // Merge and sanitise local and global configuration to get Merged configuration which incorporates all necessary configs.
        MergedConfiguration mergedConfig = MergedConfigurationBuilder.build(localBasicConfiguration, globalBasicConfig, storeManager);

        // Initialise the 2 components needed by StandardJanusGraph
        Backend backend = new Backend(mergedConfig, storeManager);
        GraphDatabaseConfiguration dbConfig = new GraphDatabaseConfiguration(configuration, mergedConfig, storeManager.getFeatures().isDistributed());

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
        Configuration backendConfiguration = g.getConfiguration().getConfiguration();
        KeyColumnValueStoreManager storeManager = getStoreManager(backendConfiguration);
        Backend backend = new Backend(backendConfiguration, storeManager);
        try {
            backend.clearStorage();
        } finally {
            backend.close();
        }
    }

    /**
     * Returns a Builder that allows to set the configuration options for opening a JanusGraph graph database.
     * <p>
     * In the builder, the configuration options for the graph can be set individually. Once all options are configured,
     * the graph can be opened with JanusGraphFactory.Builder#open().
     */
    public static Builder build() {
        return new Builder();
    }


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
            return JanusGraphFactory.open(writeConfiguration);
        }
    }


    @VisibleForTesting
    public static KeyColumnValueStoreManager getStoreManager(Configuration configuration) {
        try {
            return new CQLStoreManager(configuration);
        } catch (PermanentBackendException e) {
            throw new IllegalArgumentException("Could not instantiate StoreManager class: " + e);
        }
    }

}

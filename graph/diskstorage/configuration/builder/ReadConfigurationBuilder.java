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

package grakn.core.graph.diskstorage.configuration.builder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.configuration.ConfigElement;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.diskstorage.configuration.ReadConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.KCVSConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.StandardBaseTransactionConfig;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.configuration.JanusGraphConstants;
import grakn.core.graph.util.system.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_UPGRADE;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.INITIAL_STORAGE_VERSION;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * Builder to build {@link ReadConfiguration} instance of global configuration
 */
public class ReadConfigurationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ReadConfigurationBuilder.class);
    private static final String BACKLEVEL_STORAGE_VERSION_EXCEPTION = "The storage version on the client or server is lower than the storage version of the graph: graph storage version %s vs. client storage version %s when opening graph %s.";
    private static final String INCOMPATIBLE_STORAGE_VERSION_EXCEPTION = "Storage version is incompatible with current client: graph storage version %s vs. client storage version %s when opening graph %s.";

    public static ReadConfiguration buildGlobalConfiguration(BasicConfiguration localBasicConfiguration,
                                                             KeyColumnValueStoreManager storeManager,
                                                             KCVSConfigurationBuilder kcvsConfigurationBuilder) {


        BackendOperation.TransactionalProvider transactionalProvider = new BackendOperation.TransactionalProvider() {
            @Override
            public StoreTransaction openTx() throws BackendException {
                return storeManager.beginTransaction(StandardBaseTransactionConfig.of(localBasicConfiguration.get(TIMESTAMP_PROVIDER), storeManager.getFeatures().getKeyConsistentTxConfig()));
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        KeyColumnValueStore systemPropertiesStore;
        try {
            systemPropertiesStore = storeManager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open 'system_properties' store: ", e);
        }

        //Read  Global Configuration (from 'system_properties' store, everything associated to 'configuration' key)
        try (KCVSConfiguration keyColumnValueStoreConfiguration = kcvsConfigurationBuilder.buildGlobalConfiguration(transactionalProvider, systemPropertiesStore, localBasicConfiguration)) {

            //Freeze global configuration if not already frozen!
            ModifiableConfiguration globalWrite = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, keyColumnValueStoreConfiguration, BasicConfiguration.Restriction.GLOBAL);

            if (!globalWrite.isFrozen()) {
                //Copy over global configurations
                globalWrite.setAll(getGlobalSubset(localBasicConfiguration.getAll()));

                setupJanusGraphVersion(globalWrite);
                setupStorageVersion(globalWrite);
                setupTimestampProvider(globalWrite, localBasicConfiguration, storeManager);

                globalWrite.freezeConfiguration();
            } else {
                String graphName = localBasicConfiguration.getConfiguration().get(GRAPH_NAME.toStringWithoutRoot(), String.class);
                final boolean upgradeAllowed = isUpgradeAllowed(globalWrite, localBasicConfiguration);

                if (upgradeAllowed) {
                    setupUpgradeConfiguration(graphName, globalWrite);
                } else {
                    checkJanusGraphStorageVersionEquality(globalWrite, graphName);
                }

//                checkOptionsWithDiscrepancies(globalWrite, localBasicConfiguration, overwrite);
            }
            return keyColumnValueStoreConfiguration.asReadConfiguration();
        }
    }

    private static void setupUpgradeConfiguration(String graphName, ModifiableConfiguration globalWrite) {
        // If the graph doesn't have a storage version set it and update version
        if (!globalWrite.has(INITIAL_STORAGE_VERSION)) {
            janusGraphVersionsWithDisallowedUpgrade(globalWrite);
            LOG.info("graph.storage-version has been upgraded from 1 to {} and graph.janusgraph-version has been upgraded from {} to {} on graph {}",
                    JanusGraphConstants.STORAGE_VERSION, globalWrite.get(INITIAL_JANUSGRAPH_VERSION), JanusGraphConstants.VERSION, graphName);
            return;
        }
        int storageVersion = Integer.parseInt(JanusGraphConstants.STORAGE_VERSION);
        int initialStorageVersion = Integer.parseInt(globalWrite.get(INITIAL_STORAGE_VERSION));
        // If the storage version of the client or server opening the graph is lower than the graph's storage version throw an exception
        if (initialStorageVersion > storageVersion) {
            throw new JanusGraphException(String.format(BACKLEVEL_STORAGE_VERSION_EXCEPTION, globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION, graphName));
        }
        // If the graph has a storage version, but it's lower than the client or server opening the graph upgrade the version and storage version
        if (initialStorageVersion < storageVersion) {
            janusGraphVersionsWithDisallowedUpgrade(globalWrite);
            LOG.info("graph.storage-version has been upgraded from {} to {} and graph.janusgraph-version has been upgraded from {} to {} on graph {}",
                    globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION, globalWrite.get(INITIAL_JANUSGRAPH_VERSION), JanusGraphConstants.VERSION, graphName);
        } else {
            LOG.warn("Warning graph.allow-upgrade is currently set to true on graph {}. Please set graph.allow-upgrade to false in your properties file.", graphName);
        }
    }

    private static void janusGraphVersionsWithDisallowedUpgrade(ModifiableConfiguration globalWrite) {
        globalWrite.set(INITIAL_JANUSGRAPH_VERSION, JanusGraphConstants.VERSION);
        globalWrite.set(INITIAL_STORAGE_VERSION, JanusGraphConstants.STORAGE_VERSION);
        globalWrite.set(ALLOW_UPGRADE, false);
    }

    private static void setupJanusGraphVersion(ModifiableConfiguration globalWrite) {
        Preconditions.checkArgument(!globalWrite.has(INITIAL_JANUSGRAPH_VERSION), "Database has already been initialized but not frozen");
        globalWrite.set(INITIAL_JANUSGRAPH_VERSION, JanusGraphConstants.VERSION);
    }

    private static void setupStorageVersion(ModifiableConfiguration globalWrite) {
        Preconditions.checkArgument(!globalWrite.has(INITIAL_STORAGE_VERSION), "Database has already been initialized but not frozen");
        globalWrite.set(INITIAL_STORAGE_VERSION, JanusGraphConstants.STORAGE_VERSION);
    }

    private static void setupTimestampProvider(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration, KeyColumnValueStoreManager storeManager) {
        /* If the configuration does not explicitly set a timestamp provider and
         * the storage backend both supports timestamps and has a preference for
         * a specific timestamp provider, then apply the backend's preference.
         */
        if (!localBasicConfiguration.has(TIMESTAMP_PROVIDER)) {
            StoreFeatures f = storeManager.getFeatures();
            final TimestampProviders backendPreference;
            if (f.hasTimestamps() && null != (backendPreference = f.getPreferredTimestamps())) {
                globalWrite.set(TIMESTAMP_PROVIDER, backendPreference);
                LOG.debug("Set timestamps to {} according to storage backend preference",
                        LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
            } else {
                globalWrite.set(TIMESTAMP_PROVIDER, TIMESTAMP_PROVIDER.getDefaultValue());
                LOG.debug("Set default timestamp provider {}", LoggerUtil.sanitizeAndLaunder(globalWrite.get(TIMESTAMP_PROVIDER)));
            }
        } else {
            LOG.debug("Using configured timestamp provider {}", localBasicConfiguration.get(TIMESTAMP_PROVIDER));
        }
    }

    private static Map<ConfigElement.PathIdentifier, Object> getGlobalSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> ((ConfigOption) entry.getKey().element).isGlobal());
    }

    private static Map<ConfigElement.PathIdentifier, Object> getManagedSubset(Map<ConfigElement.PathIdentifier, Object> m) {
        return Maps.filterEntries(m, entry -> ((ConfigOption) entry.getKey().element).isManaged());
    }

    private static void checkJanusGraphStorageVersionEquality(ModifiableConfiguration globalWrite, String graphName) {
        if (!Objects.equals(globalWrite.get(INITIAL_STORAGE_VERSION), JanusGraphConstants.STORAGE_VERSION)) {
            String storageVersion = (globalWrite.has(INITIAL_STORAGE_VERSION)) ? globalWrite.get(INITIAL_STORAGE_VERSION) : "1";
            throw new JanusGraphException(String.format(INCOMPATIBLE_STORAGE_VERSION_EXCEPTION, storageVersion, JanusGraphConstants.STORAGE_VERSION, graphName));
        }
    }

    private static boolean isUpgradeAllowed(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration) {
        if (localBasicConfiguration.has(ALLOW_UPGRADE)) {
            return localBasicConfiguration.get(ALLOW_UPGRADE);
        } else if (globalWrite.has(ALLOW_UPGRADE)) {
            return globalWrite.get(ALLOW_UPGRADE);
        }
        return ALLOW_UPGRADE.getDefaultValue();
    }
}

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

package grakn.core.graph.diskstorage.configuration.builder;

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
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.StandardBaseTransactionConfig;
import grakn.core.graph.diskstorage.util.time.TimestampProviders;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.util.system.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * Builder to build ReadConfiguration instance of global configuration
 */
public class ReadConfigurationBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ReadConfigurationBuilder.class);

    public static ReadConfiguration buildGlobalConfiguration(BasicConfiguration localBasicConfiguration,
                                                             KeyColumnValueStoreManager storeManager,
                                                             KCVSConfigurationBuilder kcvsConfigurationBuilder) {

        StandardBaseTransactionConfig txConfig = StandardBaseTransactionConfig.of(localBasicConfiguration.get(TIMESTAMP_PROVIDER), storeManager.getFeatures().getKeyConsistentTxConfig());
        BackendOperation.TransactionalProvider transactionalProvider = BackendOperation.buildTxProvider(storeManager, txConfig);
        KeyColumnValueStore systemPropertiesStore;
        try {
            systemPropertiesStore = storeManager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open 'system_properties' store: ", e);
        }

        //Read  Global Configuration (from 'system_properties' store, everything associated to 'configuration' key)
        try (KCVSConfiguration keyColumnValueStoreConfiguration = kcvsConfigurationBuilder.buildGlobalConfiguration(transactionalProvider, systemPropertiesStore, localBasicConfiguration)) {

            //Freeze global configuration if not already frozen!
            ModifiableConfiguration globalWrite = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, keyColumnValueStoreConfiguration);

            if (!globalWrite.isFrozen()) {
                //Copy over global configurations
                globalWrite.setAll(getGlobalSubset(localBasicConfiguration.getAll()));

                setupTimestampProvider(globalWrite, localBasicConfiguration, storeManager);

                globalWrite.freezeConfiguration();
            }

            return keyColumnValueStoreConfiguration.asReadConfiguration();
        }
    }


    private static void setupTimestampProvider(ModifiableConfiguration globalWrite, BasicConfiguration localBasicConfiguration, KeyColumnValueStoreManager storeManager) {
        /* If the configuration does not explicitly set a timestamp provider and
         * the storage backend both supports timestamps and has a preference for
         * a specific timestamp provider, then apply the backend's preference.
         */
        if (!localBasicConfiguration.has(TIMESTAMP_PROVIDER)) {
            StoreFeatures f = storeManager.getFeatures();
            TimestampProviders backendPreference;
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

}

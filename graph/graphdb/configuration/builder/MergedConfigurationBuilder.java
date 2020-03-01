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

package grakn.core.graph.graphdb.configuration.builder;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.configuration.MergedConfiguration;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.diskstorage.configuration.backend.CommonsConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_FIXED_PARTITION;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_KEY_CONSISTENT;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_STORE_TTL;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG_DEFAULT_TTL;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;

/**
 * Builder for GraphDatabaseConfiguration
 */
public class MergedConfigurationBuilder {

    private final static AtomicLong instanceCounter = new AtomicLong(0);


    /**
     * This methods merges the 3 configurations into 1 and provides a wrapper configuration: GraphDatabase config
     * <p>
     * We create an 'overwrite' config in order to set and force parameters that were not explicitly set by the user in local config.
     *
     * @param localBasicConfiguration local configurations provided by user
     * @return Configuration that contains both local and global bits, to bed fed to the new Graph
     */
    public static MergedConfiguration build(BasicConfiguration localBasicConfiguration, BasicConfiguration globalBasicConfig, KeyColumnValueStoreManager storeManager) {
        Configuration combinedConfig = new MergedConfiguration(localBasicConfiguration, globalBasicConfig);

        //Compute unique instance id
        ModifiableConfiguration overwrite = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration());
        overwrite.set(UNIQUE_INSTANCE_ID, uniqueGraphId());

        StoreFeatures storeFeatures = storeManager.getFeatures();
        checkAndOverwriteTransactionLogConfiguration(combinedConfig, overwrite, storeFeatures);
        checkAndOverwriteSystemManagementLogConfiguration(combinedConfig, overwrite);

        return new MergedConfiguration(overwrite, combinedConfig);
    }

    // This used to be way more fancy in the original Janus, but for Grakn usecase it doesnt need to be fancy for now
    private static String uniqueGraphId(){
        return String.valueOf(instanceCounter.incrementAndGet());
    }


    private static void checkAndOverwriteTransactionLogConfiguration(Configuration combinedConfig, ModifiableConfiguration overwrite, StoreFeatures storeFeatures) {

        //Default LOG configuration for system and tx LOG
        //TRANSACTION LOG: send_delay=0, ttl=2days and backend=default
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND, TRANSACTION_LOG).equals(LOG_BACKEND.getDefaultValue()),
                "Must use default LOG backend for transaction LOG");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY, TRANSACTION_LOG) ||
                combinedConfig.get(LOG_SEND_DELAY, TRANSACTION_LOG).isZero(), "Send delay must be 0 for transaction LOG.");
        overwrite.set(LOG_SEND_DELAY, Duration.ZERO, TRANSACTION_LOG);
        if (!combinedConfig.has(LOG_STORE_TTL, TRANSACTION_LOG) && TTLKCVSManager.supportsAnyTTL(storeFeatures)) {
            overwrite.set(LOG_STORE_TTL, TRANSACTION_LOG_DEFAULT_TTL, TRANSACTION_LOG);
        }
    }

    private static void checkAndOverwriteSystemManagementLogConfiguration(Configuration combinedConfig, ModifiableConfiguration overwrite) {

        //SYSTEM MANAGEMENT LOG: backend=default and send_delay=0 and key_consistent=true and fixed-partitions=true
        Preconditions.checkArgument(combinedConfig.get(LOG_BACKEND, MANAGEMENT_LOG).equals(LOG_BACKEND.getDefaultValue()),
                "Must use default LOG backend for system LOG");
        Preconditions.checkArgument(!combinedConfig.has(LOG_SEND_DELAY, MANAGEMENT_LOG) ||
                combinedConfig.get(LOG_SEND_DELAY, MANAGEMENT_LOG).isZero(), "Send delay must be 0 for system LOG.");
        overwrite.set(LOG_SEND_DELAY, Duration.ZERO, MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(LOG_KEY_CONSISTENT, MANAGEMENT_LOG) ||
                combinedConfig.get(LOG_KEY_CONSISTENT, MANAGEMENT_LOG), "Management LOG must be configured to be key-consistent");
        overwrite.set(LOG_KEY_CONSISTENT, true, MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(LOG_FIXED_PARTITION, MANAGEMENT_LOG)
                || combinedConfig.get(LOG_FIXED_PARTITION, MANAGEMENT_LOG), "Fixed partitions must be enabled for management LOG");
        overwrite.set(LOG_FIXED_PARTITION, true, MANAGEMENT_LOG);
    }

}

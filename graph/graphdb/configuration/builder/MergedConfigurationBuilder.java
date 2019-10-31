// Copyright 2018 JanusGraph Authors
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

package grakn.core.graph.graphdb.configuration.builder;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.idmanagement.UniqueInstanceIdRetriever;

import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_STORE_TTL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG_DEFAULT_TTL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;

/**
 * Builder for {@link GraphDatabaseConfiguration}
 */
public class MergedConfigurationBuilder {

    /**
     * This methods merges the 3 configurations into 1 and provides a wrapper configuration: GraphDatabase config
     *
     * We create an 'overwrite' config in order to set and force parameters that were not explicitly set by the user in local config.
     * @param localBasicConfiguration local configurations provided by user
     * @return Configuration that contains both local and global bits, to bed fed to the new Graph
     */
    public static MergedConfiguration build(BasicConfiguration localBasicConfiguration, BasicConfiguration globalBasicConfig, KeyColumnValueStoreManager storeManager) {
        Configuration combinedConfig = new MergedConfiguration(localBasicConfiguration, globalBasicConfig);

        //Compute unique instance id
        ModifiableConfiguration overwrite = new ModifiableConfiguration(ROOT_NS, new CommonsConfiguration(), BasicConfiguration.Restriction.NONE);
        String uniqueGraphId = UniqueInstanceIdRetriever.getInstance().getOrGenerateUniqueInstanceId(combinedConfig);
        overwrite.set(UNIQUE_INSTANCE_ID, uniqueGraphId);
        // If lock prefix is unspecified, specify it now
        if (!localBasicConfiguration.has(LOCK_LOCAL_MEDIATOR_GROUP)) {
            overwrite.set(LOCK_LOCAL_MEDIATOR_GROUP, storeManager.getName());
        }

        StoreFeatures storeFeatures = storeManager.getFeatures();
        checkAndOverwriteTransactionLogConfiguration(combinedConfig, overwrite, storeFeatures);
        checkAndOverwriteSystemManagementLogConfiguration(combinedConfig, overwrite);

        return new MergedConfiguration(overwrite, combinedConfig);
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
        Preconditions.checkArgument(!combinedConfig.has(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG) ||
                combinedConfig.get(KCVSLog.LOG_KEY_CONSISTENT, MANAGEMENT_LOG), "Management LOG must be configured to be key-consistent");
        overwrite.set(KCVSLog.LOG_KEY_CONSISTENT, true, MANAGEMENT_LOG);
        Preconditions.checkArgument(!combinedConfig.has(KCVSLogManager.LOG_FIXED_PARTITION, MANAGEMENT_LOG)
                || combinedConfig.get(KCVSLogManager.LOG_FIXED_PARTITION, MANAGEMENT_LOG), "Fixed partitions must be enabled for management LOG");
        overwrite.set(KCVSLogManager.LOG_FIXED_PARTITION, true, MANAGEMENT_LOG);
    }

}

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

package grakn.core.graph.diskstorage.log.kcvs;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.configuration.ConfigOption;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;
import grakn.core.graph.diskstorage.log.Log;
import grakn.core.graph.diskstorage.log.LogManager;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.configuration.PreInitializeConfigOptions;
import grakn.core.graph.graphdb.database.idassigner.placement.PartitionIDRange;
import grakn.core.graph.graphdb.database.serialize.StandardSerializer;
import grakn.core.graph.util.stats.NumberUtil;
import grakn.core.graph.util.system.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_NS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_STORE_TTL;


/**
 * Implementation of {@link LogManager} against an arbitrary {@link KeyColumnValueStoreManager}. Issues {@link Log} instances
 * which wrap around a {@link KeyColumnValueStore}.
 */
@PreInitializeConfigOptions
public class KCVSLogManager implements LogManager {

    private static final Logger LOG = LoggerFactory.getLogger(KCVSLogManager.class);

    public static final ConfigOption<Boolean> LOG_FIXED_PARTITION = new ConfigOption<>(LOG_NS, "fixed-partition",
            "Whether all LOG entries are written to one fixed partition even if the backend store is partitioned." +
                    "This can cause imbalanced loads and should only be used on low volume logs",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigOption<Integer> LOG_MAX_PARTITIONS = new ConfigOption<Integer>(LOG_NS, "max-partitions",
            "The maximum number of partitions to use for logging. Setting up this many actual or virtual partitions. Must be bigger than 0 and a power of 2.",
            ConfigOption.Type.FIXED, Integer.class, integer -> integer != null && integer > 0 && NumberUtil.isPowerOf2(integer));

    /**
     * If {@link #LOG_MAX_PARTITIONS} isn't set explicitly, the number of partitions is derived by taking the configured
     * {@link GraphDatabaseConfiguration#CLUSTER_MAX_PARTITIONS} and dividing
     * the number by this constant.
     */
    private static final int CLUSTER_SIZE_DIVIDER = 8;

    /**
     * Configuration of this LOG manager
     */
    private final Configuration configuration;
    /**
     * Store Manager against which to open the {@link KeyColumnValueStore}s to wrap the {@link KCVSLog} around.
     */
    final KeyColumnValueStoreManager storeManager;
    /**
     * Id which uniquely identifies this instance. Also see {@link GraphDatabaseConfiguration#UNIQUE_INSTANCE_ID}.
     */
    final String senderId;

    /**
     * The number of first bits of the key that identifies a partition. If this number is X then there are 2^X different
     * partition blocks each of which is identified by a partition id.
     */
    final int partitionBitWidth;
    /**
     * A collection of partition ids to which the logs write in round-robin fashion.
     */
    final int[] defaultWritePartitionIds;
    /**
     * A collection of partition ids from which the readers will read concurrently.
     */
    final int[] readPartitionIds;
    /**
     * Serializer used to (de)-serialize the LOG messages
     */
    final StandardSerializer serializer;

    /**
     * Keeps track of all open logs
     */
    private final Map<String, KCVSLog> openLogs;

    /**
     * The time-to-live of all data in the index store/CF, expressed in seconds.
     */
    private final int indexStoreTTL;

    /**
     * Opens a LOG manager against the provided KCVS store with the given configuration.
     */
    public KCVSLogManager(KeyColumnValueStoreManager storeManager, Configuration config) {
        this(storeManager, config, null);
    }

    /**
     * Opens a LOG manager against the provided KCVS store with the given configuration. Also provided is a list
     * of read-partition-ids. These only apply when readers are registered against an opened LOG. In that case,
     * the readers only read from the provided list of partition ids.
     */
    private KCVSLogManager(KeyColumnValueStoreManager storeManager, Configuration config, int[] readPartitionIds) {
        Preconditions.checkArgument(storeManager != null && config != null);
        if (config.has(LOG_STORE_TTL)) {
            indexStoreTTL = getTTLSeconds(config.get(LOG_STORE_TTL));
            StoreFeatures storeFeatures = storeManager.getFeatures();
            if (storeFeatures.hasCellTTL() && !storeFeatures.hasStoreTTL()) {
                // Reduce cell-level TTL (fine-grained) to store-level TTL (coarse-grained)
                storeManager = new TTLKCVSManager(storeManager);
            } else if (!storeFeatures.hasStoreTTL()) {
                LOG.warn("Log is configured with TTL but underlying storage backend does not support TTL, hence this" +
                        "configuration option is ignored and entries must be manually removed from the backend.");
            }
        } else {
            indexStoreTTL = -1;
        }

        this.storeManager = storeManager;
        this.configuration = config;
        openLogs = new HashMap<>();

        this.senderId = config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID);
        Preconditions.checkNotNull(senderId);

        int maxPartitions;
        if (config.has(LOG_MAX_PARTITIONS)) maxPartitions = config.get(LOG_MAX_PARTITIONS);
        else maxPartitions = Math.max(1, config.get(CLUSTER_MAX_PARTITIONS) / CLUSTER_SIZE_DIVIDER);
        Preconditions.checkArgument(maxPartitions <= config.get(CLUSTER_MAX_PARTITIONS),
                "Number of LOG partitions cannot be larger than number of cluster partitions");
        this.partitionBitWidth = NumberUtil.getPowerOf2(maxPartitions);

        Preconditions.checkArgument(partitionBitWidth >= 0 && partitionBitWidth < 32);
        int numPartitions = (1 << partitionBitWidth);

        //Partitioning
        if (partitionBitWidth > 0 && !config.get(LOG_FIXED_PARTITION)) {
            //Write partitions - default initialization: writing to all partitions
            int[] writePartitions = new int[numPartitions];
            for (int i = 0; i < numPartitions; i++) writePartitions[i] = i;
            if (storeManager.getFeatures().hasLocalKeyPartition()) {
                //Write only to local partitions
                List<Integer> localPartitions = new ArrayList<>();
                try {
                    List<PartitionIDRange> partitionRanges = PartitionIDRange.getIDRanges(partitionBitWidth,
                            storeManager.getLocalKeyPartition());
                    for (PartitionIDRange idRange : partitionRanges) {
                        for (int p : idRange.getAllContainedIDs()) localPartitions.add(p);
                    }
                } catch (Throwable e) {
                    LOG.error("Could not process local id partitions", e);
                }

                if (!localPartitions.isEmpty()) {
                    writePartitions = ArrayUtils.toPrimitive(localPartitions.toArray(new Integer[localPartitions.size()]));
                }
            }
            this.defaultWritePartitionIds = writePartitions;
            //Read partitions
            if (readPartitionIds != null && readPartitionIds.length > 0) {
                for (int readPartitionId : readPartitionIds) {
                    checkValidPartitionId(readPartitionId, partitionBitWidth);
                }
                this.readPartitionIds = Arrays.copyOf(readPartitionIds, readPartitionIds.length);
            } else {
                this.readPartitionIds = new int[numPartitions];
                for (int i = 0; i < numPartitions; i++) this.readPartitionIds[i] = i;
            }
        } else {
            this.defaultWritePartitionIds = new int[]{0};
            Preconditions.checkArgument(readPartitionIds == null || (readPartitionIds.length == 0 && readPartitionIds[0] == 0),
                    "Cannot configure read partition ids on unpartitioned backend or with fixed partitions enabled");
            this.readPartitionIds = new int[]{0};
        }

        this.serializer = new StandardSerializer();
    }

    private static void checkValidPartitionId(int partitionId, int partitionBitWidth) {
        Preconditions.checkArgument(partitionId >= 0 && partitionId < (1 << partitionBitWidth));
    }

    private static int getTTLSeconds(Duration duration) {
        Preconditions.checkArgument(duration != null && !duration.isZero(), "Must provide non-zero TTL");
        long ttlSeconds = Math.max(1, duration.getSeconds());
        Preconditions.checkArgument(ttlSeconds <= Integer.MAX_VALUE, "tll value is too large [%s] - value overflow", duration);
        return (int) ttlSeconds;
    }

    @Override
    public synchronized KCVSLog openLog(String name) throws BackendException {
        if (openLogs.containsKey(name)) return openLogs.get(name);
        StoreMetaData.Container storeOptions = new StoreMetaData.Container();
        if (0 < indexStoreTTL) {
            storeOptions.put(StoreMetaData.TTL, indexStoreTTL);
        }
        KCVSLog log = new KCVSLog(name, this, storeManager.openDatabase(name, storeOptions), configuration);
        openLogs.put(name, log);
        return log;
    }

    /**
     * Must be triggered by a particular {@link KCVSLog} when it is closed so that this LOG can be removed from the list
     * of open logs.
     */
    synchronized void closedLog(KCVSLog log) {
        KCVSLog l = openLogs.remove(log.getName());
    }

    @Override
    public synchronized void close() throws BackendException {
        /* Copying the map is necessary to avoid ConcurrentModificationException.
         * The path to ConcurrentModificationException in the absence of a copy is
         * LOG.close() -> manager.closedLog(LOG) -> openLogs.remove(LOG.getName()).
         */
        for (KCVSLog log : ImmutableMap.copyOf(openLogs).values()) log.close();

        IOUtils.closeQuietly(serializer);
    }

}

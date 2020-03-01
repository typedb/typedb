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

package grakn.core.graph.diskstorage.log.kcvs;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.StoreMetaData;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.diskstorage.keycolumnvalue.ttl.TTLKCVSManager;
import grakn.core.graph.diskstorage.log.LogManager;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.graphdb.database.idassigner.placement.PartitionIDRange;
import grakn.core.graph.graphdb.database.serialize.StandardSerializer;
import grakn.core.graph.util.stats.NumberUtil;
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
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_FIXED_PARTITION;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_MAX_PARTITIONS;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.LOG_STORE_TTL;


/**
 * Implementation of LogManager against an arbitrary KeyColumnValueStoreManager. Issues Log instances
 * which wrap around a KeyColumnValueStore.
 */
public class KCVSLogManager implements LogManager {

    private static final Logger LOG = LoggerFactory.getLogger(KCVSLogManager.class);

    /**
     * If LOG_MAX_PARTITIONS isn't set explicitly, the number of partitions is derived by taking the configured
     * GraphDatabaseConfiguration#CLUSTER_MAX_PARTITIONS and dividing
     * the number by this constant.
     */
    private static final int CLUSTER_SIZE_DIVIDER = 8;

    /**
     * Configuration of this LOG manager
     */
    private final Configuration configuration;
    /**
     * Store Manager against which to open the KeyColumnValueStores to wrap the KCVSLog around.
     */
    final KeyColumnValueStoreManager storeManager;
    /**
     * Id which uniquely identifies this instance. Also see GraphDatabaseConfiguration#UNIQUE_INSTANCE_ID.
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
     * Must be triggered by a particular KCVSLog when it is closed so that this LOG can be removed from the list
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
    }

}

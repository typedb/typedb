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

package grakn.core.graph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.graphdb.database.idassigner.IDPoolExhaustedException;
import grakn.core.graph.graphdb.internal.InternalElement;
import grakn.core.graph.graphdb.internal.InternalVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.CONCURRENT_PARTITIONS;

/**
 * A id placement strategy that assigns all vertices created in a transaction
 * to the same partition id. The partition id is selected randomly from a set
 * of partition ids that are retrieved upon initialization.
 * <p>
 * The number of partition ids to choose from is configurable.
 */
public class SimpleBulkPlacementStrategy implements IDPlacementStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleBulkPlacementStrategy.class);

    public static final int PARTITION_FINDING_ATTEMPTS = 1000;

    private final Random random = new Random();

    private final int[] currentPartitions;
    private List<PartitionIDRange> localPartitionIdRanges;
    private final Set<Integer> exhaustedPartitions;

    public SimpleBulkPlacementStrategy(int concurrentPartitions) {
        Preconditions.checkArgument(concurrentPartitions > 0);
        currentPartitions = new int[concurrentPartitions];
        exhaustedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public SimpleBulkPlacementStrategy(Configuration config) {
        this(config.get(CONCURRENT_PARTITIONS));
    }

    private int nextPartitionID() {
        return currentPartitions[random.nextInt(currentPartitions.length)];
    }

    private void updateElement(int index) {
        Preconditions.checkArgument(localPartitionIdRanges != null && !localPartitionIdRanges.isEmpty(), "Local partition id ranges have not been initialized");
        int newPartition;
        int attempts = 0;
        do {
            attempts++;
            newPartition = localPartitionIdRanges.get(random.nextInt(localPartitionIdRanges.size())).getRandomID();
            if (attempts > PARTITION_FINDING_ATTEMPTS) {
                throw new IDPoolExhaustedException("Could not find non-exhausted partition");
            }
        } while (exhaustedPartitions.contains(newPartition));
        currentPartitions[index] = newPartition;
        LOG.debug("Setting partition at index [{}] to: {}", index, newPartition);
    }

    @Override
    public int getPartition(InternalElement element) {
        return nextPartitionID();
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        int partitionID = nextPartitionID();
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            entry.setValue(new SimplePartitionAssignment(partitionID));
        }
    }

    @Override
    public boolean supportsBulkPlacement() {
        return true;
    }

    @Override
    public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges) {
        Preconditions.checkArgument(localPartitionIdRanges != null && !localPartitionIdRanges.isEmpty());
        this.localPartitionIdRanges = Lists.newArrayList(localPartitionIdRanges); //copy
        for (int i = 0; i < currentPartitions.length; i++) {
            updateElement(i);
        }
    }

    public boolean isExhaustedPartition(int partitionID) {
        return exhaustedPartitions.contains(partitionID);
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        exhaustedPartitions.add(partitionID);
        for (int i = 0; i < currentPartitions.length; i++) {
            if (currentPartitions[i] == partitionID) {
                updateElement(i);
            }
        }
    }
}

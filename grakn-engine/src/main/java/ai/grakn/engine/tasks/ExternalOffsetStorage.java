/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.tasks;

import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.tasks.config.ZookeeperPaths.PARTITION_PATH;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;

/**
 * Store Kakfa offsets externally in Zookeeper for exactly once processing
 *
 * @author alexandraorth
 */
public class ExternalOffsetStorage {

    private final static Logger LOG = LoggerFactory.getLogger(ExternalOffsetStorage.class);
    private final ZookeeperConnection zookeeper;

    public ExternalOffsetStorage(ZookeeperConnection zookeeper){
        this.zookeeper = zookeeper;
    }

    /**
     * Get the offset from Zookeeper for the given partition.
     * @param partition Partition to get the offset of.
     * @return The offset for the given partition.
     */
    public long getOffset(TopicPartition partition) {
        try {
            String partitionPath = getPartitionPath(partition);
            long offset = (long) deserialize(zookeeper.connection().getData().forPath(partitionPath));

            LOG.debug("Offset {} read for partition %{}", partitionPath, partitionPath);

            return offset;
        } catch (RuntimeException e) {
            throw new EngineStorageException(e);
        } catch (Exception e){
            throw new EngineStorageException("Error retrieving offset");
        }
    }

    /**
     * Save the offset of the given partition in this consumer in zookeeper.
     *
     * @param partition Partition to save the offset of.
     */
    public void saveOffset(Consumer consumer, TopicPartition partition){
        try {
            long currentOffset = consumer.position(partition);
            String partitionPath = getPartitionPath(partition);

            LOG.debug("Offset at {} writing for partition {}", currentOffset, partitionPath);
            try {
                zookeeper.connection().setData()
                        .forPath(partitionPath, serialize(currentOffset));
            } catch(KeeperException.NoNodeException e) {
                zookeeper.connection().create()
                        .creatingParentContainersIfNeeded()
                        .forPath(partitionPath, serialize(currentOffset));
            }
        } catch (RuntimeException e) {
            throw new EngineStorageException(e);
        } catch (Exception e){
            throw new EngineStorageException("Error saving offset");
        }
    }

    /**
     * Get ZK path for an identifier of the given topic partition.
     * @param partition The topic partition to identify.
     * @return Unique identifier for the given partition.
     */
    private static String getPartitionPath(TopicPartition partition){
        String identifier = partition.topic() + partition.partition();
        return String.format(PARTITION_PATH, identifier);
    }
}

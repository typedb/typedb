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
 *
 */

package ai.grakn.engine.tasks.manager;

import ai.grakn.exception.EngineStorageException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static java.lang.String.format;
import static ai.grakn.engine.tasks.config.ZookeeperPaths.PARTITION_PATH;
import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;

/**
 * Class that will store the last offsets after a rebalance in an external storage (zookeeper)
 *
 * @author alexandraorth
 */
public class ExternalStorageRebalancer implements ConsumerRebalanceListener {

    private final static Logger LOG = LoggerFactory.getLogger(ExternalStorageRebalancer.class);

    private final ZookeeperConnection zookeeper;
    private final KafkaConsumer consumer;

    public ExternalStorageRebalancer(KafkaConsumer consumer, ZookeeperConnection zookeeper){
        this.zookeeper = zookeeper;
        this.consumer = consumer;
    }

    /**
     * Get the offset of the new partition from the external store.
     * Seek the consumer to that point and then delete.
     * @param partitions Partitions assigned that used to re-set the consumers.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.debug(format("Consumer partitions assigned %s", partitions));

        for(TopicPartition partition : partitions){
            consumer.seek(partition, getOffsetFromZookeeper(partition));
            deleteOffsetFromZookeeper(partition);
        }
    }

    /**
     * Save the offset of the current partition in the external store.
     * @param partitions Partitions that were revoked to save the offsets of.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.debug(format("Consumer partitions revoked %s", partitions));

        partitions.forEach(this::saveOffsetInZookeeper);
    }


    /**
     * Get the offset from Zookeeper for the given partition.
     * @param partition Partition to get the offset of.
     * @return The offset for the given partition.
     */
    private long getOffsetFromZookeeper(TopicPartition partition) {
        try {
            String partitionPath = getPartitionPath(partition);
            long offset = (long) deserialize(zookeeper.connection().getData().forPath(partitionPath));

            LOG.debug(format("Offset %s read for partition %s", partitionPath, partitionPath));

            return offset;
        } catch (Exception e){
            throw new EngineStorageException("Error retrieving offset");
        }
    }

    /**
     * Save the offset of the given partition in this consumer in zookeeper.
     *
     * @param partition Partition to save the offset of.
     */
    private void saveOffsetInZookeeper(TopicPartition partition){
        try {
            long currentOffset = consumer.position(partition);
            String partitionPath = getPartitionPath(partition);

            LOG.debug(format("Offset at %s writing for partition %s", currentOffset, partitionPath));
            zookeeper.connection().create()
                    .creatingParentContainersIfNeeded()
                    .forPath(partitionPath, serialize(currentOffset));
        } catch (Exception e){
            throw new EngineStorageException("Error saving offset");
        }
    }

    /**
     * Delete any offset saved for the given partition from zookeeper.
     *
     * @param partition Partition to delete the offset of.
     */
    private void deleteOffsetFromZookeeper(TopicPartition partition){
        try {
            String partitionPath = getPartitionPath(partition);

            LOG.debug(format("Offset at %s deleting", partitionPath));
            zookeeper.connection().delete()
                    .forPath(partitionPath);
        } catch (Exception e){
            throw new EngineStorageException("Error deleting offset");
        }
    }

    /**
     * Get ZK path for an identifier of the given topic partition.
     * @param partition The topic partition to identify.
     * @return Unique identifier for the given partition.
     */
    private String getPartitionPath(TopicPartition partition){
        String identifier = partition.topic() + partition.partition();
        return format(PARTITION_PATH, identifier);
    }
}

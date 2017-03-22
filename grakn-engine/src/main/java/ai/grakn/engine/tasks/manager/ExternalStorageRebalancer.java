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

import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.exception.EngineStorageException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Class that will store the last offsets after a rebalance in an external storage (zookeeper)
 *
 * @author alexandraorth
 */
public class ExternalStorageRebalancer implements ConsumerRebalanceListener {

    private final static Logger LOG = LoggerFactory.getLogger(ExternalStorageRebalancer.class);

    private final Consumer consumer;
    private final ExternalOffsetStorage externalOffsetStorage;

    private ExternalStorageRebalancer(Consumer consumer, ExternalOffsetStorage externalOffsetStorage){
        this.consumer = consumer;
        this.externalOffsetStorage = externalOffsetStorage;
    }

    public static ExternalStorageRebalancer rebalanceListener(Consumer consumer, ExternalOffsetStorage externalOffsetStorage){
        return new ExternalStorageRebalancer(consumer, externalOffsetStorage);
    }

    /**
     * Get the offset of the new partition from the external store.
     * Seek the consumer to that point and then delete.
     * @param partitions Partitions assigned that used to re-set the consumers.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.debug("Consumer partitions assigned {}", partitions);

        for(TopicPartition partition : partitions){
            try {
                consumer.seek(partition, externalOffsetStorage.getOffset(partition));
            } catch (EngineStorageException e){
                consumer.seekToBeginning(Collections.singletonList(partition));
                LOG.debug("Could not retrieve offset for partition {}, seeking to beginning", partition);
            } finally {
                consumer.commitSync();
            }
        }
    }

    /**
     * Save the offset of the current partition in the external store.
     * @param partitions Partitions that were revoked to save the offsets of.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.debug("Consumer partitions revoked {}", partitions);

        for (TopicPartition partition : partitions) {
            try {
                externalOffsetStorage.saveOffset(consumer, partition);
            } catch (EngineStorageException e) {
                LOG.error("Error saving offset in Zookeeper", e);
            }
        }
    }
}

/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.engine.backgroundtasks.distributed;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final KafkaLogger LOG = KafkaLogger.getInstance();
    private KafkaConsumer consumer;

    public RebalanceListener(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.debug("Partitions assigned: " + partitions);
        consumer.commitSync();
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.debug("Partitions revoked: " + partitions);
        consumer.commitSync();
    }
}

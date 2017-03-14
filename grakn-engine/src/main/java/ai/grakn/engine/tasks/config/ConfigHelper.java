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

package ai.grakn.engine.tasks.config;

import ai.grakn.engine.tasks.TaskIdDeserializer;
import ai.grakn.engine.tasks.TaskIdSerializer;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateDeserializer;
import ai.grakn.engine.tasks.TaskStateSerializer;
import ai.grakn.engine.GraknEngineConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * <p>
 * Class containing helper methods to retrieve the default configuration for
 * Zookeeper and Kafka consumers & producers
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 * //TODO Refactor this class
 */
public class ConfigHelper {

    public static Consumer<TaskId, TaskState> kafkaConsumer(String groupId) {
        Properties properties = new Properties();
        properties.putAll(GraknEngineConfig.getInstance().getProperties());

        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("metadata.max.age.ms", "1000");

        // The max poll time should be set to its largest value
        // Our task runners will only poll again after the tasks have completed
        properties.put("max.poll.interval.ms", Integer.MAX_VALUE);

        // Max poll records should be set to one: each TaskRunner should only handle one record at a time
        properties.put("max.poll.records", "1");

        return new KafkaConsumer<>(properties, new TaskIdDeserializer(), new TaskStateDeserializer());
    }

    public static Producer<TaskId, TaskState> kafkaProducer() {
        Properties properties = new Properties();
        properties.putAll(GraknEngineConfig.getInstance().getProperties());

        return new KafkaProducer<>(properties, new TaskIdSerializer(), new TaskStateSerializer());
    }
}

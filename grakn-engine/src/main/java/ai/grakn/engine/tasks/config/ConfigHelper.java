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

import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.engine.tasks.TaskConfigurationDeserializer;
import ai.grakn.engine.tasks.TaskConfigurationSerializer;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateDeserializer;
import ai.grakn.engine.tasks.TaskStateSerializer;
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

    public static Consumer<TaskState, TaskConfiguration> kafkaConsumer(String groupId, Properties properties) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);

        newProperties.put("group.id", groupId);
        newProperties.put("enable.auto.commit", "false");
        newProperties.put("auto.offset.reset", "earliest");
        newProperties.put("metadata.max.age.ms", "1000");

        // The max poll time should be set to its largest value
        // Our task runners will only poll again after the tasks have completed
        //TODO Make this number more reasonable
        newProperties.put("max.poll.interval.ms", Integer.MAX_VALUE);

        // Max poll records should be set to one: each TaskRunner should only handle one record at a time
        newProperties.put("max.poll.records", "1");

        return new KafkaConsumer<>(newProperties, new TaskStateDeserializer(), new TaskConfigurationDeserializer());
    }

    public static Producer<TaskState, TaskConfiguration> kafkaProducer(Properties properties) {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);

        return new KafkaProducer<>(newProperties, new TaskStateSerializer(), new TaskConfigurationSerializer());
    }
}

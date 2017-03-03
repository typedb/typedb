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

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.util.ConfigProperties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

import static ai.grakn.engine.util.ConfigProperties.KAFKA_BATCH_SIZE;
import static ai.grakn.engine.util.ConfigProperties.KAFKA_BOOTSTRAP_SERVERS;
import static ai.grakn.engine.util.ConfigProperties.KAFKA_BUFFER_MEM;
import static ai.grakn.engine.util.ConfigProperties.KAFKA_LINGER_MS;
import static ai.grakn.engine.util.ConfigProperties.KAFKA_RETRIES;
import static ai.grakn.engine.util.ConfigProperties.KAFKA_SESSION_TIMEOUT;
import static ai.grakn.engine.util.ConfigProperties.ZK_BACKOFF_BASE_SLEEP_TIME;
import static ai.grakn.engine.util.ConfigProperties.ZK_BACKOFF_MAX_RETRIES;
import static ai.grakn.engine.util.ConfigProperties.ZK_CONNECTION_TIMEOUT;
import static ai.grakn.engine.util.ConfigProperties.ZK_SERVERS;
import static ai.grakn.engine.util.ConfigProperties.ZK_SESSION_TIMEOUT;

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

    public static CuratorFramework client() {
        int sleep = ConfigProperties.getInstance().getPropertyAsInt(ZK_BACKOFF_BASE_SLEEP_TIME);
        int retries = ConfigProperties.getInstance().getPropertyAsInt(ZK_BACKOFF_MAX_RETRIES);

        return CuratorFrameworkFactory.builder()
                    .connectString(ConfigProperties.getInstance().getProperty(ZK_SERVERS))
                    .namespace(ZookeeperPaths.TASKS_NAMESPACE)
                    .sessionTimeoutMs(ConfigProperties.getInstance().getPropertyAsInt(ZK_SESSION_TIMEOUT))
                    .connectionTimeoutMs(ConfigProperties.getInstance().getPropertyAsInt(ZK_CONNECTION_TIMEOUT))
                    .retryPolicy(new ExponentialBackoffRetry(sleep, retries))
                    .build();
    }

    public static Consumer<TaskId, TaskState> kafkaConsumer(String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigProperties.getInstance().getProperty(KAFKA_BOOTSTRAP_SERVERS));
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("metadata.max.age.ms", "1000");
        properties.put("session.timeout.ms", ConfigProperties.getInstance().getProperty(KAFKA_SESSION_TIMEOUT));
        properties.put("key.serializer", "ai.grakn.engine.tasks.TaskIdSerializer");
        properties.put("key.deserializer", "ai.grakn.engine.tasks.TaskIdDeserializer");
        properties.put("value.serializer", "ai.grakn.engine.tasks.TaskStateSerializer");
        properties.put("value.deserializer", "ai.grakn.engine.tasks.TaskStateDeserializer");

        // The max poll time should be set to its largest value
        // Our task runners will only poll again after the tasks have completed
        properties.put("max.poll.interval.ms", Integer.MAX_VALUE);

        // Max poll records should be set to one: each TaskRunner should only handle one record at a time
        properties.put("max.poll.records", "1");

        return new KafkaConsumer<>(properties);
    }

    public static Producer<TaskId, TaskState> kafkaProducer() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", ConfigProperties.getInstance().getProperty(KAFKA_BOOTSTRAP_SERVERS));
        properties.put("acks", "all");
        properties.put("retries", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_RETRIES));
        properties.put("batch.size", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_BATCH_SIZE));
        properties.put("linger.ms", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_LINGER_MS));
        properties.put("buffer.memory", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_BUFFER_MEM));
        properties.put("key.serializer", "ai.grakn.engine.tasks.TaskIdSerializer");
        properties.put("key.deserializer", "ai.grakn.engine.tasks.TaskIdDeserializer");
        properties.put("value.serializer", "ai.grakn.engine.tasks.TaskStateSerializer");
        properties.put("value.deserializer", "ai.grakn.engine.tasks.TaskStateDeserializer");

        return new KafkaProducer<>(properties);
    }
}

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

package ai.grakn.engine.backgroundtasks.config;

import ai.grakn.engine.util.ConfigProperties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

import static ai.grakn.engine.util.ConfigProperties.*;

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

    public static <K,V> KafkaConsumer<K, V> kafkaConsumer(String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", ConfigProperties.getInstance().getProperty(KAFKA_BOOTSTRAP_SERVERS));
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", false);
        properties.put("session.timeout.ms", ConfigProperties.getInstance().getProperty(KAFKA_SESSION_TIMEOUT));
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(properties);
    }

    public static <K,V> KafkaProducer<K, V> kafkaProducer() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", ConfigProperties.getInstance().getProperty(KAFKA_BOOTSTRAP_SERVERS));
        properties.put("acks", "all");
        properties.put("retries", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_RETRIES));
        properties.put("batch.size", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_BATCH_SIZE));
        properties.put("linger.ms", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_LINGER_MS));
        properties.put("buffer.memory", ConfigProperties.getInstance().getPropertyAsInt(KAFKA_BUFFER_MEM));
        properties.put("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);
    }
}

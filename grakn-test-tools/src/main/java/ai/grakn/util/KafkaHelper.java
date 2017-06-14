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

package ai.grakn.util;

import info.batey.kafka.unit.KafkaUnit;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *     Starts Embedded Kafka
 * </p>
 *
 * <p>
 *     Helper class for starting and working with an embedded kafka.
 *     This should be used for testing purposes only
 * </p>
 *
 * @author fppt
 *
 */
public class KafkaHelper {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(KafkaHelper.class);
    private static AtomicInteger KAFKA_COUNTER = new AtomicInteger(0);
    private static KafkaUnit kafkaUnit = new KafkaUnit(2181, 9092);

    /**
     *
     * @param numThreads The number of threads availiable to Kafka. This maps to the created kafka partitions
     * @param queues The names of the kafka queues to create
     */
    public static void startEmbedded(int numThreads, String ... queues){
        // Clean-up ironically uses a lot of memory
        if (KAFKA_COUNTER.getAndIncrement() == 0) {
            LOG.info("Starting kafka...");
            kafkaUnit.setKafkaBrokerConfig("log.cleaner.enable", "false");
            kafkaUnit.startup();
            for (String queue : queues) {
                kafkaUnit.createTopic(queue, numThreads * 2);
            }
            LOG.info("Kafka started.");
        }
    }

    /**
     * Stops the embedded kafka
     */
    public static void stopEmbedded(){
        if (KAFKA_COUNTER.decrementAndGet() == 0) {
            LOG.info("Stopping kafka...");
            kafkaUnit.shutdown();
            LOG.info("Kafka stopped.");
        }
    }
}

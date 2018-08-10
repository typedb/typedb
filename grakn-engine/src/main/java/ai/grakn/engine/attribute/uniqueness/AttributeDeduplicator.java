/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.attribute.uniqueness.queue.Queue;
import ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

import static ai.grakn.engine.attribute.uniqueness.DeduplicationAlgorithm.deduplicate;

/**
 * This class is responsible for merging attribute duplicates which is done in order to maintain attribute uniqueness.
 * It is an always-on background thread which continuously deduplicate duplicates. This means that eventually,
 * every attribute instance will have a unique value, as the duplicates will have been removed.
 * A new incoming attribute will immediately trigger the deduplicate operation, meaning that duplicates are merged in almost real-time speed.
 * It is fault-tolerant and will re-process incoming attributes if Grakn crashes during a deduplicate process.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class AttributeDeduplicator {
    private static Logger LOG = LoggerFactory.getLogger(AttributeDeduplicator.class);
    private static final int QUEUE_GET_BATCH_MAX = 1000;
    private static final Path queuePathRelative = Paths.get("queue"); // path to the queue storage location, relative to the data directory

    private EngineGraknTxFactory txFactory;
    private Queue queue;

    private boolean stopDaemon = false;

    /**
     * Instantiates {@link AttributeDeduplicator}
     * @param config a reference to an instance of {@link GraknConfig} which is initialised from a grakn.properties.
     * @param txFactory an {@link EngineGraknTxFactory} instance which provides access to write into the database
     */
    public AttributeDeduplicator(GraknConfig config, EngineGraknTxFactory txFactory) {
        Path dataDir = Paths.get(config.getProperty(GraknConfigKey.DATA_DIR));
        Path queuePathAbsolute = dataDir.resolve(queuePathRelative);
        this.queue = new RocksDbQueue(queuePathAbsolute);
        this.txFactory = txFactory;

    }

    /**
     * Inserts an attribute to the queue. The attribute must have been inserted to the database prior to calling this method.
     * @param keyspace keyspace of the attribute
     * @param value the value of the attribute
     * @param conceptId the concept id of the attribute
     */
    public void insertAttribute(Keyspace keyspace, String value, ConceptId conceptId) {
        final Attribute newAttribute = Attribute.create(keyspace, value, conceptId);
        LOG.info("insert(" + newAttribute + ")");
        queue.insert(newAttribute);
    }

    /**
     * Starts a daemon which continuously deduplicate attribute duplicates.
     * The thread listens to the {@link RocksDbQueue} queue for incoming attributes and applies
     * the deduplicate algorithm as implemented in the {@link DeduplicationAlgorithm} class.
     *
     */
    public CompletableFuture<Void> startDaemon() {
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDaemon() - starting attribute uniqueness daemon...");
            while (!stopDaemon) {
                try {
                    LOG.info("startDaemon() - attribute uniqueness daemon started.");
                    Attributes newAttrs = queue.read(QUEUE_GET_BATCH_MAX);
                    deduplicate(txFactory, newAttrs);
                    queue.ack(newAttrs);
                }
                catch (InterruptedException e) {
                    LOG.error("deduplicate() failed with an exception. ", e);
                }
            }
            LOG.info("startDaemon() - attribute uniqueness daemon stopped");
            return null;
        });

        daemon.exceptionally(e -> {
            LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
            return null;
        });

        return daemon;
    }

    /**
     * Stops the attribute uniqueness daemon
     */
    public void stopDaemon() {
        LOG.info("stopDaemon() - stopping the attribute uniqueness daemon...");
        stopDaemon = true;
    }
}


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

import static ai.grakn.engine.attribute.uniqueness.MergeAlgorithm.merge;

/**
 * This class is responsible for merging attribute duplicates which is done in order to maintain attribute uniqueness.
 * It is an always-on background thread which continuously merge duplicates. This means that eventually,
 * every attribute instance will have a unique value, as the duplicates will have been removed.
 * A new incoming attribute will immediately trigger the merge operation, meaning that duplicates are merged in almost real-time speed.
 * It is fault-tolerant and will re-process incoming attributes if Grakn crashes during a merge process.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class AttributeUniqueness {
    private static Logger LOG = LoggerFactory.getLogger(AttributeUniqueness.class);
    private static final int QUEUE_GET_BATCH_MAX = 1000;
    private static final Path relQueuePath = Paths.get("queue"); // path to the queue storage location, relative to the data directory

    private EngineGraknTxFactory txFactory;
    private Queue newAttributeQueue;

    private boolean stopDaemon = false;

    /**
     * Instantiates {@link AttributeUniqueness}
     * @param config a reference to an instance of {@link GraknConfig} which is initialised from a grakn.properties.
     * @param txFactory an {@link EngineGraknTxFactory} instance which provides access to write into the database
     */
    public AttributeUniqueness(GraknConfig config, EngineGraknTxFactory txFactory) {
        Path dataDir = Paths.get(config.getProperty(GraknConfigKey.DATA_DIR));
        Path absQueuePath = dataDir.resolve(relQueuePath);
        this.newAttributeQueue = new RocksDbQueue(absQueuePath);
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
        LOG.info("insertAttribute(" + newAttribute + ")");
        newAttributeQueue.insertAttribute(newAttribute);
    }

    /**
     * Starts a daemon which continuously merge attribute duplicates.
     * The thread listens to the {@link RocksDbQueue} queue for incoming attributes and applies
     * the merge algorithm as implemented in the {@link MergeAlgorithm} class.
     *
     */
    public CompletableFuture<Void> startDaemon() {
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDaemon() - starting attribute uniqueness daemon...");
            while (!stopDaemon) {
                try {
                    LOG.info("startDaemon() - attribute uniqueness daemon started.");
                    Attributes newAttrs = newAttributeQueue.readAttributes(QUEUE_GET_BATCH_MAX);
                    merge(txFactory, newAttrs);
                    newAttributeQueue.ackAttributes(newAttrs);
                }
                catch (InterruptedException e) {
                    LOG.error("merge() failed with an exception. ", e);
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


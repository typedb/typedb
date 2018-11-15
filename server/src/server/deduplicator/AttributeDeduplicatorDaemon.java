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

package grakn.core.server.deduplicator;

import grakn.core.util.GraknConfigKey;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.graql.concept.ConceptId;
import grakn.core.util.GraknConfig;
import grakn.core.server.deduplicator.queue.Attribute;
import grakn.core.server.deduplicator.queue.RocksDbQueue;
import grakn.core.server.session.SessionStore;
import grakn.core.server.session.TransactionImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static grakn.core.server.deduplicator.AttributeDeduplicator.deduplicate;

/**
 * This class is responsible for de-duplicating attributes. It is done to ensure that every attribute in Grakn stays unique.
 *
 * Marking an attribute for deduplication:
 * When the {@link TransactionImpl#commit()} is invoked, it will trigger the {@link #markForDeduplication(Keyspace, String, ConceptId)}
 * which inserts the attribute to an internal queue for deduplication.
 *
 * De-duplicating attributes in the de-duplicator daemon:
 * The de-duplicator daemon is an always-on background thread which performs deduplication on incoming attributes.
 * When a new attribute is inserted, it will immediately trigger the deduplicate operation, meaning that duplicates are merged in almost real-time speed.
 * The daemon is started and stopped with {@link #startDeduplicationDaemon()} and {@link #stopDeduplicationDaemon()}
 *
 * Fault tolerance:
 * The de-duplicator daemon is fault-tolerant and will re-process incoming attributes if Grakn crashes in the middle of a deduplication.
 *
 */
public class AttributeDeduplicatorDaemon {
    private static Logger LOG = LoggerFactory.getLogger(AttributeDeduplicatorDaemon.class);
    private static final int QUEUE_GET_BATCH_MAX = 1000;
    private static final Path queueDataDirRelative = Paths.get("queue"); // path to the queue storage location, relative to the data directory

    private ExecutorService executorServiceForDaemon = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("attribute-deduplicator-daemon-%d").build());

    private SessionStore txFactory;
    private RocksDbQueue queue;

    private boolean stopDaemon = false;

    /**
     * Instantiates {@link AttributeDeduplicatorDaemon}
     * @param config a reference to an instance of {@link GraknConfig} which is initialised from a grakn.properties.
     * @param txFactory an {@link SessionStore} instance which provides access to write into the database
     */
    public AttributeDeduplicatorDaemon(GraknConfig config, SessionStore txFactory) {
        Path dataDir = Paths.get(config.getProperty(GraknConfigKey.DATA_DIR));
        Path queueDataDir = dataDir.resolve(queueDataDirRelative);
        this.queue = new RocksDbQueue(queueDataDir);
        this.txFactory = txFactory;
    }

    /**
     * Marks an attribute for deduplication. The attribute will be inserted to an internal queue to be processed by the de-duplicator daemon in real-time.
     * The attribute must have been inserted to the database, prior to calling this method.
     *
     * @param keyspace keyspace of the attribute
     * @param index the value of the attribute
     * @param conceptId the concept id of the attribute
     */
    public void markForDeduplication(Keyspace keyspace, String index, ConceptId conceptId) {
        Attribute attribute = Attribute.create(keyspace, index, conceptId);
        LOG.trace("insert(" + attribute + ")");
        queue.insert(attribute);
    }

    /**
     * Starts a daemon which performs deduplication on incoming attributes in real-time.
     * The thread listens to the {@link RocksDbQueue} queue for incoming attributes and applies
     * the {@link AttributeDeduplicator#deduplicate(SessionStore, KeyspaceIndexPair)} algorithm.
     *
     */
    public CompletableFuture<Void> startDeduplicationDaemon() {
        stopDaemon = false;
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDeduplicationDaemon() - attribute de-duplicator daemon started.");
            while (!stopDaemon) {
                try {
                    List<Attribute> attributes = queue.read(QUEUE_GET_BATCH_MAX);

                    LOG.trace("starting a new batch to process these new attributes: " + attributes);

                    // group the attributes into a set of unique (keyspace -> value) pair
                    Set<KeyspaceIndexPair> uniqueKeyValuePairs = attributes.stream()
                            .map(attr -> KeyspaceIndexPair.create(attr.keyspace(), attr.index()))
                            .collect(Collectors.toSet());

                    // perform deduplicate for each (keyspace -> value)
                    for (KeyspaceIndexPair keyspaceIndexPair : uniqueKeyValuePairs) {
                        deduplicate(txFactory, keyspaceIndexPair);
                    }

                    LOG.trace("new attributes processed.");

                    queue.ack(attributes);
                }
                catch (InterruptedException | RuntimeException e) {
                    LOG.error("An exception has occurred in the attribute de-duplicator daemon. ", e);
                }
            }
            LOG.info("startDeduplicationDaemon() - attribute de-duplicator daemon stopped");
            return null;
        }, executorServiceForDaemon);

        daemon.exceptionally(e -> {
            LOG.error("An unhandled exception has occurred in the attribute de-duplicator daemon. ", e);
            return null;
        });

        return daemon;
    }

    /**
     * Stops the attribute uniqueness daemon
     */
    public void stopDeduplicationDaemon() {
        LOG.info("stopDeduplicationDaemon() - stopping the attribute de-duplicator daemon...");
        stopDaemon = true;
    }
}


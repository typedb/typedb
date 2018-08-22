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
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class is responsible for de-duplicating attributes. It is done to ensure that every attribute in Grakn stays unique.
 *
 * Marking an attribute for deduplication:
 * When the {@link EmbeddedGraknTx#commit()} is invoked, it will trigger the {@link #markForDeduplication(Keyspace, String, ConceptId)}
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
 * @author Ganeshwara Herawan Hananda
 */
public class AttributeDeduplicator {
    private static Logger LOG = LoggerFactory.getLogger(AttributeDeduplicator.class);
    private static final int QUEUE_GET_BATCH_MAX = 1000;
    private static final Path queueDataDirRelative = Paths.get("queue"); // path to the queue storage location, relative to the data directory

    private EngineGraknTxFactory txFactory;
    private RocksDbQueue queue;

    private boolean stopDaemon = false;

    /**
     * Instantiates {@link AttributeDeduplicator}
     * @param config a reference to an instance of {@link GraknConfig} which is initialised from a grakn.properties.
     * @param txFactory an {@link EngineGraknTxFactory} instance which provides access to write into the database
     */
    public AttributeDeduplicator(GraknConfig config, EngineGraknTxFactory txFactory) {
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
     * @param value the value of the attribute
     * @param conceptId the concept id of the attribute
     */
    public void markForDeduplication(Keyspace keyspace, String value, ConceptId conceptId) {
        Attribute attribute = Attribute.create(keyspace, value, conceptId);
        LOG.info("insert(" + attribute + ")");
        queue.insert(attribute);
    }

    /**
     * Starts a daemon which performs deduplication on incoming attributes in real-time.
     * The thread listens to the {@link RocksDbQueue} queue for incoming attributes and applies
     * the {@link #deduplicate(Set)} algorithm.
     *
     */
    public CompletableFuture<Void> startDeduplicationDaemon() {
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDeduplicationDaemon() - attribute de-duplicator daemon started.");
            while (!stopDaemon) {
                try {
                    List<Attribute> attributes = queue.read(QUEUE_GET_BATCH_MAX);

                    LOG.info("starting a new batch to process these new attributes: " + attributes);

                    // group the attributes into a set of unique (keyspace -> value) pair
                    Set<KeyspaceIndexPair> uniqueKeyValuePairs = attributes.stream()
                            .map(attr -> KeyspaceIndexPair.create(attr.keyspace(), attr.value()))
                            .collect(Collectors.toSet());

                    // perform deduplicate for each (keyspace -> value)
                    deduplicate(txFactory, uniqueKeyValuePairs);

                    LOG.info("new attributes processed.");

                    queue.ack(attributes);
                }
                catch (InterruptedException | RuntimeException e) {
                    LOG.error("An exception has occurred in the attribute de-duplicator daemon. ", e);
                }
            }
            LOG.info("startDeduplicationDaemon() - attribute de-duplicator daemon stopped");
            return null;
        });

        daemon.exceptionally(e -> {
            LOG.error("An unhandled exception has occurred in the attribute de-duplicator daemon. ", e);
            return null;
        });

        return daemon;
    }

    /**
     * Given an attributeValue, find the duplicates and merge them into a single unique attribute
     *
     * @param txFactory the factory object for accessing the database
     * @param keyspaceIndexPairs the set of attributes which are to be deduplicated duplicates
     */
    public static void deduplicate(EngineGraknTxFactory txFactory, Set<KeyspaceIndexPair> keyspaceIndexPairs) {
        for (KeyspaceIndexPair keyspaceIndexPair : keyspaceIndexPairs) {
            try (EmbeddedGraknTx tx = txFactory.tx(keyspaceIndexPair.keyspace(), GraknTxType.WRITE)) {
                GraphTraversalSource tinker = tx.getTinkerTraversal();
                GraphTraversal<Vertex, Vertex> duplicates = tinker.V().has(Schema.VertexProperty.INDEX.name(), keyspaceIndexPair.value());
                Vertex mergeTargetV = duplicates.next();
                while (duplicates.hasNext()) {
                    Vertex dup = duplicates.next();
                    try {
                        dup.vertices(Direction.IN).forEachRemaining(ent -> {
                            Edge edge = tinker.V(dup).inE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).filter(__.outV().is(ent)).next();
                            edge.remove();
                            ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV);
                        });
                        dup.remove();
                    }
                    catch (IllegalStateException vertexAlreadyRemovedException) {
                        LOG.warn("Trying to call the method vertices(Direction.IN) on vertex " + dup.id() + " which is already removed.");
                    }
                }
                tx.commit();
            }
        }
    }

    /**
     * Stops the attribute uniqueness daemon
     */
    public void stopDeduplicationDaemon() {
        LOG.info("stopDeduplicationDaemon() - stopping the attribute de-duplicator daemon...");
        stopDaemon = true;
    }

}


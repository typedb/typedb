/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * This class is responsible for merging attribute duplicates which is done in order to maintain attribute uniqueness.
 * It is an always-on background thread which continuously merge duplicates. This means that eventually,
 * every attribute instance will have a unique value, as the duplicates will have been removed.
 * A new incoming attribute will immediately trigger the merge operation, meaning that duplicates are merged in almost real-time speed.
 * It is fault-tolerant and will re-process incoming attributes if Grakn crashes during a merge process.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class AttributeMergerDaemon {
    private static Logger LOG = LoggerFactory.getLogger(AttributeMergerDaemon.class);

    public static AttributeMergerDaemon singleton = create();

    private static final int QUEUE_GET_BATCH_MIN = 1;
    private static final int QUEUE_GET_BATCH_MAX = 100;
    private static final int QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS = 1000;

    private Queue newAttributeQueue = new Queue();
    private MergeAlgorithm mergeAlgorithm = new MergeAlgorithm();
    private boolean stopDaemon = false;

    private static AttributeMergerDaemon create() {
        AttributeMergerDaemon singleton = new AttributeMergerDaemon();
        singleton.startDaemon();
        return singleton;
    }

    /**
     * Stops the attribute merger daemon
     */
    public void stopDaemon() {
        stopDaemon = true;
    }

    /**
     * Starts the attribute merger daemon, which continuously merge attribute duplicates.
     * The thread listens to the {@link Queue} queue for incoming attributes and applies
     * the merge algorithm as implemented in {@link MergeAlgorithm}.
     *
     */
    private CompletableFuture<Void> startDaemon() {
        CompletableFuture<Void> daemon = CompletableFuture.supplyAsync(() -> {
            LOG.info("startDaemon() - start");
            while (!stopDaemon) {
                try {
                    Queue.Attributes newAttrs = newAttributeQueue.takeBatch(QUEUE_GET_BATCH_MIN, QUEUE_GET_BATCH_MAX, QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS);
                    LOG.info("startDaemon() - starting a new batch to process these new attributes: " + newAttrs);
                    Map<KeyspaceAndValue, List<Queue.Attribute>> groupByKeyspaceAndValue = newAttrs.attributes().stream()
                            .collect(Collectors.groupingBy(attr -> KeyspaceAndValue.create(attr.keyspace(), attr.value())));
                    groupByKeyspaceAndValue.forEach((groupName, group) -> LOG.info("startDaemon() - group: " + groupName + " = " + group));
                    groupByKeyspaceAndValue.forEach((keyspaceAndValue, attrValue) -> {
                        try (EmbeddedGraknSession s  = EmbeddedGraknSession.create(keyspaceAndValue.keyspace(), "localhost:4567");
                             EmbeddedGraknTx      tx = s.transaction(GraknTxType.WRITE)) {
                            mergeAlgorithm.merge(tx, attrValue);
                            tx.commitSubmitNoLogs();
                        }
                    });
                    LOG.info("startDaemon() - merge completed.");
//                    newAttrs.markProcessed(); // TODO: enable after takeBatch is changed to processBatch()
                    LOG.info("startDaemon() - new attributes processed.");
                } catch (RuntimeException e) {
                    LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
                }
            }
            LOG.info("startDaemon() - stop");
            return null;
        });

        daemon.exceptionally(e -> {
            LOG.error("An exception has occurred in the AttributeMergerDaemon. ", e);
            return null;
        });

        return daemon;
    }

    public void add(Keyspace keyspace, String value, ConceptId conceptId) {
        final Queue.Attribute newAttribute = Queue.Attribute.create(keyspace, value, conceptId);
        LOG.info("add(" + newAttribute + ")");
        newAttributeQueue.add(newAttribute);
    }

    /**
     * TODO
     *
     */
    static class Queue {
        private static Logger LOG = LoggerFactory.getLogger(Queue.class);
        private java.util.Queue<Attribute> newAttributeQueue = new LinkedBlockingQueue<>();

        /**
         * Enqueue a new attribute to the queue
         * @param attribute
         */
        public void add(Attribute attribute) {
            newAttributeQueue.add(attribute);
        }

        // TODO: change to read
        /**
         * get n attributes where min <= n <= max. For fault tolerance, attributes are not deleted from the queue until Attributes::markProcessed() is called.
         *
         * @param min minimum number of items to be returned. the method will block until it is reached.
         * @param max the maximum number of items to be returned.
         * @param timeLimit specifies the maximum waiting time where the method will immediately return the items it has if larger than what is specified in the min param.
         * @return an {@link Queue.Attributes} instance containing a list of duplicates
         */
        Queue.Attributes takeBatch(int min, int max, long timeLimit) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            List<Attribute> batch = new LinkedList<>();

            for (int i = 0; i < max && batch.size() <= max; ++i) {
                Attribute e = newAttributeQueue.poll();
                if (e != null) batch.add(e);
            }

            return new Attributes(batch);
        }

        // TODO
        void markRead(Queue.Attributes batch) {
        }

        @AutoValue
        static abstract class Attribute {
            public abstract Keyspace keyspace();
            public abstract String value();
            public abstract ConceptId conceptId();

            public static Attribute create(Keyspace keyspace, String value, ConceptId conceptId) {
                return new AutoValue_AttributeMergerDaemon_Queue_Attribute(keyspace, value, conceptId);
            }
        }

        class Attributes {
            List<Attribute> attributes;

            Attributes(List<Attribute> attributes) {
                this.attributes = attributes;
            }

            // TODO
            List<Attribute> attributes() {
                return attributes;
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                        .add("attributes", attributes)
                        .toString();
            }
        }
    }

    /**
     * TODO
     *
     */
    static class MergeAlgorithm {
        private static Logger LOG = LoggerFactory.getLogger(MergeAlgorithm.class);

        /**
         * Merges a list of attributes.
         * The attributes being processed will be marked so they cannot be touched by other operations.
         * @param duplicates the list of attributes to be merged
         * @return the merged attribute (TODO)
         */
        public void merge(EmbeddedGraknTx tx, List<Queue.Attribute> duplicates) {
            LOG.info("merging '" + duplicates + "'...");
            if (duplicates.size() >= 1) {
                lock(duplicates);

                GraphTraversalSource tinker = tx.getTinkerTraversal();
                Vertex mergeTargetV = tinker.V().has(Schema.VertexProperty.INDEX.name(), duplicates.get(0).value()).next();
                List<Vertex> duplicatesV = duplicates.stream()
                        .map(attr -> tinker.V().has(Schema.VertexProperty.ID.name(), attr.conceptId()).next())
                        .collect(Collectors.toList());
                for (Vertex dup: duplicatesV) {
                    List<Vertex> linkedEntities = Lists.newArrayList(dup.vertices(Direction.IN));
                    for (Vertex ent: linkedEntities) {
                        Edge edge = tinker.V(dup).inE(Schema.EdgeLabel.ATTRIBUTE.getLabel()).filter(__.outV().is(ent)).next();
                        edge.remove();
                        ent.addEdge(Schema.EdgeLabel.ATTRIBUTE.getLabel(), mergeTargetV);
                    }
                    dup.remove();
                }

                unlock(duplicates);
                LOG.info("merging completed.");
            }
        }

        // TODO
        private void lock(List<Queue.Attribute> attributes) {
        }

        // TODO
        private void unlock(List<Queue.Attribute> attributes) {
        }
    }

    /**
     *
     */
    @AutoValue
    static abstract class KeyspaceAndValue {
        public abstract Keyspace keyspace();
        public abstract String value();

        public static KeyspaceAndValue create(Keyspace keyspace, String value) {
            return new AutoValue_AttributeMergerDaemon_KeyspaceAndValue(keyspace, value);
        }
    }
}
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

import ai.grakn.kb.internal.EmbeddedGraknTx;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
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
    private static final int QUEUE_GET_BATCH_MAX = 10;
    private static final int QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS = 1000;

    private Queue newAttributeQueue = new Queue();
    private MergeAlgorithm mergeAlgorithm = new MergeAlgorithm();
    private EmbeddedGraknTx tx = null;
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
                    LOG.info("startDaemon() - process new attributes...");
                    LOG.info("startDaemon() - newAttrs: " + newAttrs);
                    Map<String, List<Queue.Attribute>> grouped = newAttrs.attributes().stream().collect(Collectors.groupingBy(attr -> attr.value()));
                    LOG.info("startDaemon() - grouped: " + grouped);
                    grouped.forEach((k, attrValue) -> mergeAlgorithm.merge(tx, attrValue));
                    LOG.info("startDaemon() - merge completed.");
//                    newAttrs.markProcessed(); // TODO: enable after takeBatch is changed to processBatch()
                    LOG.info("startDaemon() - new attributes processed.");
                } catch (RuntimeException e) {
                    LOG.error("An exception has occurred in the AttributeMerger. ", e);
                }
            }
            LOG.info("startDaemon() - stop");
            return null;
        });

        daemon.exceptionally(e -> {
            LOG.error("An exception has occurred in the AttributeMerger. ", e);
            return null;
        });

        return daemon;
    }

    public void add(String conceptId, String value) {
        LOG.info("add(conceptId = " + conceptId + ", value = " + value + ")");
        newAttributeQueue.add(Queue.Attribute.create(conceptId, value));
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

        // TODO: change to readBatch / vs markRead
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
            public abstract String conceptId();
            public abstract String value();

            public static Attribute create(String conceptId, String value) {
                return new AutoValue_AttributeMerger_AttributeQueue_Attribute(conceptId, value);
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
         * Merges a list of duplicates. Given a list of duplicates, will 'keep' one and remove 'the rest'.
         * {@link ai.grakn.concept.Concept} pointing to a duplicate will correctly point to the one 'kept' rather than 'the rest' which are deleted.
         * The duplicates being processed will be marked so they cannot be touched by other operations.
         * @param duplicates the list of duplicates to be merged
         */
        public void merge(EmbeddedGraknTx tx, List<Queue.Attribute> duplicates) {
            LOG.info("merging '" + duplicates + "'...");
            lock(duplicates);

            String keep = null;
            List<String> remove = null;
            merge(tx, keep, remove);

            unlock(duplicates);
            LOG.info("merging completed.");
        }

        // TODO
        private void merge(EmbeddedGraknTx tx, String keep, List<String> remove) {
        }

        // TODO
        private void lock(List<Queue.Attribute> attributes) {
        }

        // TODO
        private void unlock(List<Queue.Attribute> attributes) {
        }
    }

}
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
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
public class AttributeMerger {
    private static Logger LOG = LoggerFactory.getLogger(AttributeMerger.class);

    public static AttributeMerger singleton = create();

    private static final int QUEUE_GET_BATCH_MIN = 1;
    private static final int QUEUE_GET_BATCH_MAX = 10;
    private static final int QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS = 1000;

    private AttributeQueue newAttributeQueue = new AttributeQueue();
    private MergeAlgorithm mergeAlgorithm = new MergeAlgorithm();
    private EmbeddedGraknTx tx = null;

    private static AttributeMerger create() {
        AttributeMerger singleton = new AttributeMerger();
        singleton.startBackground();
        return singleton;
    }

    /**
     * Starts an always-on background thread which continuously merge attribute duplicates.
     * The thread listens to the {@link AttributeQueue} queue for incoming attributes and applies
     * the merge algorithm as implemented in {@link MergeAlgorithm}.
     *
     */
    private CompletableFuture<Void> startBackground() {
        return CompletableFuture.supplyAsync(() -> {
            LOG.info("startBackground() - start");
            for (AttributeQueue.Attributes newAttrs = newAttributeQueue.getBatch(QUEUE_GET_BATCH_MIN,
                    QUEUE_GET_BATCH_MAX, QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS); ;) {
                LOG.info("startBackground() - process new attributes...");
                LOG.info("startBackground() - newAttrs: " + newAttrs);
                Map<String, List<AttributeQueue.Attribute>> grouped = newAttrs.attributes().stream().collect(Collectors.groupingBy(attr -> attr.value()));
                LOG.info("startBackground() - grouped: " + grouped);
                grouped.forEach((k, attrValue) -> mergeAlgorithm.merge(tx, attrValue));
                LOG.info("startBackground() - merge completed.");
                newAttrs.markProcessed();
                LOG.info("startBackground() - new attributes processed.");
            }
        });
    }

    public void add(String conceptId, String value) {
        newAttributeQueue.add(AttributeQueue.Attribute.create(conceptId, value));
    }

    /**
     * TODO
     *
     */
    static class AttributeQueue {
        private static Logger LOG = LoggerFactory.getLogger(AttributeQueue.class);
        private Queue<Attribute> newAttributeQueue = new LinkedBlockingQueue<>();

        /**
         * Enqueue a new attribute to the queue
         * @param attribute
         */
        public void add(Attribute attribute) {
            newAttributeQueue.add(attribute);
        }

        /**
         * get n attributes where min <= n <= max. For fault tolerance, attributes are not deleted from the queue until Attributes::markProcessed() is called.
         *
         * @param min minimum number of items to be returned. the method will block until it is reached.
         * @param max the maximum number of items to be returned.
         * @param timeLimit specifies the maximum waiting time where the method will immediately return the items it has if larger than what is specified in the min param.
         * @return an {@link Attributes} instance containing a list of duplicates
         */
        Attributes getBatch(int min, int max, long timeLimit) {
            throw new UnsupportedOperationException();
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

            List<Attribute> attributes() {
                throw new UnsupportedOperationException();
            }

            void markProcessed() {
                throw new UnsupportedOperationException();
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
        public void merge(EmbeddedGraknTx tx, List<AttributeQueue.Attribute> duplicates) {
//            lock(null);
//            String keep = null;
//            List<String> remove = null;
//            merge(tx, keep, remove);
//            unlock(null);

            throw new UnsupportedOperationException();
        }

        private void merge(EmbeddedGraknTx tx, String keep, List<String> remove) {
            throw new UnsupportedOperationException();
        }

        private void lock(List<String> attributes) {
            throw new UnsupportedOperationException();
        }

        private void unlock(List<String> attributes) {
            throw new UnsupportedOperationException();
        }
    }

}
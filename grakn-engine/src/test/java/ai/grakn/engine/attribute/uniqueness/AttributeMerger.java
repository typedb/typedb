package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.kb.internal.EmbeddedGraknTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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

    private static final int QUEUE_GET_BATCH_MIN = 1;
    private static final int QUEUE_GET_BATCH_MAX = 10;
    private static final int QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS = 1000;

    private AttributeQueue newAttributeQueue = new AttributeQueue();
    private MergeAlgorithm mergeAlgorithm = new MergeAlgorithm();
    private EmbeddedGraknTx tx = null;

    public AttributeMerger(EmbeddedGraknTx tx, AttributeMerger.AttributeQueue newAttributeQueue) {
        this.tx = tx;
        this.newAttributeQueue = newAttributeQueue;
    }

    /**
     * Starts an always-on background thread which continuously merge attribute duplicates.
     * The thread listens to the {@link AttributeQueue} queue for incoming attributes and applies
     * the merge algorithm as implemented in {@link MergeAlgorithm}.
     *
     */
    public CompletableFuture<Void> startBackground() {
        LOG.info("startBackground() - start");
        for (AttributeQueue.Attributes newAttrs = newAttributeQueue.getBatch(QUEUE_GET_BATCH_MIN,
                QUEUE_GET_BATCH_MAX, QUEUE_GET_BATCH_WAIT_TIME_LIMIT_MS); ;) {
            LOG.info("startBackground() - process new attributes...");
            LOG.info("startBackground() - newAttrs: " + newAttrs);
            Map<String, List<String>> grouped = newAttrs.attributes().stream().collect(Collectors.groupingBy(attrValue -> attrValue));
            LOG.info("startBackground() - grouped: " + grouped);
            grouped.forEach((k, attrValue) -> mergeAlgorithm.merge(tx, attrValue));
            LOG.info("startBackground() - merge completed.");
            newAttrs.markProcessed();
            LOG.info("startBackground() - new attributes processed.");
        }
    }

    /**
     * TODO
     *
     */
    static class AttributeQueue {
        private static Logger LOG = LoggerFactory.getLogger(AttributeQueue.class);
        private Queue<String> newAttributeQueue = new LinkedBlockingQueue<>();

        /**
         * Enqueue a new attribute to the queue
         * @param attribute
         */
        public void add(String attribute) {
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

        class Attributes {
            List<String> attributes;

            Attributes(List<String> attributes) {
                this.attributes = attributes;
            }

            List<String> attributes() {
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
        public void merge(EmbeddedGraknTx tx, List<String> duplicates) {
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
package ai.grakn.engine.attribute.uniqueness;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 *
 *
 */
public class UniquenessProcessor {
    private AttributeQueue newAttributeQueue = new AttributeQueue();
    private Deduplicator attributeDeduplicator = null;

    /**
     *
     *
     */
    public void start() {
        for (List<String> newAttributes = newAttributeQueue.take(1, 10, 1000); ;) {
            Map<String, List<String>> grouped = newAttributes.stream().collect(Collectors.groupingBy(attrValue -> attrValue));
            grouped.forEach((k, attrValue) -> attributeDeduplicator.deduplicate(attrValue));
        }
    }

    /**
     *
     *
     */
    class AttributeQueue {
        private Queue<String> newAttributeQueue = new LinkedBlockingQueue<>();

        /**
         *
         * @param min
         * @param max
         * @param timeLimit
         * @return
         */
        List<String> take(int min, int max, long timeLimit) {
            return null;
        }
    }

    /**
     *
     *
     */
    class Deduplicator {
        /**
         *
         * @param duplicates
         */
        public void deduplicate(List<String> duplicates) {
            lock(null);
            String keep = null;
            List<String> remove = null;
            deduplicate(keep, remove);
            unlock(null);
        }

        private void deduplicate(String keep, List<String> remove) {

        }

        private void lock(List<String> attributes) {

        }

        private void unlock(List<String> attributes) {

        }
    }

}
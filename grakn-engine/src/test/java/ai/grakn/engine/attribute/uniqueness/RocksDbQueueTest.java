package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class RocksDbQueueTest {
    Path path = Paths.get("./queue");

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(path.toFile());
    }

    @Test
    public void shouldBeAbleToInsertNewAttributes() throws InterruptedException {
        try (RocksDbQueue queue = new RocksDbQueue(path)) {
            List<Attribute> attributes = Arrays.asList(
                    Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")),
                    Attribute.create(Keyspace.of("k2"), "v2", ConceptId.of("c2")),
                    Attribute.create(Keyspace.of("k3"), "v3", ConceptId.of("c3")),
                    Attribute.create(Keyspace.of("k4"), "v4", ConceptId.of("c4")),
                    Attribute.create(Keyspace.of("k5"), "v5", ConceptId.of("c5"))
            );

            for (Attribute attr : attributes) {
                queue.insert(attr);
            }
            List<Attribute> insertedAttributes = queue.read(attributes.size());
            assertThat(insertedAttributes, equalTo(attributes));
        }
    }

    @Test
    public void readButUnackedAttributesShouldRemainInTheQueue() throws InterruptedException {
        try (RocksDbQueue queue = new RocksDbQueue(path)) {
            List<Attribute> attributes = Arrays.asList(
                    Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")),
                    Attribute.create(Keyspace.of("k2"), "v2", ConceptId.of("c2")),
                    Attribute.create(Keyspace.of("k3"), "v3", ConceptId.of("c3")),
                    Attribute.create(Keyspace.of("k4"), "v4", ConceptId.of("c4")),
                    Attribute.create(Keyspace.of("k5"), "v5", ConceptId.of("c5"))
            );

            for (Attribute attr : attributes) {
                queue.insert(attr);
            }
            List<Attribute> insertedAttributes = queue.read(Integer.MAX_VALUE);
            assertThat(insertedAttributes, equalTo(attributes));
            List<Attribute> remainingAttributes = queue.read(Integer.MAX_VALUE);
            assertThat(remainingAttributes, equalTo(attributes));
        }
    }

    @Test
    public void shouldBeAbleToAckOnlySomeOfTheReadAttributes() throws InterruptedException {
        try (RocksDbQueue queue = new RocksDbQueue(path)) {
            List<Attribute> attributes1 = Arrays.asList(
                    Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")),
                    Attribute.create(Keyspace.of("k2"), "v2", ConceptId.of("c2"))
            );
            List<Attribute> attributes2 = Arrays.asList(
                    Attribute.create(Keyspace.of("k3"), "v3", ConceptId.of("c3")),
                    Attribute.create(Keyspace.of("k4"), "v4", ConceptId.of("c4")),
                    Attribute.create(Keyspace.of("k5"), "v5", ConceptId.of("c5"))
            );

            Stream.concat(attributes1.stream(), attributes2.stream()).forEach(attr -> queue.insert(attr));

            List<Attribute> insertedAttributes1 = queue.read(attributes1.size());
            queue.ack(insertedAttributes1);
            List<Attribute> insertedAttributes2 = queue.read(Integer.MAX_VALUE);
            assertThat(insertedAttributes2, equalTo(attributes2));
        }
    }

    /**
     * this test might not be immediately obvious hence warrants an explanation.
     *
     * the read() method implements the 'guarded block' pattern using wait() and notifyAll(), and we want to
     * test if it properly waits until the queue becomes non-empty.
     *
     * the correct behavior is for read() to wait until insert() has been invoked, and this is verified
     * if the element 'added-after-read' appears AFTER 'added-after-300ms'.
     *
     * @throws InterruptedException
     */
    @Test
    public void theReadMethodMustWaitUntilTheQueueBecomesNonEmpty() throws InterruptedException {
        List<String> verifyOrderOfOperation = new ArrayList<>();
        RocksDbQueue queue = new RocksDbQueue(path);

        CompletableFuture.supplyAsync(() -> {

            // operation a: wait for 300ms and insert 'added-after-300ms' into the list
            try { Thread.sleep(300); } catch (InterruptedException e) { throw new RuntimeException(e); }
            queue.insert(Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")));
            verifyOrderOfOperation.add("added-after-300ms");

            return null;
        });

        // operation b: insert 'added-after-read' after queue.read(x)
        queue.read(5);
        verifyOrderOfOperation.add("added-after-read");

        // the element 'added-after-read' must appear AFTER 'added-after-300ms'
        assertThat(verifyOrderOfOperation, equalTo(Arrays.asList("added-after-300ms", "added-after-read")));
    }
}

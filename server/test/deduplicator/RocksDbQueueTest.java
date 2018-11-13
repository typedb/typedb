package grakn.core.server.deduplicator;

import grakn.core.Keyspace;
import grakn.core.concept.ConceptId;
import grakn.core.server.deduplicator.queue.Attribute;
import grakn.core.server.deduplicator.queue.RocksDbQueue;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RocksDbQueueTest {

    @Test
    public void shouldBeAbleToInsertNewAttributes() throws InterruptedException, IOException {
        Path queuePath = Files.createTempDirectory("rocksdb-test-dir");
        try (RocksDbQueue queue = new RocksDbQueue(queuePath)) {
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
        FileUtils.deleteDirectory(queuePath.toFile());
    }

    @Test
    public void readButUnackedAttributesShouldRemainInTheQueue() throws InterruptedException, IOException {
        Path queuePath = Files.createTempDirectory("rocksdb-test-dir");
        try (RocksDbQueue queue = new RocksDbQueue(queuePath)) {
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
        FileUtils.deleteDirectory(queuePath.toFile());
    }

    @Test
    public void shouldBeAbleToAckOnlySomeOfTheReadAttributes() throws InterruptedException, IOException {
        Path queuePath = Files.createTempDirectory("rocksdb-test-dir");
        try (RocksDbQueue queue = new RocksDbQueue(queuePath)) {
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
        FileUtils.deleteDirectory(queuePath.toFile());
    }

    /**
     * the read() method implements the 'guarded block' pattern using wait() and notifyAll(), and we want to
     * test if it properly blocks until the queue becomes non-empty.
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    @Test
    public void theReadMethodMustBlockUntilTheQueueBecomesNonEmpty() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        Path queuePath = Files.createTempDirectory("rocksdb-test-dir");
        RocksDbQueue queue = new RocksDbQueue(queuePath);

        Attribute input = Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1"));
        List<Attribute> expectedOutput = Arrays.asList(input);

        // perform a read() on the currently empty queue asynchronously.
        CompletableFuture<List<Attribute>> readMustBlock_untilQueueNonEmpty = CompletableFuture.supplyAsync(() -> {
            try {
                return queue.read(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // verify if read() indeed blocks, in which case a TimeoutException is thrown
        try {
            readMustBlock_untilQueueNonEmpty.get(500L, TimeUnit.MILLISECONDS);
            throw new RuntimeException("The expected TimeoutException isn't thrown");
        } catch (TimeoutException e) {
            // The expected TimeoutException is thrown
        }

        // now that we've verified that a TimeoutException is thrown, let's insert some data into the queue
        queue.insert(input);

        // by this time, that read() operation should return the result
        List<Attribute> actualOutput = readMustBlock_untilQueueNonEmpty.get(500L, TimeUnit.MILLISECONDS);
        assertThat(actualOutput, equalTo(expectedOutput));
    }
}

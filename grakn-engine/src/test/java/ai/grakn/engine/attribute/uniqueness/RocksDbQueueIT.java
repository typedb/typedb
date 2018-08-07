package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class RocksDbQueueIT {
    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(Paths.get("./queue").toFile());
    }

    @Test
    public void shouldBeAbleToInsertNewAttributes() throws InterruptedException {
        RocksDbQueue queue = new RocksDbQueue();
        List<Attribute> attributes = Arrays.asList(
                Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")),
                Attribute.create(Keyspace.of("k2"), "v2", ConceptId.of("c2")),
                Attribute.create(Keyspace.of("k3"), "v3", ConceptId.of("c3")),
                Attribute.create(Keyspace.of("k4"), "v4", ConceptId.of("c4")),
                Attribute.create(Keyspace.of("k5"), "v5", ConceptId.of("c5"))
        );

        for (Attribute a: attributes) {
            queue.insertAttribute(a);
        }
        Attributes insertedAttributes = queue.readAttributes(attributes.size());
        assertThat(insertedAttributes.attributes(), equalTo(attributes));
        queue.close();
    }

    @Test
    public void unackedAttributesShouldStillBeAvailableForAReRead() throws InterruptedException {
        RocksDbQueue queue = new RocksDbQueue();
        List<Attribute> attributes = Arrays.asList(
                Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")),
                Attribute.create(Keyspace.of("k2"), "v2", ConceptId.of("c2")),
                Attribute.create(Keyspace.of("k3"), "v3", ConceptId.of("c3")),
                Attribute.create(Keyspace.of("k4"), "v4", ConceptId.of("c4")),
                Attribute.create(Keyspace.of("k5"), "v5", ConceptId.of("c5"))
        );

        for (Attribute a: attributes) {
            queue.insertAttribute(a);
        }
        Attributes insertedAttributes = queue.readAttributes(Integer.MAX_VALUE);
        assertThat(insertedAttributes.attributes(), equalTo(attributes));
        Attributes remainingAttributes = queue.readAttributes(Integer.MAX_VALUE);
        assertThat(remainingAttributes.attributes(), equalTo(attributes));
        queue.close();
    }

    @Test
    public void shouldBeAbleToAckOnlySomeOfTheReadAttributes() throws InterruptedException {
        RocksDbQueue queue = new RocksDbQueue();
        List<Attribute> attributes1 = Arrays.asList(
                Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")),
                Attribute.create(Keyspace.of("k2"), "v2", ConceptId.of("c2"))
        );
        List<Attribute> attributes2 = Arrays.asList(
                Attribute.create(Keyspace.of("k3"), "v3", ConceptId.of("c3")),
                Attribute.create(Keyspace.of("k4"), "v4", ConceptId.of("c4")),
                Attribute.create(Keyspace.of("k5"), "v5", ConceptId.of("c5"))
        );

        Stream.concat(attributes1.stream(), attributes2.stream()).forEach(a -> queue.insertAttribute(a));

        Attributes insertedAttributes1 = queue.readAttributes(attributes1.size());
        queue.ackAttributes(insertedAttributes1);
        Attributes insertedAttributes2 = queue.readAttributes(Integer.MAX_VALUE);
        assertThat(insertedAttributes2.attributes(), equalTo(attributes2));
        queue.close();
    }

    @Ignore
    @Test
    public void readAttributesMustBlockUntilThereAreItemsInTheQueue() throws InterruptedException {
        RocksDbQueue queue = new RocksDbQueue();
        AtomicInteger i = new AtomicInteger();
        CompletableFuture.supplyAsync(() -> {
            i.incrementAndGet();
            try { Thread.sleep(5000); } catch (InterruptedException e) { throw new RuntimeException(e); }
            System.out.println("future");
            queue.insertAttribute(Attribute.create(Keyspace.of("k1"), "v1", ConceptId.of("c1")));
            return null;
        });
        System.out.println("main thread");
        queue.readAttributes(5);
    }
}

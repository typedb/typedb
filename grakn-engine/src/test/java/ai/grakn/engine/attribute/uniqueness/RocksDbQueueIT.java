package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.attribute.uniqueness.queue.Attribute;
import ai.grakn.engine.attribute.uniqueness.queue.Attributes;
import ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class RocksDbQueueIT {
    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(Paths.get("./queue").toFile());
    }

    @Test
    public void shouldBeAbleToInsertNewAttributes() {
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
        Attributes insertedAttributes = queue.readAttributes(0, attributes.size(), 0);
        assertThat(insertedAttributes.attributes(), equalTo(attributes));
        queue.close();
    }

    @Test
    public void unackedAttributesShouldStillBeAvailableForAReRead() {
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
        Attributes insertedAttributes = queue.readAttributes(0, Integer.MAX_VALUE, 0);
        assertThat(insertedAttributes.attributes(), equalTo(attributes));
        Attributes remainingAttributes = queue.readAttributes(0, Integer.MAX_VALUE, 0);
        assertThat(remainingAttributes.attributes(), equalTo(attributes));
        queue.close();
    }

    @Test
    public void shouldBeAbleToAckReadAttributes() {
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
        Attributes insertedAttributes = queue.readAttributes(0, Integer.MAX_VALUE, 0);
        queue.ackAttributes(insertedAttributes);
        Attributes remainingAttributes = queue.readAttributes(0, Integer.MAX_VALUE, 0);
        assertThat(remainingAttributes.attributes(), emptyIterable());
        queue.close();
    }

    @Test
    public void shouldBeAbleToAckOnlySomeOfTheReadAttributes() {
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

        Attributes insertedAttributes1 = queue.readAttributes(0, attributes1.size(), 0);
        queue.ackAttributes(insertedAttributes1);
        Attributes insertedAttributes2 = queue.readAttributes(0, Integer.MAX_VALUE, 0);
        assertThat(insertedAttributes2.attributes(), equalTo(attributes2));
        queue.close();
    }
}

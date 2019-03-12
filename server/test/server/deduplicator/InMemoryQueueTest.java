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

package grakn.core.server.deduplicator;

import grakn.core.concept.ConceptId;
import grakn.core.server.deduplicator.queue.Attribute;
import grakn.core.server.deduplicator.queue.InMemoryQueue;
import grakn.core.server.keyspace.KeyspaceImpl;
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

public class InMemoryQueueTest {

    @Test
    public void shouldBeAbleToInsertNewAttributes() throws InterruptedException {
        InMemoryQueue queue = new InMemoryQueue();
        List<Attribute> attributes = Arrays.asList(
                Attribute.create(KeyspaceImpl.of("k1"), "v1", ConceptId.of("c1")),
                Attribute.create(KeyspaceImpl.of("k2"), "v2", ConceptId.of("c2")),
                Attribute.create(KeyspaceImpl.of("k3"), "v3", ConceptId.of("c3")),
                Attribute.create(KeyspaceImpl.of("k4"), "v4", ConceptId.of("c4")),
                Attribute.create(KeyspaceImpl.of("k5"), "v5", ConceptId.of("c5"))
        );

        for (Attribute attr : attributes) {
            queue.insert(attr);
        }
        List<Attribute> insertedAttributes = queue.read(attributes.size());
        assertThat(insertedAttributes, equalTo(attributes));
    }


    @Test
    public void readButUnackedAttributesShouldRemainInTheQueue() throws InterruptedException {
        InMemoryQueue queue = new InMemoryQueue();
        List<Attribute> attributes = Arrays.asList(
                Attribute.create(KeyspaceImpl.of("k1"), "v1", ConceptId.of("c1")),
                Attribute.create(KeyspaceImpl.of("k2"), "v2", ConceptId.of("c2")),
                Attribute.create(KeyspaceImpl.of("k3"), "v3", ConceptId.of("c3")),
                Attribute.create(KeyspaceImpl.of("k4"), "v4", ConceptId.of("c4")),
                Attribute.create(KeyspaceImpl.of("k5"), "v5", ConceptId.of("c5"))
        );

        for (Attribute attr : attributes) {
            queue.insert(attr);
        }
        List<Attribute> insertedAttributes = queue.read(Integer.MAX_VALUE);
        assertThat(insertedAttributes, equalTo(attributes));
        List<Attribute> remainingAttributes = queue.read(Integer.MAX_VALUE);
        assertThat(remainingAttributes, equalTo(attributes));
    }

    @Test
    public void shouldBeAbleToAckOnlySomeOfTheReadAttributes() throws InterruptedException {
        InMemoryQueue queue = new InMemoryQueue();
        List<Attribute> attributes1 = Arrays.asList(
                Attribute.create(KeyspaceImpl.of("k1"), "v1", ConceptId.of("c1")),
                Attribute.create(KeyspaceImpl.of("k2"), "v2", ConceptId.of("c2"))
        );
        List<Attribute> attributes2 = Arrays.asList(
                Attribute.create(KeyspaceImpl.of("k3"), "v3", ConceptId.of("c3")),
                Attribute.create(KeyspaceImpl.of("k4"), "v4", ConceptId.of("c4")),
                Attribute.create(KeyspaceImpl.of("k5"), "v5", ConceptId.of("c5"))
        );

        Stream.concat(attributes1.stream(), attributes2.stream()).forEach(attr -> queue.insert(attr));

        List<Attribute> insertedAttributes1 = queue.read(attributes1.size());
        queue.ack(insertedAttributes1);
        List<Attribute> insertedAttributes2 = queue.read(Integer.MAX_VALUE);
        assertThat(insertedAttributes2, equalTo(attributes2));
    }

    /**
     * the read() method implements the 'guarded block' pattern using wait() and notifyAll(), and we want to
     * test if it properly blocks.
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    @Test(expected = TimeoutException.class)
    public void theReadMethodMustBlockIfTheQueueIsEmpty() throws ExecutionException, InterruptedException, TimeoutException {
        InMemoryQueue queue = new InMemoryQueue();

        // perform a read() on the currently empty queue asynchronously.
        CompletableFuture<List<Attribute>> readMustBlock = CompletableFuture.supplyAsync(() -> {
            try {
                return queue.read(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // verify if read() indeed blocks, in which case a TimeoutException is thrown
        readMustBlock.get(500L, TimeUnit.MILLISECONDS);
    }

    /**
     * the read() method implements the 'guarded block' pattern using wait() and notifyAll(), and we want to
     * test if it properly returns after blocking only once the queue is non-empty.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    @Test
    public void theReadMethodMustReturnOnceTheQueueIsNonEmpty() throws InterruptedException, ExecutionException, TimeoutException {
        InMemoryQueue queue = new InMemoryQueue();

        Attribute input = Attribute.create(KeyspaceImpl.of("k1"), "v1", ConceptId.of("c1"));
        List<Attribute> expectedOutput = Arrays.asList(input);

        // perform a read() on the currently empty queue asynchronously.
        CompletableFuture<List<Attribute>> readMustBlock_untilQueueNonEmpty = CompletableFuture.supplyAsync(() -> {
            try {
                return queue.read(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(300L); // wait 300ms to simulate insert() being called after read()
        queue.insert(input);

        // by this time, that read() operation should return the result
        List<Attribute> actualOutput = readMustBlock_untilQueueNonEmpty.get(1000L, TimeUnit.MILLISECONDS);
        assertThat(actualOutput, equalTo(expectedOutput));
    }
}

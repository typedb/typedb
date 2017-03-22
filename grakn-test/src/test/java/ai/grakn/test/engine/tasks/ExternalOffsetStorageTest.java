/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.engine.tasks;

import ai.grakn.engine.tasks.ExternalOffsetStorage;
import ai.grakn.engine.tasks.manager.ZookeeperConnection;
import ai.grakn.exception.EngineStorageException;
import ai.grakn.test.EngineContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.*;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalOffsetStorageTest {

    private static ZookeeperConnection zookeeper;
    private static ExternalOffsetStorage offsetStorage;

    @ClassRule
    public static EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setupStorage(){
        zookeeper = new ZookeeperConnection();
        offsetStorage = new ExternalOffsetStorage(zookeeper);
    }

    @AfterClass
    public static void shutdownZKConnection(){
        zookeeper.close();
    }

    @Test
    public void whenSavingNewConsumerOffset_CanGetOffset() throws Exception{
        long consumerOffset = 1L;

        TopicPartition partition = new TopicPartition("hello", 3);
        Consumer consumer = mock(Consumer.class);
        when(consumer.position(partition)).thenReturn(consumerOffset);

        // Save offset in Zookeeper
        offsetStorage.saveOffset(consumer, partition);

        // Retrieve it and test result
        long retrievedOffset = offsetStorage.getOffset(partition);

        assertEquals(consumerOffset, retrievedOffset);
    }

    @Test
    public void whenSavingUpdatedConsumerOffset_CanGetOffset(){
        TopicPartition partition = new TopicPartition("hello", 3);
        long originalConsumerOffset = 2L;

        Consumer consumer = mock(Consumer.class);
        when(consumer.position(partition)).thenReturn(originalConsumerOffset);

        // Save offset in Zookeeper
        offsetStorage.saveOffset(consumer, partition);

        // Retrieve it and test result
        long retrievedOffset = offsetStorage.getOffset(partition);
        assertEquals(retrievedOffset, originalConsumerOffset);

        // update the offset again
        long newConsumerOffset = 3L;
        when(consumer.position(partition)).thenReturn(newConsumerOffset);
        offsetStorage.saveOffset(consumer, partition);

        retrievedOffset = offsetStorage.getOffset(partition);
        assertEquals(retrievedOffset, newConsumerOffset);
    }

    @Test
    public void whenGettingNonExistingOffset_StorageExceptionIsThrown(){
        TopicPartition partition = new TopicPartition("goodbye", 4);
        exception.expect(EngineStorageException.class);
        offsetStorage.getOffset(partition);
    }
}

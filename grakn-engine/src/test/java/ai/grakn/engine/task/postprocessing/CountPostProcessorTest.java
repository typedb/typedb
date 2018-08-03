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

package ai.grakn.engine.task.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisCountStorage;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CountPostProcessorTest {
    private static RedisCountStorage countStorage = mock(RedisCountStorage.class);
    private static GraknConfig configMock = mock(GraknConfig.class);
    private static EngineGraknTxFactory factoryMock = mock(EngineGraknTxFactory.class);
    private static LockProvider lockProviderMock = mock(LockProvider.class);
    private static MetricRegistry metricRegistry = new MetricRegistry();
    private static CountPostProcessor countPostProcessor;

    private final Map<ConceptId, Long> newInstanceCounts = new HashMap<>();
    private final Keyspace keyspace = SampleKBLoader.randomKeyspace();

    @Before
    public void setupMocks(){
        countStorage = mock(RedisCountStorage.class);
        when(countStorage.getCount(any())).thenReturn(1L);

        configMock = mock(GraknConfig.class);
        when(configMock.getProperty(GraknConfigKey.SHARDING_THRESHOLD)).thenReturn(5L);

        KeyspaceStore keyspaceStoreMock = mock(KeyspaceStore.class);
        when(keyspaceStoreMock.containsKeyspace(any())).thenReturn(true);

        EmbeddedGraknTx txMock = mock(EmbeddedGraknTx.class);
        when(txMock.admin()).thenReturn(mock(GraknAdmin.class));

        factoryMock = mock(EngineGraknTxFactory.class);
        when(factoryMock.keyspaceStore()).thenReturn(keyspaceStoreMock);
        when(factoryMock.tx(any(Keyspace.class), any())).thenReturn(txMock);

        lockProviderMock = mock(LockProvider.class);
        when(lockProviderMock.getLock(any())).thenReturn(new ReentrantLock());

        metricRegistry = new MetricRegistry();
        countPostProcessor = CountPostProcessor.create(configMock, factoryMock, lockProviderMock, metricRegistry, countStorage);
    }

    @Before
    public void makeCountMap(){
        //Create Fake New Instances
        newInstanceCounts.put(ConceptId.of("a"), 1L);
        newInstanceCounts.put(ConceptId.of("b"), 2L);
        newInstanceCounts.put(ConceptId.of("c"), 3L);
        newInstanceCounts.put(ConceptId.of("d"), 4L);
    }

    @Test
    public void whenUpdatingInstanceCounts_EnsureRedisIsUpdated(){
        //Create fake commit log
        CommitLog commitLog = CommitLog.create(keyspace, newInstanceCounts, Collections.emptyMap());

        //Update The Counts
        countPostProcessor.updateCounts(commitLog);

        //Check the calls
        newInstanceCounts.forEach((id, value) -> {
            //Redis is updated
            verify(countStorage, Mockito.times(1)).getShardCount(keyspace, id);
            verify(countStorage, Mockito.times(1)).incrementInstanceCount(keyspace, id, value);
        });

        //No Sharding takes place
        verify(factoryMock, Mockito.times(0)).tx(any(Keyspace.class), any());
    }

    @Test
    public void whenBreachingTheShardingThreshold_ShardingHappens(){
        //Configure mock to return value which breaches threshold
        ConceptId id = ConceptId.of("e");
        newInstanceCounts.put(id, 6L);
        when(countStorage.incrementInstanceCount(keyspace, id, 6L)).thenReturn(6L);
        when(countStorage.incrementInstanceCount(keyspace, id, 0L)).thenReturn(6L);

        //Create fake commit log
        CommitLog commitLog = CommitLog.create(keyspace, newInstanceCounts, Collections.emptyMap());

        //Update The Counts
        countPostProcessor.updateCounts(commitLog);

        //Check Sharding Takes Place
        verify(factoryMock, Mockito.times(1)).tx(keyspace, GraknTxType.WRITE);
        verify(countStorage, Mockito.times(1)).incrementShardCount(keyspace, id, 1);
    }
}

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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.postprocessing;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.util.SampleKBLoader;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InstanceCountPostProcessorTest {
    private static RedisCountStorage countStorage = mock(RedisCountStorage.class);
    private static GraknConfig configMock = mock(GraknConfig.class);
    private static EngineGraknTxFactory factoryMock = mock(EngineGraknTxFactory.class);
    private static LockProvider lockProviderMock = mock(LockProvider.class);
    private static MetricRegistry metricRegistry = new MetricRegistry();
    private static InstanceCountPostProcessor countPostProcessor = InstanceCountPostProcessor.create(configMock, factoryMock, lockProviderMock, metricRegistry, countStorage);

    private final Map<ConceptId, Long> newInstanceCounts = new HashMap<>();
    private final Keyspace keyspace = SampleKBLoader.randomKeyspace();

    @BeforeClass
    public static void setupMocks(){
        countStorage = mock(RedisCountStorage.class);
        when(countStorage.getCount(any())).thenReturn(1L);

        configMock = mock(GraknConfig.class);
        when(configMock.getProperty(GraknConfigKey.SHARDING_THRESHOLD)).thenReturn(10L);
        when(configMock.getProperty(GraknConfigKey.LOADER_REPEAT_COMMITS)).thenReturn(5);

        factoryMock = mock(EngineGraknTxFactory.class);

        lockProviderMock = mock(LockProvider.class);
        metricRegistry = new MetricRegistry();
        countPostProcessor = InstanceCountPostProcessor.create(configMock, factoryMock, lockProviderMock, metricRegistry, countStorage);
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
            verify(countStorage, Mockito.times(1)).getCount(RedisCountStorage.getKeyNumShards(keyspace, id));
            verify(countStorage, Mockito.times(1)).adjustCount(RedisCountStorage.getKeyNumInstances(keyspace, id), value);
        });
    }
}

/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
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

package ai.grakn.test.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.kb.log.CommitLog;
import ai.grakn.test.rule.EngineContext;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostProcessingTaskTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    @ClassRule
    public static EngineContext engine = EngineContext.createWithInMemoryRedis();

    private String mockResourceIndex;
    private Set<ConceptId> mockResourceSet;
    private TaskConfiguration mockConfiguration;
    private PostProcessor postProcessor;

    @Before
    public void mockPostProcessing() throws JsonProcessingException {
        mockResourceIndex = UUID.randomUUID().toString();
        mockResourceSet = Sets.newHashSet();
        mockConfiguration = mock(TaskConfiguration.class);
        postProcessor = PostProcessor.create(engine.config(), engine.getJedisPool(), engine.server().factory(), engine.server().lockProvider(), METRIC_REGISTRY);
        Keyspace keyspace = Keyspace.of("testing");

        //Configure commit log to be returned
        CommitLog commitLog = CommitLog.createDefault(keyspace);
        commitLog.attributes().put(mockResourceIndex, mockResourceSet);
        when(mockConfiguration.configuration()).thenReturn(mapper.writeValueAsString(commitLog));

        //Initialise keyspaces
        Grakn.session(engine.uri(), SystemKeyspace.SYSTEM_KB_KEYSPACE).open(GraknTxType.WRITE).close();
        Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE).close();
    }

    @Test
    public void whenPPTaskCalledWithCastingsToPP_PostProcessingPerformCastingsFixCalled(){
        PostProcessingTask task = new PostProcessingTask();

        task.initialize(mockConfiguration, engine.config(), engine.server().factory(),
                METRIC_REGISTRY, postProcessor);
        task.start();

        verify(mockConfiguration, times(1)).configuration();
    }

    @Test
    public void whenPPTaskCalledWithResourcesToPP_PostProcessingPerformResourcesFixCalled(){
        PostProcessingTask task = new PostProcessingTask();

        task.initialize(mockConfiguration, engine.config(), engine.server().factory(),
                METRIC_REGISTRY, postProcessor);
        task.start();

        verify(mockConfiguration, times(1)).configuration();
    }

    @Test
    public void whenTwoPPTasksStartCalledInDifferentThreads_PostProcessingRunsTwice() throws InterruptedException {
        // Add a bunch of jobs to the cache
        PostProcessingTask task1 = new PostProcessingTask();
        PostProcessingTask task2 = new PostProcessingTask();
        task1.initialize(mockConfiguration, engine.config(), engine.server().factory(),
                METRIC_REGISTRY, postProcessor);
        task2.initialize(mockConfiguration, engine.config(), engine.server().factory(),
                METRIC_REGISTRY, postProcessor);

        Thread pp1 = new Thread(task1::start);
        Thread pp2 = new Thread(task2::start);

        pp1.start();
        pp2.start();

        pp1.join();
        pp2.join();

        verify(mockConfiguration, times(2)).configuration();
    }
}
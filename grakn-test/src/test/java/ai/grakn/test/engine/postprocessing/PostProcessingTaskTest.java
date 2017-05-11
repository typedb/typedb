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

import ai.grakn.concept.ConceptId;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostProcessingTaskTest {

    @ClassRule
    public static EngineContext engine = EngineContext.startInMemoryServer();

    private String mockCastingIndex;
    private String mockResourceIndex;
    private Set<ConceptId> mockCastingSet;
    private Set<ConceptId> mockResourceSet;
    private TaskConfiguration mockConfiguration;
    private Consumer<TaskCheckpoint> mockConsumer;

    @Before
    public void mockPostProcessing(){
        mockConsumer = mock(Consumer.class);
        mockCastingIndex = UUID.randomUUID().toString();
        mockResourceIndex = UUID.randomUUID().toString();
        mockCastingSet = Sets.newHashSet();
        mockResourceSet = Sets.newHashSet();
        mockConfiguration = mock(TaskConfiguration.class);
        when(mockConfiguration.json()).thenReturn(Json.object(
                KEYSPACE, "testing",
                REST.Request.COMMIT_LOG_FIXING, Json.object(
                        Schema.BaseType.CASTING.name(), Json.object(mockCastingIndex, mockCastingSet),
                        Schema.BaseType.RESOURCE.name(), Json.object(mockResourceIndex, mockResourceSet)
                )));
    }

    @Test
    public void whenPPTaskCalledWithCastingsToPP_PostProcessingPerformCastingsFixCalled(){
        PostProcessingTask task = new PostProcessingTask();

        task.start(mockConsumer, mockConfiguration);

        verify(mockConfiguration, times(4)).json();
    }

    @Test
    public void whenPPTaskCalledWithResourcesToPP_PostProcessingPerformResourcesFixCalled(){
        PostProcessingTask task = new PostProcessingTask();

        task.start(mockConsumer, mockConfiguration);

        verify(mockConfiguration, times(4)).json();
    }

    @Test
    public void whenTwoPPTasksStartCalledInDifferentThreads_PostProcessingRunsTwice() throws InterruptedException {
        // Add a bunch of jobs to the cache
        PostProcessingTask task1 = new PostProcessingTask();
        PostProcessingTask task2 = new PostProcessingTask();

        Thread pp1 = new Thread(() -> task1.start(mockConsumer, mockConfiguration));
        Thread pp2 = new Thread(() -> task2.start(mockConsumer, mockConfiguration));

        pp1.start();
        pp2.start();

        pp1.join();
        pp2.join();

        verify(mockConfiguration, times(8)).json();
    }
}
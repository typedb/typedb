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
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.NonReentrantLock;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.engine.tasks.TaskConfiguration;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import java.util.concurrent.locks.Lock;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.mockito.Mockito;

import static ai.grakn.util.REST.Request.KEYSPACE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PostProcessingTaskTest {

    private static final String TEST_KEYSPACE = UUID.randomUUID().toString();

    private String mockCastingIndex;
    private String mockResourceIndex;
    private Set<ConceptId> mockCastingSet;
    private Set<ConceptId> mockResourceSet;
    private TaskConfiguration mockConfiguration;
    private Consumer<TaskCheckpoint> mockConsumer;
    private PostProcessing mockPostProcessing;

    @BeforeClass
    public static void mockEngineCache(){
        Lock lock = new NonReentrantLock();
        LockProvider.add(PostProcessingTask.LOCK_KEY, () -> lock);
    }

    @AfterClass
    public static void clearEngineCache(){
        LockProvider.clear();
    }

    @Before
    public void mockPostProcessing(){
        mockPostProcessing = mock(PostProcessing.class);
        mockConsumer = mock(Consumer.class);

        mockCastingIndex = UUID.randomUUID().toString();
        mockResourceIndex = UUID.randomUUID().toString();
        mockCastingSet = Sets.newHashSet();
        mockResourceSet = Sets.newHashSet();
        mockConfiguration = TaskConfiguration.of(
                Json.object(
                        KEYSPACE, TEST_KEYSPACE,
                    REST.Request.COMMIT_LOG_FIXING, Json.object(
                        Schema.BaseType.CASTING.name(), Json.object(mockCastingIndex, mockCastingSet),
                        Schema.BaseType.RESOURCE.name(), Json.object(mockResourceIndex, mockResourceSet)
                    ))
        );

        Mockito.reset(mockPostProcessing);
    }

    @Test
    public void whenPPTaskStartCalledAndNotEnoughTimeElapsed_PostProcessingRunNotCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, Long.MAX_VALUE);

        task.start(mockConsumer, mockConfiguration);

        verify(mockPostProcessing, never())
                .performCastingFix(TEST_KEYSPACE, mockCastingIndex, mockCastingSet);
    }

    @Test
    public void whenPPTaskCalledWithCastingsToPP_PostProcessingPerformCastingsFixCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, 0);

        task.start(mockConsumer, mockConfiguration);

        verify(mockPostProcessing, times(1))
                .performCastingFix(TEST_KEYSPACE, mockCastingIndex, mockCastingSet);
    }

    @Test
    public void whenPPTaskCalledWithResourcesToPP_PostProcessingPerformResourcesFixCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, 0);

        task.start(mockConsumer, mockConfiguration);

        verify(mockPostProcessing, times(1))
                .performResourceFix(TEST_KEYSPACE, mockResourceIndex, mockResourceSet);
    }

    @Test
    public void whenPPTaskStartCalledAndNotEnoughTimeElapsed_PostProcessingStartReturnsTrue(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, Long.MAX_VALUE);

        assertTrue("Task " + task + " ran when it should not have", task.start(mockConsumer, mockConfiguration));
    }

    @Test
    public void whenPPTaskStartCalledAndPostProcessingRuns_PostProcessingStartReturnsFalse(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, 0);

        assertFalse("Task " + task + " did not run when it should have", task.start(mockConsumer, mockConfiguration));
    }

    @Test
    public void whenPPTaskStopCalled_PostProcessingStopIsCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, 1000);

        task.stop();

        verify(mockPostProcessing, times(1)).stop();
    }

    @Test
    public void whenTwoPPTasksStartCalledInDifferentThreads_PostProcessingRunsTwice() throws InterruptedException {
        // Add a bunch of jobs to the cache
        PostProcessingTask task1 = new PostProcessingTask(mockPostProcessing, 0);
        PostProcessingTask task2 = new PostProcessingTask(mockPostProcessing, 0);

        Thread pp1 = new Thread(() -> {
            task1.start(mockConsumer, mockConfiguration);
        });
        Thread pp2 = new Thread(() -> {
            task2.start(mockConsumer, mockConfiguration);
        });

        pp1.start();
        pp2.start();

        pp1.join();
        pp2.join();

        verify(mockPostProcessing, times(2))
                .performCastingFix(TEST_KEYSPACE, mockCastingIndex, mockCastingSet);
    }
}

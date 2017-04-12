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

import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.cache.EngineCacheStandAlone;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.NonReentrantLock;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.function.Consumer;

import static ai.grakn.engine.postprocessing.PostProcessingTask.LOCK_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostProcessingTaskTest {

    private Json mockJson;
    private Consumer<TaskCheckpoint> mockConsumer;
    private PostProcessing mockPostProcessing;

    @BeforeClass
    public static void mockEngineCache(){
        EngineCacheProvider.init(EngineCacheStandAlone.getCache());
        LockProvider.add(LOCK_KEY, new NonReentrantLock());
    }

    @AfterClass
    public static void clearEngineCache(){
        EngineCacheStandAlone.getCache().getKeyspaces().forEach(k -> EngineCacheStandAlone.getCache().clearAllJobs(k));
        EngineCacheProvider.clearCache();
        LockProvider.clear();
    }

    @Before
    public void mockPostProcessing(){
        mockPostProcessing = mock(PostProcessing.class);
        mockConsumer = mock(Consumer.class);
        mockJson = mock(Json.class);
    }

    @Test
    public void whenPPTaskStartCalledAndEnoughTimeElapsed_PostProcessingRunIsCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, 0);

        task.start(mockConsumer, mockJson);

        verify(mockPostProcessing, times(1)).run();
    }

    @Test
    public void whenPPTaskStartCalledAndNotEnoughTimeElapsed_PostProcessingRunNotCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, Long.MAX_VALUE);

        task.start(mockConsumer, mockJson);

        verify(mockPostProcessing, times(0)).run();
    }

    @Test
    public void whenPPTaskStopCalled_PostProcessingStopIsCalled(){
        PostProcessingTask task = new PostProcessingTask(mockPostProcessing, 1000);

        task.stop();

        verify(mockPostProcessing, times(1)).stop();
    }

    @Test
    public void whenTwoPPTasksStartCalledInDifferentThreads_PostProcessingOnlyRunsOnce() throws InterruptedException {
        Object object = new Object();

        when(mockPostProcessing.run()).thenAnswer(invocation -> {
            synchronized (object){
                object.wait(Duration.ofMinutes(1).toMillis());
            }
            return true;
        });

        // Add a bunch of jobs to the cache
        PostProcessingTask task1 = new PostProcessingTask(mockPostProcessing, 0);
        PostProcessingTask task2 = new PostProcessingTask(mockPostProcessing, 0);

        Thread pp1 = new Thread(() -> {
            task1.start(mockConsumer, mockJson);

            synchronized (object) {
                object.notifyAll();
            }
        });
        Thread pp2 = new Thread(() -> {
            task2.start(mockConsumer, mockJson);

            synchronized (object) {
                object.notifyAll();
            }
        });

        pp1.start();
        pp2.start();

        pp1.join();
        pp2.join();

        verify(mockPostProcessing, times(1)).run();
    }
}

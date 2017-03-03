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

package ai.grakn.test.engine.tasks.manager;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.test.engine.tasks.ShortExecutionTestTask;
import mjson.Json;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.SCHEDULED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.TaskSchedule.at;
import static ai.grakn.engine.tasks.TaskSchedule.recurring;
import static ai.grakn.test.GraknTestEnv.hideLogs;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static java.time.Instant.now;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandaloneTaskManagerTest {
    private TaskManager taskManager;

    @Before
    public void setUp() {
        hideLogs();
        taskManager = new StandaloneTaskManager() ;
    }

    @Test
    public void testRunSingle() {
        TaskState task = createTask(CREATED, TaskSchedule.now(), Json.object());
        taskManager.addTask(task);

        // Wait for task to be executed.
        waitToFinish(task.getId());
        assertEquals(COMPLETED, taskManager.storage().getState(task.getId()).status());
    }

    private void waitToFinish(TaskId id) {
        TaskStateStorage storage = taskManager.storage();
        final long initial = new Date().getTime();

        while ((new Date().getTime())-initial < 10000) {
            if (storage.getState(id).status() == COMPLETED)
                break;

            try {
                Thread.sleep(100);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void consecutiveRunSingle() {
        // Schedule tasks
        List<TaskId> ids = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            TaskState task = createTask(CREATED, TaskSchedule.now(), Json.object());
            taskManager.addTask(task);
            ids.add(task.getId());
        }

        // Check that they all finished
        for(TaskId id: ids) {
            if(taskManager.storage().getState(id).status() != COMPLETED)
                waitToFinish(id);
            assertEquals(COMPLETED, taskManager.storage().getState(id).status());
        }
    }

    @Test
    public void concurrentConsecutiveRuns() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(this::consecutiveRunSingle);
        executorService.submit(this::consecutiveRunSingle);
        executorService.submit(this::consecutiveRunSingle);

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testRunRecurring() throws Exception {
        TaskState task = createTask(CREATED, recurring(now().plusSeconds(10), Duration.ofSeconds(100)), Json.object());
        taskManager.addTask(task);

        Thread.sleep(2000);

        assertTrue(ShortExecutionTestTask.startedCounter.get() > 1);

        // Stop task..
        taskManager.stopTask(task.getId(), null);
    }

    @Test
    public void testStopSingle() {
        TaskState task = createTask(CREATED, at(now().plusSeconds(10)), Json.object());
        taskManager.addTask(task);

        TaskStatus status = taskManager.storage().getState(task.getId()).status();
        assertTrue(status == SCHEDULED || status == RUNNING);

        taskManager.stopTask(task.getId(), this.getClass().getName());

        status = taskManager.storage().getState(task.getId()).status();
        assertEquals(STOPPED, status);
    }

    @Test
    public void consecutiveStopStart() {
        for (int i = 0; i < 100000; i++) {
            testStopSingle();
        }
    }

    @Test
    public void concurrentConsecutiveStopStart() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(this::consecutiveStopStart);
        executorService.submit(this::consecutiveStopStart);

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
}

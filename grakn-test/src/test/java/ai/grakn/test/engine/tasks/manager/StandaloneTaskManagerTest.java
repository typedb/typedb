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
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.ImmutableList;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.RUNNING;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.TaskSchedule.at;
import static ai.grakn.engine.tasks.TaskSchedule.recurring;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.failingTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static java.time.Instant.now;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitQuickcheck.class)
public class StandaloneTaskManagerTest {
    private TaskManager taskManager;

    @Before
    public void setUp() {
        taskManager = new StandaloneTaskManager(EngineID.me()) ;
    }

    @After
    public void shutdown() throws Exception {
        taskManager.close();
    }

    @Test
    public void testRunSingle() {
        TaskState task = createTask();
        taskManager.addLowPriorityTask(task);

        // Wait for task to be executed.
        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertEquals(COMPLETED, taskManager.storage().getState(task.getId()).status());
    }

    @Property(trials=10)
    public void afterRunning_AllNonFailingTasksAreRecordedAsCompleted(List<TaskState> tasks) {
        // Schedule tasks
        tasks.forEach(taskManager::addLowPriorityTask);

        waitForDoneStatus(taskManager.storage(), tasks);

        completableTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have completed.", taskManager.storage().getState(task).status(), is(COMPLETED))
        );
    }

    @Property(trials=10)
    public void afterRunning_AllFailingTasksAreRecordedAsFailed(List<TaskState> tasks) {
        // Schedule tasks
        tasks.forEach(taskManager::addLowPriorityTask);

        waitForDoneStatus(taskManager.storage(), tasks);

        failingTasks(tasks).forEach(task ->
                assertThat("Task " + task + " should have failed.", taskManager.storage().getState(task).status(), is(FAILED))
        );
    }

    @Test
    public void testRunRecurring() throws Exception {
        TaskState task = createTask(ShortExecutionMockTask.class, recurring(Duration.ofMillis(100)));
        taskManager.addLowPriorityTask(task);

        Thread.sleep(2000);

        assertTrue(ShortExecutionMockTask.startedCounter.get() > 1);

        // Stop task..
        taskManager.stopTask(task.getId());
    }

    @Test
    public void testStopSingle() {
        TaskState task = createTask(ShortExecutionMockTask.class, at(now().plusSeconds(1000)));
        taskManager.addLowPriorityTask(task);

        TaskStatus status = taskManager.storage().getState(task.getId()).status();
        assertTrue(status == CREATED || status == RUNNING);

        taskManager.stopTask(task.getId());

        status = taskManager.storage().getState(task.getId()).status();
        assertEquals(STOPPED, status);
    }

    @Test
    public void consecutiveStopStart() {
        // Disable excessive logs for this test
        ((Logger) LoggerFactory.getLogger(StandaloneTaskManager.class)).setLevel(Level.OFF);

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

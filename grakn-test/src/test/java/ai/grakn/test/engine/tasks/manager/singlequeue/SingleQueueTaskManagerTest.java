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

package ai.grakn.test.engine.tasks.manager.singlequeue;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.engine.tasks.mock.EndlessExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.generator.TaskStates.NewTask;
import ai.grakn.generator.TaskStates.WithClass;
import ai.grakn.test.EngineContext;
import com.google.common.collect.ImmutableList;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.util.List;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.cancelledTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.clearTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.completedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskFinishes;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskStarts;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(JUnitQuickcheck.class)
public class SingleQueueTaskManagerTest {

    private static TaskManager taskManager;

    @ClassRule
    public static final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @BeforeClass
    public static void setup() throws Exception{
        //TODO: Get rid of this patch when we better isolate tests
        EngineCacheProvider.clearCache();
        taskManager = new SingleQueueTaskManager(EngineID.me());
    }

    @AfterClass
    public static void closeTaskManager() throws Exception {
        taskManager.close();
    }

    @Before
    public void clearAllTasks(){
        clearTasks();
    }

    @Property(trials=10)
    public void afterSubmitting_AllTasksAreCompleted(List<@NewTask TaskState> tasks){
        tasks.forEach(taskManager::addTask);
        waitForStatus(taskManager.storage(), tasks, COMPLETED, FAILED);

        assertEquals(completableTasks(tasks), completedTasks());
    }

    @Ignore// Failing randomly - may be a race condition
    @Property(trials=10)
    public void whenStoppingATaskBeforeItsExecuted_TheTaskIsNotExecuted(@NewTask TaskState task) {
        taskManager.stopTask(task.getId());

        taskManager.addTask(task);

        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Ignore// Failing randomly - may be a race condition
    @Property(trials=10)
    public void whenStoppingATaskBeforeItsExecuted_TheTaskIsMarkedAsStopped(@NewTask TaskState task) {
        taskManager.stopTask(task.getId());

        taskManager.addTask(task);

        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertStatus(task, STOPPED);
    }

    @Property(trials=10)
    public void whenStoppingATaskDuringExecution_TheTaskIsCancelled(
            @NewTask @WithClass(EndlessExecutionMockTask.class) TaskState task) {
        whenTaskStarts(id -> taskManager.stopTask(id));

        taskManager.addTask(task);

        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertThat(completedTasks(), empty());
        assertThat(cancelledTasks(), contains(task.getId()));
    }

    @Property(trials=10)
    public void whenStoppingATaskDuringExecution_TheTaskIsMarkedAsStopped(
            @NewTask @WithClass(EndlessExecutionMockTask.class) TaskState task) {
        whenTaskStarts(id -> taskManager.stopTask(id));

        taskManager.addTask(task);

        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertStatus(task, STOPPED);
    }

    @Property(trials=10)
    public void whenStoppingATaskAfterExecution_TheTaskIsNotCancelled(@NewTask TaskState task) {
        whenTaskFinishes(id -> taskManager.stopTask(id));

        taskManager.addTask(task);

        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertThat(cancelledTasks(), empty());
    }

    @Property(trials=10)
    public void whenStoppingATaskAfterExecution_TheTaskIsMarkedAsCompleted(@NewTask TaskState task) {
        whenTaskFinishes(id -> taskManager.stopTask(id));

        taskManager.addTask(task);

        waitForDoneStatus(taskManager.storage(), ImmutableList.of(task));

        assertStatus(task, COMPLETED, FAILED);
    }

    private void assertStatus(TaskState task, TaskStatus... status) {
        assertTrue("Task not in storage", taskManager.storage().containsTask(task.getId()));
        assertThat(taskManager.storage().getState(task.getId()).status(), isOneOf(status));
    }
}

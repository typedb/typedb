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
 *
 */

package ai.grakn.test.engine;

import ai.grakn.client.TaskClient;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.mock.EndlessExecutionMockTask;
import ai.grakn.generator.TaskStates.NewTask;
import ai.grakn.generator.TaskStates.WithClass;
import ai.grakn.test.EngineContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.clearTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskStarts;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.completedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitQuickcheck.class)
public class GraknEngineServerIT {

    private static final int PORT1 = 4567;
    private static final int PORT2 = 5678;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine1 = EngineContext.startSingleQueueServer().port(PORT1);

    @ClassRule
    public static final EngineContext engine2 = EngineContext.startSingleQueueServer().port(PORT2);

    private TaskStateStorage storage;

    @Before
    public void setUp() {
        clearTasks();
        storage = engine1.getTaskManager().storage();
    }

    @Test
    public void whenCreatingTwoEngines_TheyHaveDifferentTaskManagers() {
        assertNotEquals(engine1.getTaskManager(), engine2.getTaskManager());
    }

    @Ignore // Failing randomly
    @Property(trials=10)
    public void whenSendingTasksToTwoEngines_TheyAllComplete(
            List<@NewTask TaskState> tasks1, List<@NewTask TaskState> tasks2) {

        List<TaskState> allTasks = Lists.newArrayList(tasks1);
        allTasks.addAll(tasks2);

        tasks1.forEach(engine1.getTaskManager()::addTask);
        tasks2.forEach(engine2.getTaskManager()::addTask);

        waitForStatus(storage, allTasks, COMPLETED, STOPPED, FAILED);

        assertEquals(completableTasks(allTasks), completedTasks());
    }

    @Ignore  // TODO: Fix this test - may be a race condition
    @Property(trials=10)
    public void whenEngine1StopsATaskBeforeExecution_TheTaskIsStopped(@NewTask TaskState task) {
        assertTrue(TaskClient.of("localhost", PORT1).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task);

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Ignore  // TODO: Fix this test - may be a race condition
    @Property(trials=10)
    public void whenEngine2StopsATaskBeforeExecution_TheTaskIsStopped(@NewTask TaskState task) {
        assertTrue(TaskClient.of("localhost", PORT2).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task);

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Property(trials=10)
    public void whenEngine1StopsATaskDuringExecution_TheTaskIsStopped(
            @NewTask @WithClass(EndlessExecutionMockTask.class) TaskState task) {
        whenTaskStarts(id -> TaskClient.of("localhost", PORT1).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task);

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Property(trials=10)
    public void whenEngine2StopsATaskDuringExecution_TheTaskIsStopped(
            @NewTask @WithClass(EndlessExecutionMockTask.class) TaskState task) {
        whenTaskStarts(id -> TaskClient.of("localhost", PORT2).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task);

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }
}

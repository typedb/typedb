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
import ai.grakn.engine.TaskId;
import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.tasks.mock.EndlessExecutionMockTask;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.clearTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.completedTasks;
import static ai.grakn.engine.tasks.mock.MockBackgroundTask.whenTaskStarts;
import ai.grakn.generator.TaskStates.WithClass;
import ai.grakn.test.EngineContext;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.configuration;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForDoneStatus;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class GraknEngineServerIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine1 = EngineContext.startSingleQueueServer();

    @ClassRule
    public static final EngineContext engine2 = EngineContext.startSingleQueueServer();

    private TaskStateStorage storage;

    @Before
    public void setUp() {
        clearTasks();
        storage = engine1.getTaskManager().storage();
        storage.clear();
    }

    @Test
    public void whenCreatingTwoEngines_TheyHaveDifferentTaskManagers() {
        assertNotEquals(engine1.getTaskManager(), engine2.getTaskManager());
    }

    @Property(trials=10)
    public void whenSendingTasksToTwoEngines_TheyAllComplete(
            List<TaskState> tasks1, List<TaskState> tasks2) {

        Set<TaskState> allTasks = new HashSet<>();
        allTasks.addAll(tasks1);
        allTasks.addAll(tasks2);

        tasks1.forEach((taskState) -> engine1.getTaskManager().addTask(taskState, configuration(taskState)));
        tasks2.forEach((taskState) -> engine2.getTaskManager().addTask(taskState, configuration(taskState)));

        waitForStatus(storage, allTasks, COMPLETED, STOPPED, FAILED);

        Multiset<TaskId> completableTasks = completableTasks(allTasks);
        assertEquals(completableTasks, completedTasks());

        Set<TaskState> tasks = engine1.getTaskManager().storage()
                .getTasks(null, null, null, null, 0, 0);

        assertThat(tasks.stream().filter(t -> t.status().equals(COMPLETED)).count(), equalTo((long)
                completableTasks.size()));
    }

    @Property(trials=10)
    @Ignore("Stop not implemented yet")
    public void whenEngine1StopsATaskBeforeExecution_TheTaskIsStopped(TaskState task) {
        assertTrue(TaskClient.of("localhost", engine1.port()).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task, configuration(task));

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Property(trials=10)
    @Ignore("Stop not implemented yet")
    public void whenEngine2StopsATaskBeforeExecution_TheTaskIsStopped(TaskState task) {
        assertTrue(TaskClient.of("localhost", engine2.port()).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task, configuration(task));

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Property(trials=10)
    @Ignore("Stop not implemented yet")
    public void whenEngine1StopsATaskDuringExecution_TheTaskIsStopped(
            @WithClass(EndlessExecutionMockTask.class) TaskState task) {
        whenTaskStarts(id -> TaskClient.of("localhost", engine1.port()).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task, configuration(task));

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }

    @Property(trials=10)
    @Ignore("Stop not implemented yet")
    public void whenEngine2StopsATaskDuringExecution_TheTaskIsStopped(
            @WithClass(EndlessExecutionMockTask.class) TaskState task) {
        whenTaskStarts(id -> TaskClient.of("localhost", engine2.port()).stopTask(task.getId()));

        engine1.getTaskManager().addTask(task, configuration(task));

        waitForDoneStatus(storage, ImmutableList.of(task));

        assertThat(completedTasks(), empty());
    }
}

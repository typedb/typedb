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

import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.generator.TaskStates.NewTask;
import ai.grakn.test.EngineContext;
import com.google.common.collect.Lists;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.TaskStatus.STOPPED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.clearTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnitQuickcheck.class)
public class GraknEngineServerIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine1 = EngineContext.startSingleQueueServer().port(4567);

    @ClassRule
    public static final EngineContext engine2 = EngineContext.startSingleQueueServer().port(5678);

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
}

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

package ai.grakn.test.engine.tasks.manager.multiqueue;

import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskManager;
import ai.grakn.test.EngineContext;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static junit.framework.TestCase.assertEquals;

public class MultiQueueTaskManagerTest {
    private MultiQueueTaskManager manager;

    @ClassRule
    public static final EngineContext engine = EngineContext.startMultiQueueServer();

    @Before
    public void setup() throws Exception {
        manager = (MultiQueueTaskManager) engine.getTaskManager();
    }

    /**
     * Run end to end test and assert that the state
     * is correct in zookeeper.
     */
    @Test
    public void endToEndTest(){
        final int startCount = ShortExecutionMockTask.startedCounter.get();

        Set<TaskState> tasks = createTasks(20);
        tasks.forEach(manager::addTask);

        waitForStatus(manager.storage(), tasks, COMPLETED);
        assertEquals(20, ShortExecutionMockTask.startedCounter.get()-startCount);
    }
}

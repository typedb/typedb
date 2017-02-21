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

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.TaskId;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.multiqueue.MultiQueueTaskManager;
import ai.grakn.test.EngineContext;
import ai.grakn.test.engine.tasks.TestTask;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiQueueTaskManagerTest {
    private MultiQueueTaskManager manager;

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @Before
    public void setup() throws Exception {
        manager = (MultiQueueTaskManager) engine.getTaskManager();
    }

    private void waitToFinish(TaskId id) {
        while (true) {
            try {
                TaskStatus status = manager.storage().getState(id).status();
                if (status == COMPLETED || status == FAILED) {
                    System.out.println(id + " ------> " + status);
                    break;
                }

                System.out.println("Checking " + id + " " + status);

                Thread.sleep(5000);
            } catch (Exception ignored){}
        }
    }

    /**
     * Run end to end test and assert that the state
     * is correct in zookeeper.
     */
    @Test
    public void endToEndTest(){
        Collection<TaskId> ids = new HashSet<>();
        final int startCount = TestTask.startedCounter.get();

        for(int i = 0; i < 20; i++) {
            TaskState task = new TaskState(TestTask.class).configuration(Json.object("name", "task" + i));
            manager.addTask(task);
            ids.add(task.getId());
        }

        ids.forEach(this::waitToFinish);
        assertTrue(ids.stream().map(m -> manager.storage().getState(m).status()).allMatch(s -> s == COMPLETED));
        assertEquals(20, TestTask.startedCounter.get()-startCount);
    }
}

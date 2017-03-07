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

import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.generator.TaskStates.Status;
import ai.grakn.generator.TaskStates.UniqueIds;
import ai.grakn.test.EngineContext;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

import java.util.List;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.clearCompletedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(JUnitQuickcheck.class)
public class SingleQueueTaskManagerTest {

    private static TaskManager taskManager;

    @ClassRule
    public static final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @BeforeClass
    public static void setup(){
        taskManager = new SingleQueueTaskManager(EngineID.of("me"));
    }

    @AfterClass
    public static void closeTaskManager() throws Exception {
        taskManager.close();
    }

    @Before
    public void clearTasks(){
        clearCompletedTasks();
    }

    @Property(trials=10)
    public void afterSubmitting_AllTasksAreCompleted(List<@UniqueIds @Status(CREATED) TaskState> tasks){
        tasks.forEach(taskManager::addTask);
        waitForStatus(taskManager.storage(), tasks, COMPLETED, FAILED);

        assertEquals(completableTasks(tasks), completedTasks());
    }
}

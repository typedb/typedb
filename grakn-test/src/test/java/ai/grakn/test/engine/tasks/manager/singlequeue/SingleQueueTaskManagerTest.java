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
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskRunner;
import ai.grakn.generator.TaskStates.Status;
import ai.grakn.generator.TaskStates.UniqueIds;
import ai.grakn.test.EngineContext;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
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

    @Rule
    public final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @Before
    public void setup(){
        ((Logger) org.slf4j.LoggerFactory.getLogger(SingleQueueTaskRunner.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(SingleQueueTaskManager.class)).setLevel(Level.DEBUG);
        taskManager = new SingleQueueTaskManager();
    }

    @After
    public void tearDown() throws Exception {
        taskManager.close();
        clearCompletedTasks();
    }

    @Property(trials=10)
    public void afterSubmitting_AllTasksAreCompleted(List<@UniqueIds @Status(CREATED) TaskState> tasks){
        tasks.forEach(taskManager::addTask);
        waitForStatus(taskManager.storage(), tasks, COMPLETED, FAILED);

        assertEquals(completableTasks(tasks), completedTasks());
    }
}

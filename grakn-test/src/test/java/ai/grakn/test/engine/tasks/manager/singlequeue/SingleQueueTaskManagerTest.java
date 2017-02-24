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
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static ai.grakn.engine.TaskStatus.COMPLETED;
import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.clearCompletedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completableTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.completedTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTasks;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.waitForStatus;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class SingleQueueTaskManagerTest {

    private static TaskManager taskManager;

    @ClassRule
    public static final EngineContext kafkaServer = EngineContext.startKafkaServer();

    @BeforeClass
    public static void setup(){
        ((Logger) org.slf4j.LoggerFactory.getLogger(SingleQueueTaskRunner.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(SingleQueueTaskManager.class)).setLevel(Level.DEBUG);
        taskManager = new SingleQueueTaskManager();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        taskManager.close();
    }

    @After
    public void clear(){
        clearCompletedTasks();
    }

    @Test
    @Ignore
    public void afterSubmitting_AllTasksAreCompleted(){
        Set<TaskState> tasks = createTasks(10, CREATED);
        tasks.forEach(taskManager::addTask);
        waitForStatus(taskManager.storage(), tasks, COMPLETED);

        assertEquals(tasks.stream().map(TaskState::getId).collect(toSet()), newHashSet(completedTasks()));
    }
}

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

package ai.grakn.test.client;

import ai.grakn.Grakn;
import ai.grakn.client.TaskClient;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.test.engine.tasks.ShortExecutionTestTask;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Service;

import java.time.Duration;
import java.time.Instant;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskClientTest {

    private static TaskClient client;
    private static TaskManager manager;
    private static Service spark;

    @BeforeClass
    public static void setUp() {
        client = TaskClient.of(Grakn.DEFAULT_URI);

        spark = Service.ignite();
        spark.port(4567);

        manager = mock(TaskManager.class);
        when(manager.storage()).thenReturn(mock(TaskStateStorage.class));

        new TasksController(spark, manager);

        spark.awaitInitialization();
    }

    @AfterClass
    public static void tearDown() {
        spark.stop();
    }

    @Test
    public void whenSendingATask_TheTaskManagerReceivedTheTask(){
        Class taskClass = ShortExecutionTestTask.class;
        String creator = this.getClass().getName();
        Instant runAt = now();
        Duration interval = Duration.ofSeconds(1);
        Json configuration = Json.nil();

        TaskId identifier = client.sendTask(taskClass, creator, runAt, interval, configuration);

        verify(manager).addTask(argThat(argument ->
                argument.getId().equals(identifier)
                && argument.taskClass().equals(taskClass)
                && argument.configuration().equals(configuration)
                && argument.schedule().runAt().equals(runAt)
                && argument.schedule().interval().get().equals(interval)
                && argument.creator().equals(creator)));
    }

    @Test
    public void whenGettingStatusOfATask_TheTaskManagerReceivedTheRequest(){
        TaskState task = createTask();
        when(manager.storage().getState(task.getId())).thenReturn(task);

        client.getStatus(task.getId());

        verify(manager.storage()).getState(eq(task.getId()));
    }

    @Test
    public void whenGettingStatusOfATask_TheTaskManagerReturnsAStatus(){
        TaskState task = createTask();
        when(manager.storage().getState(task.getId())).thenReturn(task);

        TaskStatus status = client.getStatus(task.getId());

        assertThat(status, equalTo(CREATED));
    }

    @Test
    public void whenStoppingATask_TheTaskManagerIsToldToStopTheTask() {
        TaskId taskId = TaskId.generate();

        assertTrue(client.stopTask(taskId));

        verify(manager).stopTask(eq(taskId), any());
    }

    @Test
    public void whenStoppingATaskAndThereIsAnError_ReturnFalse() {
        TaskId taskId = TaskId.generate();

        doThrow(new RuntimeException("out of cheese error")).when(manager).stopTask(any(), any());

        assertFalse(client.stopTask(taskId));
    }
}
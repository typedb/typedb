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

package ai.grakn.test.client;

import static ai.grakn.engine.TaskStatus.CREATED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;

import ai.grakn.client.TaskClient;
import com.codahale.metrics.MetricRegistry;
import static java.time.Instant.now;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.test.SparkContext;

import java.time.Duration;
import java.time.Instant;
import mjson.Json;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TaskClientTest {

    private static TaskClient client;
    private static TaskManager manager = mock(TaskManager.class);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SparkContext ctx = SparkContext.withControllers(spark -> {
        new TasksController(spark, manager, new MetricRegistry());
    });

    @Before
    public void setUp() {
        client = TaskClient.of("localhost", ctx.port());
        when(manager.storage()).thenReturn(mock(TaskStateStorage.class));
    }

    @Test
    public void whenSendingATask_TheTaskManagerReceivedTheTask(){
        Class<?> taskClass = ShortExecutionMockTask.class;
        String creator = this.getClass().getName();
        Instant runAt = now();
        Duration interval = Duration.ofSeconds(1);
        Json configuration = Json.nil();

        TaskId identifier = client.sendTask(taskClass, creator, runAt, interval, configuration,
                true).getTaskId();

        verify(manager).runTask(argThat(argument ->
                argument.getId().equals(identifier)
                && argument.taskClass().equals(taskClass)
                && argument.schedule().runAt().equals(runAt)
                && argument.schedule().interval().get().equals(interval)
                && argument.creator().equals(creator)),
                argThat(argument -> argument.json().toString().equals(configuration.toString())));
    }

    @Test
    public void whenSendingATaskAndServerIsUnavailable_TheClientThrowsAnUnavailableException(){
        ctx.stop();

        try {
            Class<?> taskClass = ShortExecutionMockTask.class;
            String creator = this.getClass().getName();
            Instant runAt = now();
            Json configuration = Json.nil();

            exception.expect(GraknBackendException.class);
            client.sendTask(taskClass, creator, runAt, null, configuration, false);
        } finally {
            ctx.start();
        }
    }

    @Test
    public void whenGettingStatusOfATaskAndServerIsUnavailable_TheClientThrowsAnUnavailableException(){
        ctx.stop();

        try {
            TaskState task = createTask();

            exception.expect(GraknBackendException.class);
            client.getStatus(task.getId());
        } finally {
            ctx.start();
        }
    }

    @Test
    public void whenGettingStatusOfATaskAndSeverHasNotStoredTask_TheClientThrowsStorageException(){
        TaskState task = createTask();
        when(manager.storage().getState(task.getId()))
                .thenThrow(GraknBackendException.stateStorage());

        exception.expect(GraknBackendException.class);

        client.getStatus(task.getId());
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

        client.stopTask(taskId);

        verify(manager).stopTask(eq(taskId));
    }

    @Test
    public void whenStoppingATaskAndThereIsAnError_ReturnFalse() {
        TaskId taskId = TaskId.generate();

        doThrow(new RuntimeException("out of cheese error")).when(manager).stopTask(any());

        assertFalse(client.stopTask(taskId));
    }
}
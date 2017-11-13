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

package ai.grakn.client;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.exception.GraknBackendException;
import com.codahale.metrics.MetricRegistry;
import junit.framework.TestCase;
import mjson.Json;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;

import static ai.grakn.engine.TaskStatus.CREATED;
import static java.time.Instant.now;

// TODO: move this test into grakn-client when possible
public class TaskClientTest {

    private static TaskClient client;
    private static TaskManager manager = Mockito.mock(TaskManager.class);

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SparkContext ctx = SparkContext.withControllers(spark -> {
        new TasksController(spark, manager, new MetricRegistry());
    });

    @Before
    public void setUp() {
        client = TaskClient.of(ctx.uri());
        Mockito.when(manager.storage()).thenReturn(Mockito.mock(TaskStateStorage.class));
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

        Mockito.verify(manager).runTask(ArgumentMatchers.argThat(argument ->
                argument.getId().equals(identifier)
                && argument.taskClass().equals(taskClass)
                && argument.schedule().runAt().equals(runAt)
                && argument.schedule().interval().get().equals(interval)
                && argument.creator().equals(creator)),
                ArgumentMatchers.argThat(argument -> argument.json().toString().equals(configuration.toString())));
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
        Mockito.when(manager.storage().getState(task.getId()))
                .thenThrow(GraknBackendException.stateStorage());

        exception.expect(GraknBackendException.class);

        client.getStatus(task.getId());
    }

    @Test
    public void whenGettingStatusOfATask_TheTaskManagerReceivedTheRequest(){
        TaskState task = createTask();
        Mockito.when(manager.storage().getState(task.getId())).thenReturn(task);

        client.getStatus(task.getId());

        Mockito.verify(manager.storage()).getState(ArgumentMatchers.eq(task.getId()));
    }

    @Test
    public void whenGettingStatusOfATask_TheTaskManagerReturnsAStatus(){
        TaskState task = createTask();
        Mockito.when(manager.storage().getState(task.getId())).thenReturn(task);

        TaskStatus status = client.getStatus(task.getId());

        MatcherAssert.assertThat(status, IsEqual.equalTo(CREATED));
    }

    @Test
    public void whenStoppingATask_TheTaskManagerIsToldToStopTheTask() {
        TaskId taskId = TaskId.generate();

        client.stopTask(taskId);

        Mockito.verify(manager).stopTask(ArgumentMatchers.eq(taskId));
    }

    @Test
    public void whenStoppingATaskAndThereIsAnError_ReturnFalse() {
        TaskId taskId = TaskId.generate();

        Mockito.doThrow(new RuntimeException("out of cheese error")).when(manager).stopTask(ArgumentMatchers.any());

        TestCase.assertFalse(client.stopTask(taskId));
    }

    private TaskState createTask() {
        return TaskState.of(ShortExecutionMockTask.class, TaskClient.class.getName(), TaskSchedule.now(), TaskState.Priority.LOW);
    }
}
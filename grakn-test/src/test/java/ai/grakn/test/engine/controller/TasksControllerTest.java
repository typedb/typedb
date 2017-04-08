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

package ai.grakn.test.engine.controller;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.tasks.TaskStateStorage;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.mapper.ObjectMapper;
import com.jayway.restassured.mapper.ObjectMapperDeserializationContext;
import com.jayway.restassured.mapper.ObjectMapperSerializationContext;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import spark.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static ai.grakn.engine.GraknEngineServer.configureSpark;
import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.test.engine.tasks.BackgroundTaskTestUtils.createTask;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_PARAMETERS;
import static ai.grakn.util.ErrorMessage.UNAVAILABLE_TASK_CLASS;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.Tasks.GET;
import static ai.grakn.util.REST.WebPath.Tasks.TASKS;
import static com.jayway.restassured.RestAssured.with;
import static java.time.Instant.now;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TasksControllerTest {
    private static final int PORT = 4567;
    private static final String URI = "localhost:" + PORT;
    private static Service spark;
    private static TaskManager manager;
    private final JsonMapper jsonMapper = new JsonMapper();

    //TODO tests for stopping and getting multiple

    @BeforeClass
    public static void setUp() throws Exception {
        spark = Service.ignite();
        configureSpark(spark, PORT);

        manager = mock(TaskManager.class);
        when(manager.storage()).thenReturn(mock(TaskStateStorage.class));

        new TasksController(spark, manager);

        spark.awaitInitialization();
    }

    @AfterClass
    public static void stopSpark() throws IOException {
        spark.stop();

        // Block until server is truly stopped
        // This occurs when there is no longer a port assigned to the Spark server
        boolean running = true;
        while (running) {
            try {
                spark.port();
            } catch(IllegalStateException e){
                running = false;
            }
        }
    }

    @Test
    public void afterSendingTask_ItReceivedByStorage(){
        send();

        verify(manager, atLeastOnce()).addTask(
                argThat(argument -> argument.taskClass().equals(ShortExecutionMockTask.class)));
    }

    @Test
    public void afterSendingTask_TheResponseContainsTheCorrectIdentifier(){
        Response response = send();

        assertThat(response.getBody().as(Json.class, jsonMapper).at("id"), notNullValue());
    }

    @Test
    public void afterSendingTask_TheResponseTypeIsJson(){
        Response response = send();

        assertThat(response.contentType(), equalTo(ContentType.APPLICATION_JSON.getMimeType()));
    }

    @Test
    public void afterSendingTaskWithRunAt_ItIsDelayedInStorage(){
        Instant runAt = now();
        send(Json.object().toString(), defaultParams());

        verify(manager).addTask(argThat(argument -> argument.schedule().runAt().equals(runAt)));
    }

    @Test
    public void afterSendingTaskWithInterval_ItIsRecurringInStorage(){
        Duration interval = Duration.ofSeconds(1);
        send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli()),
                        TASK_RUN_INTERVAL_PARAMETER, Long.toString(interval.toMillis())
                )
        );

        verify(manager).addTask(argThat(argument -> argument.schedule().interval().isPresent()));
        verify(manager).addTask(argThat(argument -> argument.schedule().isRecurring()));
    }

    @Test
    public void afterSendingTaskWithMissingClassName_Grakn400IsThrown(){
        Response response = send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli())
                )
        );

        String exception = response.getBody().as(Json.class, jsonMapper).at("exception").asString();
        assertThat(exception, containsString(MISSING_MANDATORY_PARAMETERS.getMessage(TASK_CLASS_NAME_PARAMETER)));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMissingCreatedBy_Grakn400IsThrown(){
        Response response = send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli())
                )
        );

        String exception = response.getBody().as(Json.class, jsonMapper).at("exception").asString();
        assertThat(exception, containsString(MISSING_MANDATORY_PARAMETERS.getMessage(TASK_CREATOR_PARAMETER)));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMissingRunAt_Grakn400IsThrown(){
        Response response = send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName()
                )
        );

        String exception = response.getBody().as(Json.class, jsonMapper).at("exception").asString();
        assertThat(exception, containsString(MISSING_MANDATORY_PARAMETERS.getMessage(TASK_RUN_AT_PARAMETER)));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithInvalidBackgroundTaskClassName_Grakn400IsThrown(){
        Response response = send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, this.getClass().getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli())
                )
        );

        String exception = response.getBody().as(Json.class, jsonMapper).at("exception").asString();
        assertThat(exception, containsString(UNAVAILABLE_TASK_CLASS.getMessage(this.getClass().getName())));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMalformedInterval_Grakn400IsThrown(){
        Response response = send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli()),
                        TASK_RUN_INTERVAL_PARAMETER, "malformed"
                )
        );

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMalformedConfiguration_Grakn400IsThrown(){
        Response response = send("non-json configuration");
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMalformedRunAt_Grakn400IsThrown(){
        Response response = send(Json.object().toString(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, ""
                )
        );

        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void whenGettingTaskById_TaskIsReturned(){
        TaskState task = createTask();

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());
        Json json = response.as(Json.class, jsonMapper);

        assertThat(json.at("id").asString(), equalTo(task.getId().getValue()));
        assertThat(json.at(TASK_CLASS_NAME_PARAMETER).asString(), equalTo(task.taskClass().getName()));
        assertThat(json.at(TASK_CREATOR_PARAMETER).asString(), equalTo(task.creator()));
        assertThat(json.at(TASK_RUN_AT_PARAMETER).asLong(), equalTo(task.schedule().runAt().toEpochMilli()));
        assertThat(json.at(TASK_STATUS_PARAMETER).asString(), equalTo(task.status().name()));
        assertThat(json.at("configuration"), equalTo(task.configuration()));
    }

    @Test
    public void whenGettingTaskById_TheResponseStatusIs200(){
        TaskState task = createTask();

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());

        assertThat(response.statusCode(), equalTo(200));
    }

    @Test
    public void whenGettingTaskById_TheResponseTypeIsJson(){
        TaskState task = createTask();

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());

        assertThat(response.contentType(), equalTo(ContentType.APPLICATION_JSON.getMimeType()));
    }

    @Test
    public void whenGettingTaskByIdRecurring_TaskIsReturned(){
        Duration duration = Duration.ofMillis(100);
        TaskState task = createTask(ShortExecutionMockTask.class, TaskSchedule.recurring(duration));

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());
        Json json = response.as(Json.class, jsonMapper);

        assertThat(json.at("id").asString(), equalTo(task.getId().getValue()));
        assertThat(json.at(TASK_RUN_INTERVAL_PARAMETER).asLong(), equalTo(task.schedule().interval().get().toMillis()));
    }

    @Test
    public void whenGettingTaskByIdRunning_TaskIsReturned(){
        EngineID engineId = EngineID.me();
        TaskState task = createTask().markRunning(engineId);

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());
        Json json = response.as(Json.class, jsonMapper);

        assertThat(json.at("id").asString(), equalTo(task.getId().getValue()));
        assertThat(json.at("engineID").asString(), equalTo(engineId.value()));
    }

    @Test
    public void whenGettingTaskByIdDelayed_TaskIdReturned(){
        Instant runAt = Instant.now().plusMillis(10);
        TaskState task = createTask(ShortExecutionMockTask.class, TaskSchedule.at(runAt));

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());
        Json json = response.as(Json.class, jsonMapper);

        assertThat(json.at("id").asString(), equalTo(task.getId().getValue()));
        assertThat(json.at(TASK_RUN_AT_PARAMETER).asLong(), equalTo(runAt.toEpochMilli()));
    }

    @Test
    public void whenGettingTaskByIdFailed_ItIsReturned(){
        Exception exception = new RuntimeException();
        TaskState task = createTask().markFailed(exception);

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());
        Json json = response.as(Json.class, jsonMapper);

        assertThat(json.at("id").asString(), equalTo(task.getId().getValue()));
        assertThat(json.at(TASK_STATUS_PARAMETER).asString(), equalTo(FAILED.name()));
        assertThat(json.at("stackTrace").asString(), equalTo(getFullStackTrace(exception)));

    }

    private Map<String, String> defaultParams(){
        return ImmutableMap.of(
                TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                TASK_CREATOR_PARAMETER, this.getClass().getName(),
                TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli())
        );
    }

    private Response send(){
        return send(Json.object().toString(), defaultParams());
    }

    private Response send(String configuration){
        return send(configuration, defaultParams());
    }

    private Response send(String configuration, Map<String, String> params){
        RequestSpecification request = with().queryParams(params).body(configuration);
        return request.post(String.format("http://%s%s", URI, TASKS));
    }

    private Response get(TaskId taskId){
        return with().get(String.format("http://%s%s", URI, GET.replace(ID_PARAMETER, taskId.getValue())));
    }

    public static class JsonMapper implements ObjectMapper{

        @Override
        public Object deserialize(ObjectMapperDeserializationContext objectMapperDeserializationContext) {
            return Json.read(objectMapperDeserializationContext.getDataToDeserialize().asString());
        }

        @Override
        public Object serialize(ObjectMapperSerializationContext objectMapperSerializationContext) {
            return objectMapperSerializationContext.getObjectToSerialize().toString();
        }
    }
}

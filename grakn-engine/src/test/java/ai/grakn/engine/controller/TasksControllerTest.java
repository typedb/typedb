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

package ai.grakn.engine.controller;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskStateStorage;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.exception.GraknBackendException;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.config.ObjectMapperConfig;
import com.jayway.restassured.config.RestAssuredConfig;
import com.jayway.restassured.mapper.ObjectMapper;
import com.jayway.restassured.mapper.ObjectMapperDeserializationContext;
import com.jayway.restassured.mapper.ObjectMapperSerializationContext;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import mjson.Json;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.UriBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.controller.Utilities.createTask;
import static ai.grakn.engine.controller.Utilities.exception;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.UNAVAILABLE_TASK_CLASS;
import static ai.grakn.util.REST.Request.CONFIGURATION_PARAM;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_PRIORITY_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.Tasks.GET;
import static ai.grakn.util.REST.WebPath.Tasks.TASK;
import static com.jayway.restassured.RestAssured.with;
import static java.time.Instant.now;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TasksControllerTest {
    private static TaskManager manager = mock(TaskManager.class);
    private final JsonMapper jsonMapper = new JsonMapper();

    @ClassRule
    public static final SparkContext ctx = SparkContext.withControllers(spark -> {
        new TasksController(spark, manager, new MetricRegistry());
    });

    @Before
    public void reset(){
        Mockito.reset(manager);
        when(manager.storage()).thenReturn(mock(TaskStateStorage.class));
    }

    @Test
    public void afterSendingTask_ItReceivedByStorage(){
        send();

        verify(manager, atLeastOnce()).runTask(
                argThat(argument -> argument.taskClass().equals(ShortExecutionMockTask.class)), any());
    }

    @Test
    public void afterSendingTask_TheResponseContainsTheCorrectIdentifier(){
        Response response = send();

        assertThat(response.getBody().as(Json.class, jsonMapper).at(0).at("id"), notNullValue());
    }

    @Test
    public void afterSendingTask_TheResponseTypeIsJson(){
        Response response = send();

        assertThat(response.contentType(), equalTo(ContentType.APPLICATION_JSON.getMimeType()));
    }

    @Test
    public void afterSendingTaskWithRunAt_ItIsDelayedInStorage(){
        Instant runAt = now();

        Map<String, String> defaultParams = defaultParams();
        defaultParams.put(TASK_RUN_AT_PARAMETER, Long.toString(runAt.toEpochMilli()));
        send(Collections.emptyMap(), defaultParams);

        verify(manager).runTask(argThat(argument -> argument.schedule().runAt().equals(runAt)), any());
    }

    @Test
    public void afterSendingTaskWithDifferentConf_ResponseWellFormed(){
        ImmutableList<Json> tasks = ImmutableList
                .of(makeJsonTask(ImmutableMap.of("conf_key_1", "conf_value_1"), defaultParams()),
                        makeJsonTask(ImmutableMap.of("conf_key_2", "conf_value_2"),
                                defaultParams()));
        Response response = send(tasks);
        assertThat(response.getBody().as(Json.class, jsonMapper).at(0).at("id"), notNullValue());
        assertThat(response.getBody().as(Json.class, jsonMapper).at(1).at("id"), notNullValue());
    }

    @Test
    public void afterSendingTaskWithInterval_ItIsRecurringInStorage(){
        Duration interval = Duration.ofSeconds(1);
        send(Collections.emptyMap(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli()),
                        TASK_RUN_INTERVAL_PARAMETER, Long.toString(interval.toMillis())
                )
        );

        verify(manager).runTask(argThat(argument -> argument.schedule().interval().isPresent()), any());
        verify(manager).runTask(argThat(argument -> argument.schedule().isRecurring()), any());
    }

    @Test
    public void afterSendingTaskWithMissingClassName_Grakn400IsThrown(){
        Response response = sendDefaultMinus(TASK_CLASS_NAME_PARAMETER);

        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(TASK_CLASS_NAME_PARAMETER)));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMissingCreatedBy_Grakn400IsThrown(){
        Response response = sendDefaultMinus(TASK_CREATOR_PARAMETER);

        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(TASK_CREATOR_PARAMETER)));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMissingRunAt_Grakn400IsThrown(){
        Response response = sendDefaultMinus(TASK_RUN_AT_PARAMETER);

        assertThat(exception(response), containsString(MISSING_MANDATORY_REQUEST_PARAMETERS.getMessage(TASK_RUN_AT_PARAMETER)));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithInvalidPriority_Grakn400IsThrown(){
        Response response = send(Collections.emptyMap(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli()),
                        TASK_PRIORITY_PARAMETER, "invalid"
                )
        );

        assertThat(exception(response), containsString("Exception on Grakn engine"));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMissingPriority_TaskSubmittedWithDefaultLowPriority(){
        send();

        verify(manager).runTask(argThat(argument -> argument.priority().equals(TaskState.Priority.LOW)), any());
    }

    @Test
    public void afterSendingTaskWithLowPriority_TaskSubmittedWithLowPriority(){
        send(TaskState.Priority.LOW);

        verify(manager).runTask(argThat(argument -> argument.priority().equals(TaskState.Priority.LOW)), any());
    }

    @Test
    public void afterSendingTaskWithHighPriority_TaskSubmittedWithHighPriority(){
        send(TaskState.Priority.HIGH);

        verify(manager).runTask(argThat(argument -> argument.priority().equals(TaskState.Priority.HIGH)), any());
    }

    @Test
    public void afterSendingTaskWithInvalidBackgroundTaskClassName_Grakn400IsThrown(){
        Response response = send(Collections.emptyMap(),
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
        Response response = send(Collections.emptyMap(),
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
        Json malformed = Json.make(defaultParams())
                .set(CONFIGURATION_PARAM, "non-json configuration");
        Response response = send(ImmutableList.of(malformed));
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingTaskWithMalformedRunAt_Grakn400IsThrown(){
        Response response = send(Collections.emptyMap(),
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_CREATOR_PARAMETER, this.getClass().getName(),
                        TASK_RUN_AT_PARAMETER, ""
                )
        );
        assertThat(response.statusCode(), equalTo(400));
    }

    @Test
    public void afterSendingWellFormedNBulk_NTaskSubmitted() {
        Map<String, String> params = new HashMap<>(defaultParams());
        params.put(TASK_PRIORITY_PARAMETER, TaskState.Priority.LOW.name());
        int nOfTasks = 10;
        Response response = send(Collections.emptyMap(), params, nOfTasks);
        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        verify(manager, times(nOfTasks))
                .runTask(argThat(argument -> argument.priority().equals(TaskState.Priority.LOW)),
                        any());
        assertThat(Json.read(response.getBody().asString()).asJsonList().stream()
                .allMatch(e -> e.at("code").asInteger() == HttpStatus.SC_OK), equalTo(true));
    }

    @Test
    public void afterSendingPartiallyWellFormedNBulk_BadRequest(){
        Map<String, String> params = new HashMap<>(defaultParams());
        params.put(TASK_PRIORITY_PARAMETER, TaskState.Priority.LOW.name());
        int nOfTasks = 10;
        Response response = send(Collections.emptyMap(), ImmutableMap.of(
                // Missing required parameter
                TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                TASK_RUN_AT_PARAMETER, ""), nOfTasks);
        assertThat(response.statusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    @Test
    public void afterSendingBulkWithNoTask_OkAndEmptyResponse(){
        Response response = send(ImmutableList.of());
        assertThat(response.statusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(Json.read(response.body().asString()), equalTo(Json.array()));
    }

    @Test
    public void afterSendingSingleTaskInBulkWithMalformedRunAt_BadRequest(){
        Response response = send(Collections.emptyMap(),
                // Missing required parameter
                ImmutableMap.of(
                        TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName(),
                        TASK_RUN_AT_PARAMETER, ""));
        assertThat(response.statusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    @Test
    public void whenGettingTaskById_TaskIsReturned(){
        TaskState task = createTask();

        when(manager.storage().getState(task.getId())).thenReturn(task);

        Response response = get(task.getId());
        Json json = response.as(Json.class, jsonMapper);

        assertThat(json.at("id").asString(), equalTo(task.getId().getValue()));
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
    public void whenGettingNonExistingTaskById_TheResponseStatusIs404(){
        TaskState task = createTask();

        when(manager.storage().getState(task.getId())).thenThrow(GraknBackendException.stateStorageMissingId(task.getId()));

        Response response = get(task.getId());

        assertThat(response.statusCode(), equalTo(404));
    }

    @Test
    @Ignore("Recurring task not implemented")
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
    }

    @Test
    @Ignore("Delayed task not implemented")
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
    }

    private Map<String, String> defaultParams(){
        Map<String, String> params = new HashMap<>();
        params.put(TASK_CLASS_NAME_PARAMETER, ShortExecutionMockTask.class.getName());
        params.put(TASK_CREATOR_PARAMETER, this.getClass().getName());
        params.put(TASK_RUN_AT_PARAMETER, Long.toString(now().toEpochMilli()));

        return params;
    }

    private Response send(){
        return send(Collections.emptyMap(), defaultParams());
    }

    private Response send(TaskState.Priority priority){
        Map<String, String> params = defaultParams();
        params.put(TASK_PRIORITY_PARAMETER, priority.name());
        return send(Collections.emptyMap(), params);
    }

    private Response sendDefaultMinus(String property){
        Map<String, String> params = new HashMap<>(defaultParams());
        params.remove(property);
        return send(Collections.emptyMap(), params);
    }

    private Response send(Map<String, String> configuration, Map<String, String> params){
        return send(configuration, params, 1);
    }

    private Response send(Map<String, String> configuration, Map<String, String> params, int times){
        Json jsonParams = makeJsonTask(configuration, params);
        return send(copy(times, jsonParams));
    }

    // Using this because nCopies is not serialised correctly into Json
    private ImmutableList<Json> copy(int times, Json jsonParams) {
        Builder<Json> builder = ImmutableList.builder();
        for(int i = 0; i < times; i++) {
            builder.add((Json)org.apache.commons.lang.SerializationUtils.clone(jsonParams));
        }
        return builder.build();
    }

    private Response send(ImmutableList<Json> tasks){
        Json tasksList = Json.object().set("tasks", tasks);
        RequestSpecification request = with()
                .config(new RestAssuredConfig().objectMapperConfig(new ObjectMapperConfig(jsonMapper)))
                .body(tasksList);
        return request.post(UriBuilder.fromUri(ctx.uri().toURI()).path(TASK).build());
    }

    private Response get(TaskId taskId){
        return with().get(GET.replace(ID_PARAMETER, taskId.getValue()));
    }

    private Json makeJsonTask(Map<String, String> configuration, Map<String, String> params) {
        return Json.make(params).set(CONFIGURATION_PARAM, Json.make(configuration));
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

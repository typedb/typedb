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

package ai.grakn.engine.controller;

import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.exception.EngineStorageException;
import ai.grakn.exception.GraknEngineServerException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static ai.grakn.engine.controller.GraqlController.mandatoryQueryParameter;
import static ai.grakn.engine.tasks.TaskSchedule.recurring;
import static ai.grakn.util.ErrorMessage.UNAVAILABLE_TASK_CLASS;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.OFFSET_PARAM;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.Tasks.GET;
import static ai.grakn.util.REST.WebPath.Tasks.STOP;
import static ai.grakn.util.REST.WebPath.Tasks.TASKS;
import static java.lang.Long.parseLong;
import static java.time.Instant.ofEpochMilli;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * <p>
 *     Endpoints used to query and control queued background tasks.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
@Path("/tasks")
@Api(value = "/tasks", description = "Endpoints used to query and control queued background tasks.", produces = "application/json")
public class TasksController {
    private final TaskManager manager;

    public TasksController(Service spark, TaskManager manager) {
        if (manager==null) {
            throw new GraknEngineServerException(500,"Task manager has not been instantiated.");
        }
        this.manager = manager;

        spark.get(TASKS,       this::getTasks);
        spark.get(GET,         this::getTask);
        spark.put(STOP,        this::stopTask);
        spark.post(TASKS,      this::createTask);

        spark.exception(EngineStorageException.class, (e, req, res) -> handleNotFoundInStorage(e, res));
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Get tasks matching a specific TaskStatus.")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "status", value = "TaskStatus as string.", dataType = "string", paramType = "query"),
        @ApiImplicitParam(name = "className", value = "Class name of BackgroundTask Object.", dataType = "string", paramType = "query"),
        @ApiImplicitParam(name = "creator", value = "Who instantiated these tasks.", dataType = "string", paramType = "query"),
        @ApiImplicitParam(name = "limit", value = "Limit the number of entries in the returned result.", dataType = "integer", paramType = "query"),
        @ApiImplicitParam(name = "offset", value = "Use in conjunction with limit for pagination.", dataType = "integer", paramType = "query")
    })
    private Json getTasks(Request request, Response response) {
        TaskStatus status = null;
        String className = request.queryParams(TASK_CLASS_NAME_PARAMETER);
        String creator = request.queryParams(TASK_CREATOR_PARAMETER);
        int limit = 0;
        int offset = 0;

        if(request.queryParams(LIMIT_PARAM) != null) {
            limit = Integer.parseInt(request.queryParams(LIMIT_PARAM));
        }

        if(request.queryParams(OFFSET_PARAM) != null) {
            offset = Integer.parseInt(request.queryParams(OFFSET_PARAM));
        }

        if(request.queryParams(TASK_STATUS_PARAMETER) != null) {
            status = TaskStatus.valueOf(request.queryParams(TASK_STATUS_PARAMETER));
        }

        Json result = Json.array();
        manager.storage()
                .getTasks(status, className, creator, null, limit, offset).stream()
                .map(this::serialiseStateSubset)
                .forEach(result::add);

        response.status(200);
        response.type(APPLICATION_JSON.getMimeType());

        return result;
    }

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Get the state of a specific task by its ID.", produces = "application/json")
    @ApiImplicitParam(name = "uuid", value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private Json getTask(Request request, Response response) {
        String id = request.params("id");

        response.status(200);
        response.type("application/json");

        return serialiseStateFull(manager.storage().getState(TaskId.of(id)));
    }

    @PUT
    @Path("/{id}/stop")
    @ApiOperation(value = "Stop a running or paused task.")
    @ApiImplicitParam(name = "uuid", value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private Json stopTask(Request request, Response response) {
        String id = request.params(ID_PARAMETER);
        manager.stopTask(TaskId.of(id));
        return Json.object();
    }

    @POST
    @Path("/")
    @ApiOperation(value = "Schedule a task.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "className", value = "Class name of object implementing the BackgroundTask interface", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "createdBy", value = "String representing the user scheduling this task", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "runAt", value = "Time to run at as milliseconds since the UNIX epoch", required = true, dataType = "long", paramType = "query"),
            @ApiImplicitParam(name = "interval",value = "If set the task will be marked as recurring and the value will be the time in milliseconds between repeated executions of this task. Value should be as Long.",
                    dataType = "long", paramType = "query"),
            @ApiImplicitParam(name = "configuration", value = "JSON Object that will be given to the task as configuration.", dataType = "String", paramType = "body")
    })
    private Json createTask(Request request, Response response) {
        String className = mandatoryQueryParameter(request, TASK_CLASS_NAME_PARAMETER);
        String createdBy = mandatoryQueryParameter(request, TASK_CREATOR_PARAMETER);
        String runAtTime = mandatoryQueryParameter(request, TASK_RUN_AT_PARAMETER);
        String intervalParam = request.queryParams(TASK_RUN_INTERVAL_PARAMETER);

        TaskSchedule schedule;
        Json configuration;
        try {
            // Get the schedule of the task
            Optional<Duration> optionalInterval = Optional.ofNullable(intervalParam).map(Long::valueOf).map(Duration::ofMillis);
            Instant time = ofEpochMilli(parseLong(runAtTime));
            schedule = optionalInterval
                    .map(interval -> recurring(time, interval))
                    .orElse(TaskSchedule.at(time));

            // Get the configuration of the task
            configuration = request.body().isEmpty() ? Json.object() : Json.read(request.body());
        } catch (Exception e){
            throw new GraknEngineServerException(400, e);
        }

        // Get the class of this background task
        Class<?> clazz = getClass(className);

        // Create and schedule the task
        TaskState taskState = TaskState.of(clazz, createdBy, schedule, configuration);
        manager.addTask(taskState);

        // Configure the response
        response.type(APPLICATION_JSON.getMimeType());
        response.status(200);

        return Json.object("id", taskState.getId().getValue());
    }

    /**
     * Use reflection to get a reference to the given class. Returns a 500 to the client if the class is
     * unavailable.
     *
     * @param className class to retrieve
     * @return reference to the given class
     */
    private Class<?> getClass(String className){
        try {
            Class<?> clazz = Class.forName(className);
            if (!BackgroundTask.class.isAssignableFrom(clazz)) {
                throw new GraknEngineServerException(400, UNAVAILABLE_TASK_CLASS, className);
            }

            return clazz;
        } catch (ClassNotFoundException e) {
            throw new GraknEngineServerException(400, UNAVAILABLE_TASK_CLASS, className);
        }
    }

    /**
     * Error accessing or retrieving a task from storage. This throws a 404 Task Not Found to the user.
     * @param exception {@link EngineStorageException} thrown by the server
     * @param response The response object providing functionality for modifying the response
     */
    private void handleNotFoundInStorage(Exception exception, Response response){
        response.status(404);
        response.body(Json.object("exception", exception.getMessage()).toString());
    }

    // TODO: Return 'schedule' object as its own object
    private Json serialiseStateSubset(TaskState state) {
        return Json.object()
                .set("id", state.getId().getValue())
                .set("status", state.status().name())
                .set("creator", state.creator())
                .set("className", state.taskClass().getName())
                .set("runAt", state.schedule().runAt().toEpochMilli())
                .set("recurring", state.schedule().isRecurring());
    }

    private Json serialiseStateFull(TaskState state) {
        return serialiseStateSubset(state)
                .set("interval", state.schedule().interval().map(Duration::toMillis).orElse(null))
                .set("recurring", state.schedule().isRecurring())
                .set("exception", state.exception())
                .set("stackTrace", state.stackTrace())
                .set("engineID", state.engineID() != null ? state.engineID().value() : null)
                .set("configuration", state.configuration());
    }
}

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
import ai.grakn.engine.tasks.TaskId;
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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static ai.grakn.engine.tasks.TaskSchedule.recurring;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.OFFSET_PARAM;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STOP;
import static ai.grakn.util.REST.WebPath.ALL_TASKS_URI;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

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
    private final Logger LOG = LoggerFactory.getLogger(TasksController.class);
    private final TaskManager manager;

    public TasksController(TaskManager manager) {
        if (manager==null) {
            throw new GraknEngineServerException(500,"Task manager has not been instantiated.");
        }
        this.manager = manager;

        get(ALL_TASKS_URI, this::getTasks);
        get(TASKS_URI + "/" + ID_PARAMETER, this::getTask);
        put(TASKS_URI + "/" + ID_PARAMETER + TASK_STOP, this::stopTask);
        post(TASKS_SCHEDULE_URI, this::scheduleTask);
    }

    @GET
    @Path("/all")
    @ApiOperation(value = "Get tasks matching a specific TaskStatus.")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "status", value = "TaskStatus as string.", dataType = "string", paramType = "query"),
        @ApiImplicitParam(name = "className", value = "Class name of BackgroundTask Object.", dataType = "string", paramType = "query"),
        @ApiImplicitParam(name = "creator", value = "Who instantiated these tasks.", dataType = "string", paramType = "query"),
        @ApiImplicitParam(name = "limit", value = "Limit the number of entries in the returned result.", dataType = "integer", paramType = "query"),
        @ApiImplicitParam(name = "offset", value = "Use in conjunction with limit for pagination.", dataType = "integer", paramType = "query")
    })
    private JSONArray getTasks(Request request, Response response) {
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

        JSONArray result = new JSONArray();
        for (TaskState state : manager.storage().getTasks(status, className, creator, null, limit, offset)) {
            result.put(serialiseStateSubset(state));
        }

        response.type("application/json");
        return result;
    }

    @GET
    @Path("/:uuid")
    @ApiOperation(value = "Get the state of a specific task by its ID.", produces = "application/json")
    @ApiImplicitParam(name = "uuid", value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private String getTask(Request request, Response response) {
        String id = request.params(ID_PARAMETER);

        try {
            response.status(200);
            response.type("application/json");

            return serialiseStateFull(manager.storage().getState(TaskId.of(id))).toString();
        } catch (EngineStorageException e){
           throw new GraknEngineServerException(404, format("Could not find [%s] in task storage", id));
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @PUT
    @Path("/:uuid/stop")
    @ApiOperation(value = "Stop a running or paused task.")
    @ApiImplicitParam(name = "uuid", value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private String stopTask(Request request, Response response) {
        try {
            String id = request.params(ID_PARAMETER);
            manager.stopTask(TaskId.of(id), this.getClass().getName());
            return "";
        }
        catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @POST
    @Path("/schedule")
    @ApiOperation(value = "Schedule a task.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "className", value = "Class name of object implementing the BackgroundTask interface", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "createdBy", value = "String representing the user scheduling this task", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "runAt", value = "Time to run at as milliseconds since the UNIX epoch", required = true, dataType = "long", paramType = "query"),
            @ApiImplicitParam(name = "interval",value = "If set the task will be marked as recurring and the value will be the time in milliseconds between repeated executions of this task. Value should be as Long.",
                    dataType = "long", paramType = "query"),
            @ApiImplicitParam(name = "configuration", value = "JSON Object that will be given to the task as configuration.", dataType = "String", paramType = "body")
    })
    private String scheduleTask(Request request, Response response) {
        String className = request.queryParams(TASK_CLASS_NAME_PARAMETER);
        String createdBy = request.queryParams(TASK_CREATOR_PARAMETER);
        String runAt = request.queryParams(TASK_RUN_AT_PARAMETER);

        String intervalParam = request.queryParams(TASK_RUN_INTERVAL_PARAMETER);
        Optional<Duration> optionalInterval = Optional.ofNullable(intervalParam).map(Long::valueOf).map(Duration::ofMillis);

        if(className == null || createdBy == null || runAt == null) {
            throw new GraknEngineServerException(400, "Missing mandatory parameters");
        }

        try {
            Class<? extends BackgroundTask> clazz = (Class<? extends BackgroundTask>) Class.forName(className);

            Instant time = ofEpochMilli(parseLong(runAt));

            TaskSchedule schedule = optionalInterval
                    .map(interval -> recurring(time, interval))
                    .orElse(TaskSchedule.at(time));

            Json configuration = request.body().isEmpty() ? Json.object() : Json.read(request.body());

            TaskState taskState = TaskState.of(clazz, createdBy, schedule, configuration);

            manager.addTask(taskState);

            response.type("application/json");
            return Json.object("id", taskState.getId().getValue()).toString();
        } catch (Exception e) {
            LOG.error(getFullStackTrace(e));
            throw new GraknEngineServerException(500, e);
        }
    }

    // TODO: Return 'schedule' object as its own object
    private JSONObject serialiseStateSubset(TaskState state) {
        return new JSONObject().put("id", state.getId().getValue())
                .put("status", state.status())
                .put("creator", state.creator())
                .put("className", state.taskClass().getName())
                .put("runAt", state.schedule().runAt())
                .put("recurring", state.schedule().isRecurring());
    }

    private JSONObject serialiseStateFull(TaskState state) {
        return serialiseStateSubset(state)
                .put("interval", state.schedule().interval().orElse(Duration.ZERO).toMillis())
                       .put("exception", state.exception())
                       .put("stackTrace", state.stackTrace())
                       .put("engineID", state.engineID())
                       .put("configuration", state.configuration());
    }
}

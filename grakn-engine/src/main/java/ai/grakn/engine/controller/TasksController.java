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

import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.TaskStatus;
import ai.grakn.exception.EngineStorageException;
import ai.grakn.exception.GraknEngineServerException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.OFFSET_PARAM;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;

import static ai.grakn.util.REST.WebPath.Tasks.TASKS;
import static ai.grakn.util.REST.WebPath.Tasks.GET;
import static ai.grakn.util.REST.WebPath.Tasks.PAUSE;
import static ai.grakn.util.REST.WebPath.Tasks.RESUME;
import static ai.grakn.util.REST.WebPath.Tasks.STOP;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import static spark.Spark.exception;
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

        get(TASKS,       this::getTasks);
        get(GET,         this::getTask);
        put(STOP,        this::stopTask);
        put(PAUSE,       this::pauseTask);
        put(RESUME,      this::resumeTask);
        post(TASKS,      this::scheduleTask);

        exception(EngineStorageException.class,     this::handleNotFoundInStorage);
        exception(Exception.class,                  this::handleInternalError);
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
                .getTasks(status, className, creator, limit, offset).stream()
                .map(this::serialiseStateFull)
                .forEach(result::add);

        response.status(200);
        response.type("application/json");

        return result;
    }

    @GET
    @Path("/:id")
    @ApiOperation(value = "Get the state of a specific task by its ID.", produces = "application/json")
    @ApiImplicitParam(name = "uuid", value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private String getTask(Request request, Response response) {
        String id = request.params(ID_PARAMETER);

        response.status(200);
        response.type("application/json");

        return serialiseStateFull(manager.storage().getState(id)).toString();
    }

    @PUT
    @Path("/:id/stop")
    @ApiOperation(value = "Stop a running or paused task.")
    @ApiImplicitParam(name = "id", value = "Identifier of the task to stop.", required = true, dataType = "string", paramType = "path")
    private String stopTask(Request request, Response response) {
        manager.stopTask(request.params(ID_PARAMETER), this.getClass().getName());
        return Json.object().toString();
    }

    @PUT
    @Path("/:id/pause")
    @ApiOperation(value = "Pause a running task.")
    @ApiImplicitParam(name = "id", value = "Identifier of the task to pause.", required = true, dataType = "string", paramType = "path")
    private String pauseTask(Request request, Response response) {
        manager.pauseTask(request.params(ID_PARAMETER), this.getClass().getName());
        return Json.object().toString();
    }

    @PUT
    @Path("/:id/resume")
    @ApiOperation(value = "Resume a paused task.")
    @ApiImplicitParam(name = "id", value = "Identifier of the task to resume.", required = true, dataType = "string", paramType = "path")
    private String resumeTask(Request request, Response response) {
        manager.resumeTask(request.params(ID_PARAMETER), this.getClass().getName());
        return Json.object().toString();
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
    private String scheduleTask(Request request, Response response) {
        String className = request.queryParams(TASK_CLASS_NAME_PARAMETER);
        String createdBy = request.queryParams(TASK_CREATOR_PARAMETER);
        String runAt = request.queryParams(TASK_RUN_AT_PARAMETER);

        Long interval = 0L;
        Json configuration = Json.object();

        if(request.queryParams(TASK_RUN_INTERVAL_PARAMETER) != null) {
            interval = Long.valueOf(request.queryParams(TASK_RUN_INTERVAL_PARAMETER));
        }
        if(className == null || createdBy == null || runAt == null) {
            throw new GraknEngineServerException(400, "Missing mandatory parameters");
        }

        if(request.body() != null && (!request.body().isEmpty())) {
            configuration = Json.read(request.body());
        }

        String id = manager.createTask(className, createdBy, ofEpochMilli(parseLong(runAt)), interval, configuration);

        response.status(200);
        response.type("application/json");

        return Json.object("id", id).toString();
    }

    /**
     * Error accessing or retrieving a task from storage. This throws a 404 Task Not Found to the user.
     * @param exception EngineStorageException thrown by the server
     * @param request The request object providing information about the HTTP request
     * @param response The response object providing functionality for modifying the response
     */
    private void handleNotFoundInStorage(Exception exception, Request request, Response response){
        LOG.trace(getFullStackTrace(exception));
        
        response.status(404);
        throw new GraknEngineServerException(404, format("Could not find [%s] in task storage", request.params(ID_PARAMETER)));
    }

    /**
     * Handle any exception thrown by the server
     * @param exception Exception by the server
     * @param request The request object providing information about the HTTP request
     * @param response The response object providing functionality for modifying the response
     */
    private void handleInternalError(Exception exception, Request request, Response response){
        LOG.error(request.ip() + getFullStackTrace(exception));

        response.status(500);
        throw new GraknEngineServerException(500, exception);
    }

    private Json serialiseStateSubset(TaskState state) {
        return Json.object()
                .set("id", state.getId())
                .set("status", state.status().name())
                .set("creator", state.creator())
                .set("className", state.taskClassName())
                .set("runAt", state.runAt().toString())
                .set("recurring", state.isRecurring());
    }

    private Json serialiseStateFull(TaskState state) {
        return serialiseStateSubset(state)
                .set("interval", state.interval())
                .set("exception", state.exception())
                .set("stackTrace", state.stackTrace())
                .set("engineID", state.engineID())
                .set("configuration", state.configuration());
    }
}

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

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.backgroundtasks.InMemoryTaskManager;
import ai.grakn.engine.backgroundtasks.StateStorage;
import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.backgroundtasks.TaskState;
import ai.grakn.engine.backgroundtasks.TaskStatus;
import ai.grakn.exception.GraknEngineServerException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import javafx.util.Pair;
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
import java.util.Date;

import static spark.Spark.get;
import static spark.Spark.put;
import static spark.Spark.post;

import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STOP;
import static ai.grakn.util.REST.Request.OFFSET_PARAM;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.WebPath.ALL_TASKS_URI;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;

@Path("/tasks")
@Api(value = "/tasks", description = "Endpoints used to query and control queued background tasks.", produces = "application/json")
public class TasksController {
    private final Logger LOG = LoggerFactory.getLogger(TasksController.class);
    private TaskManager taskManager;
    private StateStorage stateStorage;

    public TasksController() {
        taskManager = InMemoryTaskManager.getInstance();
        stateStorage = taskManager.storage();

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

        if(request.queryParams(LIMIT_PARAM) != null)
            limit = Integer.valueOf(request.queryParams(LIMIT_PARAM));

        if(request.queryParams(OFFSET_PARAM) != null)
            offset = Integer.valueOf(request.queryParams(OFFSET_PARAM));

        if(request.queryParams(TASK_STATUS_PARAMETER) != null) {
            status = TaskStatus.valueOf(request.queryParams(TASK_STATUS_PARAMETER));
        }

        JSONArray result = new JSONArray();
        for (Pair<String, TaskState> pair : stateStorage.getTasks(status, className, creator, limit, offset)) {
            result.put(serialiseStateSubset(pair.getKey(), pair.getValue()));
        }

        response.type("application/json");
        return result;
    }

    @GET
    @Path("/:uuid")
    @ApiOperation(value = "Get the state of a specific task by its ID.", produces = "application/json")
    @ApiImplicitParam(name = "uuid", value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private String getTask(Request request, Response response) {
        try {
            String id = request.params(ID_PARAMETER);
            JSONObject result = serialiseStateFull(id, stateStorage.getState(id));
            response.type("application/json");

            System.out.println("get one response: "+result.toString());
            return result.toString();
        } catch(Exception e) {
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
            taskManager.stopTask(id, this.getClass().getName());
            return "";
        } catch (Exception e) {
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

        Long interval = 0L;
        JSONObject configuration = new JSONObject();

        if(request.queryParams(TASK_RUN_INTERVAL_PARAMETER) != null)
            interval = Long.valueOf(request.queryParams(TASK_RUN_INTERVAL_PARAMETER));
        if(request.body() != null)
            configuration = new JSONObject(request.body());

        if(className == null || createdBy == null || runAt == null)
            throw new GraknEngineServerException(400, "Missing mandatory parameters");

        try {
            Date runAtDate = new Date(Long.valueOf(runAt));

            Class<?> clazz = Class.forName(className);
            BackgroundTask task = (BackgroundTask)clazz.newInstance();

            String id = taskManager.scheduleTask(task, createdBy, runAtDate, interval, configuration);
            JSONObject resp = new JSONObject()
                    .put("id", id);

            response.type("application/json");
            return resp.toString();

        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new GraknEngineServerException(400, e);
        }
        catch (NullPointerException e) {
            throw new GraknEngineServerException(400, "Missing mandatory parameters");
        }
        catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }


    private JSONObject serialiseStateSubset(String id, TaskState state) {
        return new JSONObject().put("id", id)
                .put("status", state.status())
                .put("creator", state.creator())
                .put("className", state.taskClassName())
                .put("runAt", state.runAt())
                .put("recurring", state.isRecurring());
    }

    private JSONObject serialiseStateFull(String id, TaskState state) {
        return serialiseStateSubset(id, state)
                       .put("interval", state.interval())
                       .put("exception", state.exception())
                       .put("stackTrace", state.stackTrace())
                       .put("executingHostname", state.executingHostname())
                       .put("configuration", state.configuration());
    }
}

/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.controller;

import io.mindmaps.engine.backgroundtasks.InMemoryTaskManager;
import io.mindmaps.engine.backgroundtasks.TaskManager;
import io.mindmaps.engine.backgroundtasks.TaskStatus;
import io.mindmaps.exception.MindmapsEngineServerException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.util.UUID;

import static io.mindmaps.util.REST.Request.*;
import static io.mindmaps.util.REST.WebPath.ALL_BACKGROUND_TASKS_URI;
import static io.mindmaps.util.REST.WebPath.BACKGROUND_TASKS_BY_STATUS;
import static io.mindmaps.util.REST.WebPath.BACKGROUND_TASK_STATUS;
import static spark.Spark.get;
import static spark.Spark.put;

@Path("/backgroundtasks")
@Api(value = "/backgroundtasks", description = "Endpoints used to query and control queued background tasks.", produces = "application/json")
public class BackgroundTasksController {
    private final Logger LOG = LoggerFactory.getLogger(BackgroundTasksController.class);
    private TaskManager taskManager;

    public BackgroundTasksController() {
        taskManager = InMemoryTaskManager.getInstance();

        get(ALL_BACKGROUND_TASKS_URI, this::getAllTasks);
        get(BACKGROUND_TASKS_BY_STATUS + TASK_STATUS_PARAMETER, this::getTasks);
        get(BACKGROUND_TASK_STATUS + UUID_PARAMETER, this::getTask);
        put(BACKGROUND_TASK_STATUS + UUID_PARAMETER + TASK_PAUSE, this::pauseTask);
        put(BACKGROUND_TASK_STATUS + UUID_PARAMETER + TASK_RESUME, this::resumeTask);
        put(BACKGROUND_TASK_STATUS + UUID_PARAMETER + TASK_STOP, this::stopTask);
        put(BACKGROUND_TASK_STATUS + UUID_PARAMETER + TASK_RESTART, this::restartTask);
    }

    @GET
    @Path("/all")
    @ApiOperation(value = "Return all known tasks.")
    private String getAllTasks(Request request, Response response) {
        JSONObject result = new JSONObject();
        taskManager.getAllTasks().forEach(x -> result.put(x.toString(), taskManager.getTaskState(x).getStatus()));
        response.type("application/json");
        return result.toString();
    }

    @GET
    @Path("/tasks/:status")
    @ApiOperation(value = "Get tasks matching a specific TaskStatus.")
    @ApiImplicitParam(name = "status", value = "TaskStatus as string", required = true, dataType = "string", paramType = "path")
    private JSONArray getTasks(Request request, Response response) {
        JSONArray result = new JSONArray();
        try {
            String status = request.params(TASK_STATUS_PARAMETER);
            TaskStatus taskStatus = TaskStatus.valueOf(status);

            taskManager.getTasks(taskStatus).forEach(result::put);
            response.type("application/json");
            return result;
        } catch(Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    @GET
    @Path("/task/:uuid")
    @ApiOperation(value = "Get the state of a specific task by its UUID.", produces = "application/json")
    @ApiImplicitParam(name = "uuid", value = "UUID of task.", required = true, dataType = "string", paramType = "path")
    private String getTask(Request request, Response response) {
        try {
            UUID uuid = UUID.fromString(request.params(UUID_PARAMETER));
            JSONObject result = new JSONObject()
                    .put("status", taskManager.getTaskState(uuid).getStatus());

            response.type("application/json");
            return result.toString();
        } catch(Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    @PUT
    @Path("/task/:uuid/pause")
    @ApiOperation(value = "Pause a running task.")
    @ApiImplicitParam(name = "uuid", value = "UUID of task.", required = true, dataType = "string", paramType = "path")
    private String pauseTask(Request request, Response response) {
        try {
            UUID uuid = UUID.fromString(request.params(UUID_PARAMETER));
            taskManager.pauseTask(uuid, this.getClass().getName(), null);
            return "";
        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    @PUT
    @Path("/task/:uuid/resume")
    @ApiOperation(value = "Resume a paused task.")
    @ApiImplicitParam(name = "uuid", value = "UUID of task.", required = true, dataType = "string", paramType = "path")
    private String resumeTask(Request request, Response response) {
        try {
            UUID uuid = UUID.fromString(request.params(UUID_PARAMETER));
            taskManager.resumeTask(uuid, this.getClass().getName(), null);
            return "";
        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    @PUT
    @Path("/task/:uuid/stop")
    @ApiOperation(value = "Stop a running or paused task.")
    @ApiImplicitParam(name = "uuid", value = "UUID of task.", required = true, dataType = "string", paramType = "path")
    private String stopTask(Request request, Response response) {
        try {
            UUID uuid = UUID.fromString(request.params(UUID_PARAMETER));
            taskManager.stopTask(uuid, this.getClass().getName(), null);
            return "";
        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    @PUT
    @Path("/task/:uuid/restart")
    @ApiOperation(value = "Restart a stopped or paused task.")
    @ApiImplicitParam(name = "uuid", value = "UUID of task.", required = true, dataType = "string", paramType = "path")
    private String restartTask(Request request, Response response) {
        try {
            UUID uuid = UUID.fromString(request.params(UUID_PARAMETER));
            taskManager.restartTask(uuid, this.getClass().getName(), null);
            return "";
        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }
}

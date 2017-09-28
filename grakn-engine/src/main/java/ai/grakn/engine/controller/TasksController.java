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


import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.util.ConcurrencyUtil;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static ai.grakn.engine.TaskStatus.FAILED;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.engine.tasks.manager.TaskSchedule.recurring;
import static ai.grakn.util.REST.Request.TASK_RUN_WAIT_PARAMETER;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.EXCEPTION;
import static ai.grakn.util.REST.Response.Task.ID;
import static ai.grakn.util.REST.Response.Task.STACK_TRACE;
import static ai.grakn.util.REST.Response.Task.STATUS;
import static ai.grakn.util.REST.WebPath.Tasks.GET;
import static ai.grakn.util.REST.WebPath.Tasks.STOP;
import static ai.grakn.util.REST.WebPath.Tasks.TASKS;
import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.Long.parseLong;
import static java.time.Instant.ofEpochMilli;
import static java.util.stream.Collectors.toList;

/**
 * <p>
 * Endpoints used to query and control queued background tasks.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
@Path("/tasks")
@Api(value = "/tasks", description = "Endpoints used to query and control queued background tasks.", produces = "application/json")
public class TasksController {

    private static final Logger LOG = LoggerFactory.getLogger(TasksController.class);
    private static final TaskState.Priority DEFAULT_TASK_PRIORITY = TaskState.Priority.LOW;

    private static final int MAX_THREADS = 10;
    private static final Duration MAX_EXECUTION_TIME = Duration.ofSeconds(30);

    private final TaskManager manager;
    private final ExecutorService executor;
    private final Timer createTasksTimer;
    private final Timer stopTaskTimer;
    private final Timer getTaskTimer;
    private final Timer getTasksTimer;

    public TasksController(Service spark, TaskManager manager, MetricRegistry metricRegistry) {
        this(spark, manager, metricRegistry, taskExecutor());
    }

    public TasksController(Service spark, TaskManager manager, MetricRegistry metricRegistry, ExecutorService executor) {
        if (manager==null) {
            throw GraknServerException.internalError("Task manager has not been instantiated.");
        }
        this.manager = manager;

        this.getTasksTimer = metricRegistry.timer(name(TasksController.class, "get-tasks"));
        this.getTaskTimer = metricRegistry.timer(name(TasksController.class, "get-task"));
        this.stopTaskTimer = metricRegistry.timer(name(TasksController.class, "stop-task"));
        this.createTasksTimer = metricRegistry.timer(name(TasksController.class, "create-tasks"));

        spark.get(TASKS, this::getTasks);
        spark.get(GET, this::getTask);
        spark.put(STOP, this::stopTask);
        spark.post(TASKS, this::createTasks);

        spark.exception(GraknServerException.class, (e, req, res) -> handleNotFoundInStorage(e, res));
        spark.exception(GraknBackendException.class, (e, req, res) -> handleNotFoundInStorage(e, res));
        this.executor = executor;
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Get tasks matching a specific TaskStatus.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = REST.Request.TASK_STATUS_PARAMETER, value = "TaskStatus as string.", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = REST.Request.TASK_CLASS_NAME_PARAMETER, value = "Class name of BackgroundTask Object.", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = REST.Request.TASK_CREATOR_PARAMETER, value = "Who instantiated these tasks.", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = REST.Request.LIMIT_PARAM, value = "Limit the number of entries in the returned result.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = REST.Request.OFFSET_PARAM, value = "Use in conjunction with limit for pagination.", dataType = "integer", paramType = "query")
    })
    private Json getTasks(Request request, Response response) {
        TaskStatus status = null;
        String className = request.queryParams(REST.Request.TASK_CLASS_NAME_PARAMETER);
        String creator = request.queryParams(REST.Request.TASK_CREATOR_PARAMETER);
        int limit = 0;
        int offset = 0;

        if (request.queryParams(REST.Request.LIMIT_PARAM) != null) {
            limit = Integer.parseInt(request.queryParams(REST.Request.LIMIT_PARAM));
        }

        if (request.queryParams(REST.Request.OFFSET_PARAM) != null) {
            offset = Integer.parseInt(request.queryParams(REST.Request.OFFSET_PARAM));
        }

        if (request.queryParams(REST.Request.TASK_STATUS_PARAMETER) != null) {
            status = TaskStatus.valueOf(request.queryParams(REST.Request.TASK_STATUS_PARAMETER));
        }

        Context context = getTasksTimer.time();
        try {
            Json result = Json.array();
            manager.storage()
                    .getTasks(status, className, creator, null, limit, offset).stream()
                    .map(this::serialiseStateSubset)
                    .forEach(result::add);

            response.status(HttpStatus.SC_OK);
            response.type(APPLICATION_JSON);
            return result;
        } finally {
            context.stop();
        }
    }

    @GET
    @Path("/{id}")
    @ApiOperation(value = "Get the state of a specific task by its ID.", produces = "application/json")
    @ApiImplicitParam(name = REST.Request.UUID_PARAMETER, value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private Json getTask(Request request, Response response) {
        String id = request.params("id");
        Context context = getTaskTimer.time();
        try {
            response.status(200);
            response.type(APPLICATION_JSON);
            return serialiseStateSubset(manager.storage().getState(TaskId.of(id)));
        } finally {
            context.stop();
        }
    }

    @PUT
    @Path("/{id}/stop")
    @ApiOperation(value = "Stop a running or paused task.")
    @ApiImplicitParam(name = REST.Request.UUID_PARAMETER, value = "ID of task.", required = true, dataType = "string", paramType = "path")
    private Json stopTask(Request request, Response response) {
        String id = request.params(REST.Request.ID_PARAMETER);
        try (Context context = stopTaskTimer.time()) {
            manager.stopTask(TaskId.of(id));
            response.status(HttpStatus.SC_OK);
            response.type(APPLICATION_JSON);
            return Json.object();
        }
    }

    @POST
    @Path("/")
    @ApiOperation(value = "Schedule a set of tasks.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = REST.Request.TASKS_PARAM, value = "JSON Array containing an ordered list of task parameters and comfigurations.", required = true, dataType = "List", paramType = "body")
    })
    private Json createTasks(Request request, Response response) {

        Json requestBodyAsJson = bodyAsJson(request);
        // This covers the previous behaviour. It looks like a quirk of the testing
        // client library we are using. Consider deprecating it.
        if (requestBodyAsJson.has("value")) {
            requestBodyAsJson = requestBodyAsJson.at("value");
        }
        boolean wait = true;
        if (requestBodyAsJson.has(TASK_RUN_WAIT_PARAMETER)) {
            wait = requestBodyAsJson.at(TASK_RUN_WAIT_PARAMETER).asBoolean();
        }
        if (!requestBodyAsJson.has(REST.Request.TASKS_PARAM)) {
            LOG.error("Malformed request body: {}", requestBodyAsJson);
            throw GraknServerException.requestMissingBodyParameters(REST.Request.TASKS_PARAM);
        }
        LOG.debug("Received request {}", request);
        List<Json> taskJsonList = requestBodyAsJson.at(REST.Request.TASKS_PARAM).asJsonList();
        Json responseJson = Json.array();
        response.type(ContentType.APPLICATION_JSON.getMimeType());
        // We need to return the list of taskStates in order
        // so the client can relate the state to each element in the request.
        final Timer.Context context = createTasksTimer.time();
        try {
            List<TaskStateWithConfiguration> taskStates = parseTasks(taskJsonList);
            CompletableFuture<List<Json>> completableFuture = executeTasks(taskStates, wait);
            try {
                return buildResponseForTasks(response, responseJson, completableFuture);
            } catch (TimeoutException | InterruptedException e) {
                LOG.error("Task interrupted", e);
                response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                return Json.object();
            } catch (Exception e) {
                LOG.error("Exception while processing batch of tasks", e);
                response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                return Json.object();
            }
        } finally {
            context.stop();
        }
    }

    private Json buildResponseForTasks(Response response, Json responseJson,
            CompletableFuture<List<Json>> completableFuture)
            throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
        List<Json> results = completableFuture
                .get(MAX_EXECUTION_TIME.getSeconds(), TimeUnit.SECONDS);
        boolean hasFailures = false;
        for (Json resultForTask : results) {
            responseJson.add(resultForTask);
            if (resultForTask.at("code").asInteger() != HttpStatus.SC_OK) {
                LOG.error("Could not add task {}", resultForTask);
                hasFailures = true;
            }
        }
        if (!hasFailures) {
            response.status(HttpStatus.SC_OK);
        } else if (responseJson.asJsonList().size() > 0) {
            response.status(HttpStatus.SC_ACCEPTED);
        } else {
            response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
        return responseJson;
    }

    private List<TaskStateWithConfiguration> parseTasks(List<Json> taskJsonList) {
        List<TaskStateWithConfiguration> taskStates = new ArrayList<>();
        for (int i = 0; i < taskJsonList.size(); i++) {
            Json singleTaskJson = taskJsonList.get(i);
            try {
                taskStates.add(new TaskStateWithConfiguration(
                        extractParametersAndProcessTask(singleTaskJson),
                        extractConfiguration(singleTaskJson), i));
            } catch (Exception e) {
                LOG.error("Malformed request at {}", singleTaskJson, e);
                // We return a failure for the full request as this imply there is
                // something wrong in the client logic that needs to be addressed
                throw e;
            }
        }
        return taskStates;
    }

    private CompletableFuture<List<Json>> executeTasks(
            List<TaskStateWithConfiguration> taskStates, boolean wait) {
        // Put the tasks in a persistent queue
        List<CompletableFuture<Json>> futures = taskStates.stream()
                .map(taskStateWithConfiguration -> CompletableFuture
                        .supplyAsync(() -> addTaskToManager(taskStateWithConfiguration, wait), executor))
                .collect(toList());
        return ConcurrencyUtil.all(futures);
    }

    private Json extractConfiguration(Json taskJson) {
        if (taskJson.has(REST.Request.CONFIGURATION_PARAM)) {
            Json config = taskJson.at(REST.Request.CONFIGURATION_PARAM);
            if (config.isNull()) {
                return Json.nil();
            }
            if (!config.isObject()) {
                throw GraknServerException.requestMissingParameters(REST.Request.CONFIGURATION_PARAM);
            } else {
                return config;
            }
        } else {
            return Json.object();
        }
    }

    // TODO: move away from JSON and create API class
    private Json addTaskToManager(TaskStateWithConfiguration taskState, boolean wait) {
        Json singleTaskReturnJson = Json.object().set("index", taskState.getIndex());
        try {
            TaskState state = taskState.getTaskState();
            TaskId id = state.getId();
            singleTaskReturnJson.set("code", HttpStatus.SC_OK);
            if (wait) {
                LOG.debug("Running task {}", state.getId());
                manager.runTask(state, TaskConfiguration.of(taskState.getConfiguration()));
                TaskState stateAfterRun = manager.storage().getState(id);
                if (stateAfterRun != null && stateAfterRun.status().equals(FAILED)) {
                    singleTaskReturnJson.set("code", HttpStatus.SC_BAD_REQUEST);
                    singleTaskReturnJson.set(EXCEPTION, stateAfterRun.exception());
                    singleTaskReturnJson.set(STACK_TRACE, stateAfterRun.stackTrace());
                }
                // TODO handle the case where the task returned is null
            } else {
                LOG.debug("Adding to queue task {}", state.getId());
                manager.addTask(state, TaskConfiguration.of(taskState.getConfiguration()));
                singleTaskReturnJson.set("code", HttpStatus.SC_OK);
            }
            singleTaskReturnJson.set("id", id.getValue());
        } catch (Exception e) {
            LOG.error("Server error while adding the task", e);
            singleTaskReturnJson.set("code", HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
        return singleTaskReturnJson;
    }

    private TaskState extractParametersAndProcessTask(Json singleTaskJson) {
        Function<String, Optional<Json>> extractor = p -> Optional
                .ofNullable(singleTaskJson.at(p));
        String className = mandatoryQueryParameter(extractor,
                REST.Request.TASK_CLASS_NAME_PARAMETER).asString();
        String createdBy = mandatoryQueryParameter(extractor,
                REST.Request.TASK_CREATOR_PARAMETER).asString();
        String runAtTime = mandatoryQueryParameter(extractor,
                REST.Request.TASK_RUN_AT_PARAMETER).asString();
        String intervalParam = extractor.apply(REST.Request.TASK_RUN_INTERVAL_PARAMETER)
                .map(Json::asString).orElse(null);
        String priorityParam = extractor.apply(REST.Request.TASK_PRIORITY_PARAMETER)
                .map(Json::asString).orElse(null);
        return processTask(className, createdBy, runAtTime, intervalParam, priorityParam);
    }

    private TaskState processTask(String className, String createdBy, String runAtTime,
            String intervalParam, String priorityParam) {
        TaskSchedule schedule;
        TaskState.Priority priority;
        try {
            // Get the schedule of the task
            Optional<Duration> optionalInterval = Optional.ofNullable(intervalParam)
                    .map(Long::valueOf).map(Duration::ofMillis);
            Instant time = ofEpochMilli(parseLong(runAtTime));
            schedule = optionalInterval
                    .map(interval -> recurring(time, interval))
                    .orElse(TaskSchedule.at(time));

            // Get the priority of a task (default is low)
            priority = Optional.ofNullable(priorityParam).map(TaskState.Priority::valueOf)
                    .orElse(DEFAULT_TASK_PRIORITY);
        } catch (Exception e) {
            throw GraknServerException.serverException(400, e);
        }

        // Get the class of this background task
        Class<?> clazz = getClass(className);
        return TaskState.of(clazz, createdBy, schedule, priority);
    }

    private Json bodyAsJson(Request request) {
        String requestBody = request.body();
        if (requestBody.isEmpty()) {
            return Json.object();
        }
        try {
            return Json.read(requestBody);
        } catch (Exception e) {
            LOG.error("Malformed json in body of request {}", requestBody, e);
            throw GraknServerException.serverException(400, e);
        }
    }

    /**
     * Use reflection to get a reference to the given class. Returns a 500 to the client if the
     * class is unavailable.
     *
     * @param className class to retrieve
     * @return reference to the given class
     */
    private Class<?> getClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            if (!BackgroundTask.class.isAssignableFrom(clazz)) {
                throw GraknServerException.invalidTask(className);
            }

            return clazz;
        } catch (ClassNotFoundException e) {

            throw GraknServerException.invalidTask(className);

        }
    }

    /**
     * Error accessing or retrieving a task from storage. This throws a 404 Task Not Found to the user.
     * @param exception {@link GraknServerException} thrown by the server
     * @param response The response object providing functionality for modifying the response
     */
    private void handleNotFoundInStorage(Exception exception, Response response){
        //TODO: Fix this. This is needed because of a mixture of exceptions being thrown within the context of this controller
        if(exception instanceof GraknServerException){
            response.status(((GraknServerException) exception).getStatus());
            response.body(Json.object("exception", exception.getMessage()).toString());
        } else {
            response.status(404);
        }
    }

    // TODO: Return 'schedule' object as its own object
    private Json serialiseStateSubset(TaskState state) {
        return Json.object()
                .set(ID, state.getId().getValue())
                .set(STATUS, state.status().name())
                .set(EXCEPTION, state.exception());
    }

    private static class TaskStateWithConfiguration {

        private final TaskState taskState;
        private Json configuration;
        private final int index;

        TaskStateWithConfiguration(TaskState taskState, Json configuration, int index) {
            this.taskState = taskState;
            this.configuration = configuration;
            this.index = index;
        }

        public TaskState getTaskState() {
            return taskState;
        }

        public int getIndex() {
            return index;
        }

        public Json getConfiguration() {
            return configuration;
        }
    }

    public static ExecutorService taskExecutor() {
        return Executors.newFixedThreadPool(MAX_THREADS, new ThreadFactoryBuilder()
                .setNameFormat("grakn-task-controller-%d").build());
    }
}

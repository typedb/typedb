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
import ai.grakn.exception.GraknBackendException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import mjson.Json;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;

import static ai.grakn.util.REST.Request.CONFIGURATION_PARAM;
import static ai.grakn.util.REST.Request.LIMIT_PARAM;
import static ai.grakn.util.REST.Request.TASKS_PARAM;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_WAIT_PARAMETER;
import static ai.grakn.util.REST.Response.Task.EXCEPTION;
import static ai.grakn.util.REST.Response.Task.STACK_TRACE;
import static ai.grakn.util.REST.WebPath.Tasks.GET;
import static ai.grakn.util.REST.WebPath.Tasks.STOP;
import static ai.grakn.util.REST.WebPath.Tasks.TASKS;
import static java.lang.String.format;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHost.DEFAULT_SCHEME_NAME;
import static org.apache.http.HttpStatus.SC_ACCEPTED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * Client for interacting with tasks on engine
 *
 * @author Felix Chapman, alexandraorth
 */
public class TaskClient extends Client {

    private final Logger LOG = LoggerFactory.getLogger(TaskClient.class);

    private final HttpClient httpClient = HttpClients.createDefault();
    private final String host;
    private final int port;

    private TaskClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static TaskClient of(String host, int port) {
        return new TaskClient(host, port);
    }

    /**
     * Submit a task to run on an Grakn Engine server
     *
     * @param taskClass Class of the Task to run
     * @param creator Class creating the task
     * @param runAt Time at which the task should be executed
     * @param interval Interval at which the task should recur, can be null
     * @param configuration Data on which to execute the task
     * @return Identifier of the submitted task that will be executed on a server
     */
    public TaskResult sendTask(Class<?> taskClass, String creator, Instant runAt, Duration interval,
            Json configuration, boolean wait) {
        return sendTask(taskClass.getName(), creator, runAt, interval, configuration, -1, wait);
    }

    TaskResult sendTask(String taskClass, String creator, Instant runAt, Duration interval,
            Json configuration, long limit, boolean wait) {
        try {
            URIBuilder uri = new URIBuilder(TASKS)
                    .setScheme(DEFAULT_SCHEME_NAME)
                    .setHost(host)
                    .setPort(port);

            Builder<String, String> taskBuilder = ImmutableMap.builder();
            taskBuilder.put(TASK_CLASS_NAME_PARAMETER, taskClass);
            taskBuilder.put(TASK_CREATOR_PARAMETER, creator);

            taskBuilder.put(TASK_RUN_AT_PARAMETER, Long.toString(runAt.toEpochMilli()));

            if (limit > -1) {
                taskBuilder.put(LIMIT_PARAM, Long.toString(limit));
            }

            if (interval != null) {
                taskBuilder.put(TASK_RUN_INTERVAL_PARAMETER, Long.toString(interval.toMillis()));
            }

            Json jsonTask = Json.make(taskBuilder.build());
            jsonTask.set(CONFIGURATION_PARAM, configuration);

            HttpPost httpPost = new HttpPost(uri.build());
            httpPost.setHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType());
            // This is a special case of sending a list of task
            // TODO update the client to support a list
            httpPost.setEntity(new StringEntity(
                    Json.object()
                            .set(TASK_RUN_WAIT_PARAMETER, wait)
                            .set(TASKS_PARAM, Json.array().add(jsonTask))
                            .toString()));

            HttpResponse response = httpClient.execute(httpPost);

            assertOk(response);

            Json jsonResponse = asJsonHandler.handleResponse(response);

            Json responseElement = jsonResponse.at(0);
            return TaskResult.builder()
                    .setTaskId(TaskId.of(responseElement.at("id").asString()))
                    .setCode(responseElement.at("code").asString())
                    .setStackTrace(responseElement.has(STACK_TRACE) ? responseElement.at(STACK_TRACE).asString() : "")
                    .setStackTrace(responseElement.has(EXCEPTION) ? responseElement.at(EXCEPTION).asString() : "")
                    .build();
        } catch (IOException e) {
            throw GraknBackendException.engineUnavailable(host, port, e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the status of a given task on the server
     *
     * @param id Identifier of the task to get status of
     * @return Status of the specified task
     * @throws GraknBackendException When the specified task has not yet been stored by the server
     */
    public TaskStatus getStatus(TaskId id) {
        try {
            URI uri = new URIBuilder(convert(GET, id))
                    .setScheme(DEFAULT_SCHEME_NAME)
                    .setPort(port)
                    .setHost(host)
                    .build();

            HttpGet httpGet = new HttpGet(uri);
            HttpResponse response = httpClient.execute(httpGet);

            // 404 Not found returned when task not yet stored
            boolean notFound = response.getStatusLine().getStatusCode() == SC_NOT_FOUND;
            if (notFound) {
                throw GraknBackendException.stateStorage();
            }

            // 200 Only returned when request successfully completed
            assertOk(response);

            Json jsonResponse = asJsonHandler.handleResponse(response);
            return TaskStatus.valueOf(jsonResponse.at("status").asString());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw GraknBackendException.engineUnavailable(host, port, e);
        }
    }

    /**
     * Stop a task using the given ID.
     *
     * @param id the ID of the task to stop
     */
    public boolean stopTask(TaskId id) {
        try {
            URI uri = new URIBuilder(convert(STOP, id))
                    .setScheme(DEFAULT_SCHEME_NAME)
                    .setPort(port)
                    .setHost(host)
                    .build();

            HttpPut httpPut = new HttpPut(uri);

            HttpResponse response = httpClient.execute(httpPut);

            boolean isOk = isOk(response);

            if (!isOk) {
                LOG.error("Failed to stop task: " + asJsonHandler.handleResponse(response));
            }

            return isOk;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw GraknBackendException.engineUnavailable(host, port, e);
        }
    }

    private boolean isOk(HttpResponse response) {
        int statusCode = response.getStatusLine().getStatusCode();
        return statusCode == SC_OK || statusCode == SC_ACCEPTED;
    }

    private void assertOk(HttpResponse response) {
        // 200 Only returned when request successfully completed
        if (!isOk(response)) {
            throw new RuntimeException(format("Status %s returned from server",
                    response.getStatusLine().getStatusCode()));
        }
    }
}
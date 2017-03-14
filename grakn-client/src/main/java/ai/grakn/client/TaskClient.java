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
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import mjson.Json;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_INTERVAL_PARAMETER;
import static ai.grakn.util.REST.WebPath.TASKS_GET_URI;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;
import static ai.grakn.util.REST.WebPath.TASKS_STOP_URI;
import static java.lang.String.format;

/**
 * Client for interacting with tasks on engine
 *
 * @author Felix Chapman
 */
public class TaskClient extends Client {
    private final Logger LOG = LoggerFactory.getLogger(TaskClient.class);

    private final String uri;

    private TaskClient(String uri) {
        this.uri = uri;
    }
    
    public static TaskClient of(String uri) {
        return new TaskClient(uri);
    }

    public TaskId sendTask(Class<?> taskClass, String creator, Instant runAt, Duration interval, Json configuration){
        try {
            String idValue = Unirest.post(format("http://%s/%s", uri, TASKS_SCHEDULE_URI))
                    .queryString(TASK_CLASS_NAME_PARAMETER, taskClass.getName())
                    .queryString(TASK_CREATOR_PARAMETER, creator)
                    .queryString(TASK_RUN_AT_PARAMETER, runAt.toEpochMilli())
                    .queryString(TASK_RUN_INTERVAL_PARAMETER, interval.toMillis())
                    .body(configuration.toString())
                    .asJson().getBody().getObject().getString("id");

            return TaskId.of(idValue);
        } catch (UnirestException e){
            throw new RuntimeException(e);
        }
    }

    public TaskStatus getStatus(TaskId id){
        try {
            String statusValue = Unirest.get(format("http://%s/%s", uri, TASKS_GET_URI))
                    .routeParam("id", id.getValue())
                    .asJson().getBody().getObject().getString("status");

            return TaskStatus.valueOf(statusValue);
        } catch (UnirestException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Stop a task using the given ID.
     * @param id the ID of the task to stop
     * @return whether the task was successfully stopped
     */
    public boolean stopTask(TaskId id) {
        try {
            HttpResponse<String> response = Unirest.put(format("http://%s/%s", uri, TASKS_STOP_URI))
                    .routeParam("id", id.getValue())
                    .asString();

            boolean isOK = response.getStatus() == HttpStatus.SC_OK;

            if (!isOK) {
                LOG.error("Failed to stop task: " + response.getBody());
            }

            return isOK;
        } catch (UnirestException e) {
            throw new RuntimeException(e);
        }
    }
}

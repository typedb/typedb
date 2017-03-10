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
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import static ai.grakn.util.REST.WebPath.TASKS_STOP_URI;
import static java.lang.String.format;

/**
 * Client for interacting with tasks on engine
 *
 * @author Felix Chapman
 */
public class TaskClient extends Client {

    private final String uri;

    private TaskClient(String uri) {
        this.uri = uri;
    }
    
    public static TaskClient of(String uri) {
        return new TaskClient(uri);
    }

    /**
     * Stop a task using the given ID.
     * @param id the ID of the task to stop
     */
    public void stopTask(TaskId id) {
        try {
            Unirest.put(format("http://%s/%s", uri, TASKS_STOP_URI))
                    .routeParam("id", id.getValue())
                    .asBinary();
        } catch (UnirestException e) {
            throw new RuntimeException(e);
        }
    }
}

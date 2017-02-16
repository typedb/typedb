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

package ai.grakn.engine.tasks.manager.singlequeue;

import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskStateStorage;
import mjson.Json;

import java.time.Instant;

/**
 * TaskManager implementation that operates using a single Kafka queue
 *
 * This class controls the SingleQueueTaskRunner. Should this TaskRunner fail the TaskManager will attempt
 * to resurrect it.
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskManager implements TaskManager {

    /**
     * Create a SingleQueueTaskManager and instantiate any needed services
     *
     * The SingleQueueTaskManager implementation must:
     *  + Instantiate a connection to zookeeper
     *  + Configure and instance of TaskStateStorage
     *  + Create and run an instance of SingleQueueTaskRunner
     */
    public SingleQueueTaskManager(){

    }

    /**
     * Close this instance of the TaskManager. Any errors that occur should not prevent the
     * subsequent ones from executing.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {

    }

    /**
     * Create an instance of a task based on the given parameters and submit it a Kafka queue.
     *
     * @param taskClassName Name of the class implementing the BackgroundTask interface
     * @param createdBy Name of the class that created the task
     * @param runAt Instant when task should run.
     * @param period A non-zero value indicates that this should be a recurring task and period indicates the delay between
     *               subsequent runs of the task after successful execution.
     * @param configuration A JSONObject instance containing configuration and optionally data for the task. This is an
     *                      optional parameter and may be set to null to not pass any configuration (task.start() will
     *                      get an initialised but empty JSONObject).
     * @return String identifier of the created task
     */
    @Override
    public String createTask(String taskClassName, String createdBy, Instant runAt, long period, Json configuration) {
        return null;
    }

    /**
     * Stop a task from running.
     */
    @Override
    public TaskManager stopTask(String id, String requesterName) {
        return null;
    }

    /**
     * Access the storage that this instance of TaskManager uses.
     * @return A TaskStateStorage object
     */
    @Override
    public TaskStateStorage storage() {
        return null;
    }
}

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

package ai.grakn.engine.backgroundtasks;

import org.json.JSONObject;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

public interface TaskManager {
    /**
     * Schedule a single shot/one off BackgroundTask to run after a @delay in milliseconds. All parameters must not be
     * null unless stated otherwise.
     * @param task Any object implementing the BackgroundTask interface that is to be scheduled for later execution.
     * @param runAt Date when task should run.
     * @param period A non-zero value indicates that this should be a recurring task and period indicates the delay between
     *               subsequent runs of the task after successful execution.
     * @param configuration A JSONObject instance containing configuration and optionally data for the task. This is an
     *                      optional parameter and may be set to null to not pass any configuration (task.start() will
     *                      get an initialised but empty JSONObject).
     * @return Assigned ID of task scheduled for later execution.
     */
    String scheduleTask(BackgroundTask task, String createdBy, Date runAt, long period, JSONObject configuration);

    /**
     * Return a future that allows registering asynchronous callbacks triggered when a task is completed.
     * @param taskId ID of task to track
     * @return A CompletableFuture instance monitoring the status of the given task.
     */
    CompletableFuture completableFuture(String taskId);

    /**
     * Stop a Scheduled, Paused or Running task. Task's .stop() method will be called to perform any cleanup and the
     * task is killed afterwards.
     * @param id String of task to stop.
     * @param requesterName Optional String to denote who requested this call; used for status reporting and may be null.
     * @return Instance of the class implementing TaskManager.
     */
    TaskManager stopTask(String id, String requesterName);

    /**
     * Return the StateStorage instance that is used by this class.
     * @return A StateStorage instance.
     */
    StateStorage storage();
}

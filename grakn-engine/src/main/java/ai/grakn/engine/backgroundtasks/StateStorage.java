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

import javafx.util.Pair;
import org.json.JSONObject;

import java.util.Date;
import java.util.Set;

public interface StateStorage {
    /**
     * Create a new task state and store it, returning an ID to later access this task state.
     * @param taskName String class name of object implementing the BackgroundTask interface. This must not be null.
     * @param createdBy String of who is creating this new state. This must not be null.
     * @param runAt Date when should this task be executed. This must not be null.
     * @param recurring Boolean marking if this task should be run again after it has finished executing successfully.
     *                  This must not be null.
     * @param interval If a task is marked as recurring, this represents the time delay between the next executing of this task.
     *                 This must not be null.
     * @param configuration A JSONObject instance containing configuration and optionally data for the task. This is an
     *                      optional parameter and may be set to null to not pass any configuration (task.start() will
     *                      get an initialised but empty JSONObject).
     * @return String form of the task id, which can be use later to update or retrieve the task state. Null if task could
     * not be created of mandatory fields were omitted.
     */
    String newState(String taskName,
                    String createdBy,
                    Date runAt,
                    Boolean recurring,
                    long interval,
                    JSONObject configuration);

    /**
     * Used to update task state. With the exception of @id any other fields may individually be null, however all parameters
     * cannot be null at the same time. Setting any of the parameters to null indicates that their values should not be
     * changed.
     * @param id ID of task to update, this must not be null.
     * @param status New status of task, may be null.
     * @param statusChangeBy String identifying caller, may be null.
     * @param executingHostname String hostname of engine instance scheduling/executing this task. May be null.
     * @param failure Throwable to store any exceptions that occurred during executing. May be null.
     * @param checkpoint String to store task checkpoint, may be null.
     */
    void updateState(String id,
                     TaskStatus status,
                     String statusChangeBy,
                     String executingHostname,
                     Throwable failure,
                     String checkpoint,
                     JSONObject configuration);

    /**
     * This is a copy of the internal TaskState object. It is guaranteed to be correct at the time of call, however the actual
     * internal state may change at any time after.
     * @param id String id of task.
     * @return TaskState object or null if no TaskState with this id could be found.
     */
    TaskState getState(String id);

    /**
     * Return a Set of Pairs of tasks that match any of the criteria. The first value of the Pair is the task id, whilst
     * the second is the TaskState. Parameters may be set to null to not match against then (rather than to match null
     * values), if *all* parameters are null then all known tasks in the system are returned.
     *
     * @param taskStatus See TaskStatus enum.
     * @param taskClassName String containing task class name. See TaskState.
     * @param createdBy String containing created by. See TaskState.
     * @param limit Limit the returned result set to @limit amount of entries.
     * @param offset Use in conjunction with @limit for pagination.
     * @return Set<Pair<String, TaskState>> of task IDs and corresponding TaskState *copies*.
     */
    Set<Pair<String, TaskState>> getTasks(TaskStatus taskStatus,
                                          String taskClassName,
                                          String createdBy,
                                          int limit,
                                          int offset);
}

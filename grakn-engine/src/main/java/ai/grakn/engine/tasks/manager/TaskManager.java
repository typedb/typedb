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

package ai.grakn.engine.tasks.manager;

import ai.grakn.engine.TaskId;
import ai.grakn.engine.tasks.BackgroundTask;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 *     The base TaskManager interface.
 * </p>
 *
 * <p>
 *     Provides common methods for scheduling tasks for execution and stopping task execution.
 *     At the moment it assumes that upon construction, some threads will be started that
 *     take care of consuming the elements of the queue.
 * </p>
 *
 * @author Denis Lobanov, alexandraorth
 */
public interface TaskManager extends Closeable {

    /**
     * Schedule a {@link BackgroundTask} for execution.
     * @param taskState Task to execute
     */
    void addTask(TaskState taskState, TaskConfiguration configuration);

    void runTask(TaskState taskState, TaskConfiguration configuration);

    /**
     * Make sure the manager is initialized and starts processing tasks
     */
    CompletableFuture<Void> start();

    /**
     * Stop a Scheduled, Paused or Running task. Task's .stop() method will be called to perform any cleanup and the
     * task is killed afterwards.
     * @param id ID of task to stop.
     */
    void stopTask(TaskId id);

    /**
     * Return the StateStorage instance that is used by this class.
     * @return A StateStorage instance.
     */
    TaskStateStorage storage();

    // TODO: Add 'pause' and 'restart' methods
}

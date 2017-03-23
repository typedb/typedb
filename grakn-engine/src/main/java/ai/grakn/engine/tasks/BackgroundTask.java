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

package ai.grakn.engine.tasks;

import mjson.Json;

import java.util.function.Consumer;

/**
 * Interface which all tasks that wish to be scheduled for later execution as background tasks must implement.
 *
 * @author Denis Lobanov
 */
public interface BackgroundTask {
    /**
     * Called to start execution of the task, may be called on a newly scheduled or previously stopped task.
     * @param saveCheckpoint Consumer<String> which can be called at any time to save a state checkpoint that would allow
     *                       the task to resume from this point should it crash.
     *
     * @return true if the task successfully completed, or false if it was stopped.
     */
    boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration);

    /**
     * Called to stop execution of the task, may be called on a running or paused task.
     * Task should stop gracefully.
     *
     * @return true if the task was successfully stopped, or false if it could not be stopped.
     *
     * TODO: Should we allow start() to be called after stop()?
     */
    boolean stop();

    /**
     * Called to suspend the execution of a currently running task. The object may be destroyed after this call.
     *
     * TODO: stop running
     */
    void pause();

    /**
     * This method may be called when resuming from a paused state or recovering from a crash or failure of any kind.
     * @param saveCheckpoint Consumer<String> which can be called at any time to save a state checkpoint that would allow
     *                       the task to resume from this point should it crash.
     * @param lastCheckpoint The last checkpoint as sent to saveCheckpoint.
     */
    boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint);

}

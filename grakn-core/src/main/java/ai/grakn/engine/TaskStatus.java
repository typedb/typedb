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

package ai.grakn.engine;

/**
 * <p>
 *     Describes the six possible states a task can be in.
 *     Each state represents a different point in the process of execution.
 * </p>
 *
 * @author alexandraorth, Denis Lobanov
 */
public enum TaskStatus {
    /**
     * Save task in the graph, but not plans to run it yet - initial state.
     */
    CREATED,
    /**
     * Scheduled for execution. For example, if one instance of the Engine server schedules it,
     * other instances won't.
     */
    SCHEDULED,
    /**
     * Currently executing task.
     */
    RUNNING,
    /**
     * The task has successfully completed execution.
     */
    COMPLETED,
    /**
     * The task has been stopped on request.
     */
    STOPPED,
    /**
     * The task has failed to execute.
     */
    FAILED;
}

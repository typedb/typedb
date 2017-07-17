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

import ai.grakn.engine.tasks.BackgroundTask;

/**
 * <p>
 *     Submits Background Tasks for processing
 * </p>
 *
 * <p>
 *     Allows tasks to be submitted for processing. Any task submitted is added to {@link TaskStateStorage}
 *     and is later executed by Task runner such as {@link ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager}.
 * </p>
 *
 * @author fppt
 */
public interface TaskSubmitter {
    /**
     * Schedule a {@link BackgroundTask} for execution.
     * @param taskState Task to execute
     */
    void addTask(TaskState taskState, TaskConfiguration configuration);
}

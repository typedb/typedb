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

import ai.grakn.engine.tasks.TaskStateStorage;

/**
 * The {@link SingleQueueTaskRunner} is used by the {@link SingleQueueTaskManager} to execute tasks from a Kafka queue.
 *
 * @author aelred, alexandrorth
 */
public class SingleQueueTaskRunner implements Runnable, AutoCloseable {

    /**
     * Create a {@link SingleQueueTaskRunner} which uses the given {@param storage} to store and retrieve information
     * about tasks.
     *
     * @param storage a place to store and retrieve information about tasks.
     */
    public SingleQueueTaskRunner(TaskStateStorage storage) {
    }

    /**
     * Poll Kafka for any new tasks.
     * After receiving tasks, accept as many as possible, up to the maximum allowed number of tasks.
     * For each task, follow the workflow based on its type:
     *  - Immediate:
     *    - Record that this engine is running this task
     *      Record that this task is running
     *    - Acknowledge message in queue
     *    - Send to thread pool for execution:
     *       - Use reflection to retrieve task
     *       - Start from checkpoint if necessary, or from beginning
     *       - Record that this engine is no longer running this task
     *         Mark as completed or failed
     *  - Delayed: TODO
     *  - Recurring: TODO
     */
    @Override
    public void run() {
    }

    /**
     * Close connection to Kafka and thread pool.
     *
     * Inform {@link SingleQueueTaskRunner#run()} method to stop and block until it returns.
     */
    @Override
    public void close() throws Exception {

    }
}

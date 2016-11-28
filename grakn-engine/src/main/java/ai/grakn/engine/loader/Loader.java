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

package ai.grakn.engine.loader;

import ai.grakn.graql.InsertQuery;

public interface Loader {

    /**
     * Load any remaining batches in the queue.
     */
    void flush();

    /**
     * @return the current batch size - minimum number of vars to be loaded in a transaction
     */
    int getBatchSize();

    /**
     * Add an insert query to the queue
     * @param query insert query to be executed
     */
    void add(InsertQuery query);

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    Loader setBatchSize(int size);

    /**
     * Set the size of the queue- this is equivalent to the size of the semaphore.
     * @param size the size of the queue
     */
    Loader setQueueSize(int size);

    /**
     * Wait for all tasks to finish for one minute.
     */
    void waitToFinish();

    /**
     * Wait for all tasks to finish.
     * @param timeout amount of time (in ms) to wait.
     */
    void waitToFinish(int timeout);

    /**
     * Print the number of jobs that have completed
     */
    void printLoaderState();
}

/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine.task;

/**
 * <p>
 *     Defines the API which must be implemented in order to be able to run the task in the background
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public interface BackgroundTask extends AutoCloseable{

    /**
     * @return The amount of seconds to wait between running this job.
     */
    default int period(){
        return 60;
    }

    /**
     * The primary method to execute when the {@link BackgroundTask} starts executing
     */
    void run();

    /**
     * Shutdown the task. This is useful if the task creates it's own processes
     */
    void close();
}

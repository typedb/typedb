/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.log;

import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.log.Log;

/**
 * Manager interface for opening {@link Log}s against a particular Log implementation.
 *
 */
public interface LogManager {

    /**
     * Opens a LOG for the given name.
     * <p>
     * If a LOG with the given name already exists, the existing LOG is returned.
     *
     * @param name Name of the LOG to be opened
     */
    Log openLog(String name) throws BackendException;

    /**
     * Closes the LOG manager and all open logs (if they haven't already been explicitly closed)
     */
    void close() throws BackendException;

}

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

package grakn.core.graph.core.log;

import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.TransactionBuilder;

/**
 * Framework for processing transaction logs. Using the {@link LogProcessorBuilder} returned by
 * {@link #addLogProcessor(String)} one can process the change events for a particular transaction LOG identified by name.
 */
public interface LogProcessorFramework {

    /**
     * Returns a processor builder for the transaction LOG with the given LOG identifier.
     * Only one processor may be registered per transaction LOG.
     *
     * @param logIdentifier Name that identifies the transaction LOG to be processed,
     *                      i.e. the one used in {@link TransactionBuilder#logIdentifier(String)}
     */
    LogProcessorBuilder addLogProcessor(String logIdentifier);

    /**
     * Removes the LOG processor for the given identifier and closes the associated LOG.
     */
    boolean removeLogProcessor(String logIdentifier);

    /**
     * Closes all LOG processors, their associated logs, and the backing graph instance
     */
    void shutdown() throws JanusGraphException;


}

// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.core.log;

import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.log.LogProcessorBuilder;

/**
 * Framework for processing transaction logs. Using the {@link LogProcessorBuilder} returned by
 * {@link #addLogProcessor(String)} one can process the change events for a particular transaction LOG identified by name.
 *
 */
public interface LogProcessorFramework {

    /**
     * Returns a processor builder for the transaction LOG with the given LOG identifier.
     * Only one processor may be registered per transaction LOG.
     *
     * @param logIdentifier Name that identifies the transaction LOG to be processed,
     *                      i.e. the one used in {@link org.janusgraph.core.TransactionBuilder#logIdentifier(String)}
     * @return
     */
    LogProcessorBuilder addLogProcessor(String logIdentifier);

    /**
     * Removes the LOG processor for the given identifier and closes the associated LOG.
     *
     * @param logIdentifier
     * @return
     */
    boolean removeLogProcessor(String logIdentifier);

    /**
     * Closes all LOG processors, their associated logs, and the backing graph instance
     *
     * @throws JanusGraphException
     */
    void shutdown() throws JanusGraphException;


}

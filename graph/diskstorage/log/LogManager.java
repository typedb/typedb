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

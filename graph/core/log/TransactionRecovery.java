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
import grakn.core.graph.core.JanusGraphFactory;

/**
 * {@link TransactionRecovery} is a process that runs in the background and read's from the transaction
 * write-ahead LOG to determine which transactions have not been successfully persisted against all
 * backends. It then attempts to recover such transactions.
 * <p>
 * This process is started via {@link JanusGraphFactory}
 */
public interface TransactionRecovery {

    /**
     * Shuts down the transaction recovery process
     */
    void shutdown() throws JanusGraphException;

}

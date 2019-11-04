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

import grakn.core.graph.core.JanusGraphTransaction;

/**
 * Allows the user to define custom behavior to process those transactional changes that are recorded in a transaction LOG.
 * {@link ChangeProcessor}s are registered with a transaction LOG processor in the {@link LogProcessorBuilder}.
 */
public interface ChangeProcessor {

    /**
     * Process the changes caused by the transaction identified by {@code txId} within a newly opened transaction {@code tx}.
     * The changes are captured in the {@link ChangeState} data structure.
     */
    void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState);

}

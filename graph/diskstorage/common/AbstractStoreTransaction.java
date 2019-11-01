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

package grakn.core.graph.diskstorage.common;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Abstract implementation of {@link StoreTransaction} to be used as the basis for more specific implementations.
 *
 */

public abstract class AbstractStoreTransaction implements StoreTransaction {

    private final BaseTransactionConfig config;

    public AbstractStoreTransaction(BaseTransactionConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    @Override
    public void commit() throws BackendException {
    }

    @Override
    public void rollback() throws BackendException {
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
        return config;
    }

}

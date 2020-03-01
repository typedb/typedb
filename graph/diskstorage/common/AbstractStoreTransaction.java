/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.diskstorage.common;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Abstract implementation of StoreTransaction to be used as the basis for more specific implementations.
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

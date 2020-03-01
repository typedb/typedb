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

package grakn.core.graph.diskstorage.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.common.AbstractStoreTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;

import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;


/**
 * This class manages the translation of read and write consistency configuration values to CQL API ConsistencyLevel types.
 */
public class CQLTransaction extends AbstractStoreTransaction {

    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    public CQLTransaction(BaseTransactionConfig config) {
        super(config);
        this.readConsistencyLevel = DefaultConsistencyLevel.valueOf(getConfiguration().getCustomOption(READ_CONSISTENCY));
        this.writeConsistencyLevel = DefaultConsistencyLevel.valueOf(getConfiguration().getCustomOption(WRITE_CONSISTENCY));
    }

    ConsistencyLevel getReadConsistencyLevel() {
        return this.readConsistencyLevel;
    }

    ConsistencyLevel getWriteConsistencyLevel() {
        return this.writeConsistencyLevel;
    }

    static CQLTransaction getTransaction(StoreTransaction storeTransaction) {
        Preconditions.checkNotNull(storeTransaction);
        Preconditions.checkArgument(storeTransaction instanceof CQLTransaction, "Unexpected transaction type %s", storeTransaction.getClass().getName());
        return (CQLTransaction) storeTransaction;
    }

    @Override
    public String toString() {
        return "CQLTransaction@" + Integer.toHexString(hashCode()) + "[read=" + this.readConsistencyLevel
                + ",write=" + this.writeConsistencyLevel + "]";
    }
}

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

package grakn.core.graph.diskstorage.locking;

import com.google.common.collect.MapMaker;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.locking.AbstractLocker;
import grakn.core.graph.diskstorage.locking.LockStatus;
import grakn.core.graph.diskstorage.util.KeyColumn;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * A store for {@code LockStatus} objects. Thread-safe so long as the method
 * calls with any given {@code StoreTransaction} are serial. Put another way,
 * thread-safety is only broken by concurrently calling this class's methods
 * with the same {@code StoreTransaction} instance in the arguments to each
 * concurrent call.
 * 
 * @see AbstractLocker
 * @param <S>
 *            The {@link LockStatus} type.
 */
public class LockerState<S> {

    /**
     * Locks taken in the LocalLockMediator and written to the store (but not
     * necessarily checked)
     */
    private final ConcurrentMap<StoreTransaction, Map<KeyColumn, S>> locks;

    public LockerState() {
        // TODO this wild guess at the concurrency level should not be hardcoded
        this(new MapMaker().concurrencyLevel(8).weakKeys()
                .makeMap());
    }

    public LockerState(ConcurrentMap<StoreTransaction, Map<KeyColumn, S>> locks) {
        this.locks = locks;
    }

    public boolean has(StoreTransaction tx, KeyColumn kc) {
        return getLocksForTx(tx).containsKey(kc);
    }

    public void take(StoreTransaction tx, KeyColumn kc, S ls) {
        getLocksForTx(tx).put(kc, ls);
    }

    public void release(StoreTransaction tx, KeyColumn kc) {
        getLocksForTx(tx).remove(kc);
    }

    public Map<KeyColumn, S> getLocksForTx(StoreTransaction tx) {
        Map<KeyColumn, S> m = locks.get(tx);

        if (null == m) {
            m = new HashMap<>();
            final Map<KeyColumn, S> x = locks.putIfAbsent(tx, m);
            if (null != x) {
                m = x;
            }
        }

        return m;
    }
}
